package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Item struct {
	SKU        string `json:"sku"`
	Qty        int    `json:"qty"`
	PriceCents int64  `json:"price_cents"`
}

type CreateOrderReq struct {
	UserID string `json:"user_id"`
	Items  []Item `json:"items"`
}

type PaymentReq struct {
	OrderID     string `json:"order_id"`
	AmountCents int64  `json:"amount_cents"`
	Currency    string `json:"currency"`
}

// Событие, которое мы шлем в шину
type OrderPaidEvent struct {
	ID         string    `json:"id"`
	Type       string    `json:"type"`
	OrderID    string    `json:"order_id"`
	OccurredAt time.Time `json:"occurred_at"`
}

// Событие, которое приходит от шины
type DeliveryConfirmedEvent struct {
	ID          string    `json:"id"`
	Type        string    `json:"type"`
	UserID      string    `json:"user_id"`
	BonusPoints int64     `json:"bonus_points"`
	OccurredAt  time.Time `json:"occurred_at"`
}

var (
	db           *pgxpool.Pool
	orderQueue   chan string
	paymentURL   string
	inventoryURL string
	eventsURL    string // URL шины

	gateMu              sync.Mutex
	consecutiveFailures int
	gateOpenUntil       time.Time

	httpClient = &http.Client{Timeout: 5 * time.Second}
)

func main() {
	port := getEnv("PORT", "8080")
	dbURL := getEnv("DATABASE_URL", "postgres://orders:orders@postgres:5432/orders")
	paymentURL = getEnv("PAYMENT_URL", "http://mocks:8080/payment")
	inventoryURL = getEnv("INVENTORY_URL", "http://mocks:8080/inventory")
	eventsURL = getEnv("EVENTS_URL", "http://mocks:8080") // База шины

	initDB(dbURL)
	orderQueue = make(chan string, 1000)

	// 5 воркеров для заказов
	for i := 0; i < 5; i++ {
		go worker()
	}
	
	// 1 воркер для отправки событий в шину
	go outboxWorker()

	recoverPendingOrders()

	http.HandleFunc("/healthz", healthz)
	http.HandleFunc("/orders", createOrder)
	http.HandleFunc("/orders/", getOrder)
	http.HandleFunc("/webhooks/delivery", handleDeliveryWebhook) // Задание 2
	http.HandleFunc("/users/", getUserBonus)                    // Задание 2

	log.Printf("Starting server on :%s", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}

func getEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback
}

func newUUID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}

func initDB(url string) {
	var err error
	db, err = pgxpool.New(context.Background(), url)
	if err != nil {
		log.Fatalf("Unable to connect to database: %v", err)
	}
	
	// Обновленная схема БД для обоих заданий
	schema := `
	CREATE TABLE IF NOT EXISTS orders (
		id UUID PRIMARY KEY,
		user_id TEXT NOT NULL,
		items JSONB NOT NULL,
		amount_cents BIGINT NOT NULL,
		status TEXT NOT NULL DEFAULT 'pending',
		reservation_done BOOLEAN NOT NULL DEFAULT FALSE,
		payment_state TEXT NOT NULL DEFAULT 'none',
		created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
	);
	
	-- Таблица исходящих событий (Outbox)
	CREATE TABLE IF NOT EXISTS outbox (
		event_id UUID PRIMARY KEY,
		event_type TEXT NOT NULL,
		payload JSONB NOT NULL,
		created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
	);

	-- Таблица обработанных входящих событий (Idempotency)
	CREATE TABLE IF NOT EXISTS processed_events (
		event_id TEXT PRIMARY KEY,
		processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
	);

	-- Таблица бонусов
	CREATE TABLE IF NOT EXISTS user_bonuses (
		user_id TEXT PRIMARY KEY,
		points BIGINT NOT NULL DEFAULT 0
	);
	`
	_, err = db.Exec(context.Background(), schema)
	if err != nil {
		log.Fatalf("Failed to create schema: %v", err)
	}
}

func healthz(w http.ResponseWriter, r *http.Request) {
	if err := db.Ping(r.Context()); err != nil {
		http.Error(w, "db not ready", 503)
		return
	}
	w.WriteHeader(200)
}

func createOrder(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", 405)
		return
	}

	var req CreateOrderReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", 400)
		return
	}
	if len(req.Items) == 0 {
		http.Error(w, "items required", 400)
		return
	}

	var amount int64
	for _, it := range req.Items {
		if it.Qty <= 0 || it.PriceCents < 0 {
			http.Error(w, "invalid item", 400)
			return
		}
		amount += int64(it.Qty) * it.PriceCents
	}

	id := newUUID()
	itemsJSON, _ := json.Marshal(req.Items)

	_, err := db.Exec(r.Context(),
		"INSERT INTO orders (id, user_id, items, amount_cents) VALUES ($1, $2, $3, $4)",
		id, req.UserID, itemsJSON, amount)
	if err != nil {
		log.Printf("db insert error: %v", err)
		http.Error(w, "db error", 500)
		return
	}

	select {
	case orderQueue <- id:
	default:
		log.Printf("queue full, order %s left in db", id)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(201)
	json.NewEncoder(w).Encode(map[string]string{"id": id, "status": "pending"})
}

func getOrder(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", 405)
		return
	}

	id := r.URL.Path[len("/orders/"):]
	var status string
	var amount int64
	err := db.QueryRow(r.Context(), "SELECT status, amount_cents FROM orders WHERE id = $1", id).Scan(&status, &amount)
	if err != nil {
		http.Error(w, "not found", 404)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"id":           id,
		"status":       status,
		"amount_cents": amount,
	})
}

func worker() {
	for id := range orderQueue {
		processOrder(id)
	}
}

func processOrder(id string) {
	ctx := context.Background()

	var userID, status, paymentState string
	var itemsJSON []byte
	var amount int64
	var reservationDone bool
	err := db.QueryRow(ctx, "SELECT user_id, items, amount_cents, status, reservation_done, payment_state FROM orders WHERE id = $1", id).
		Scan(&userID, &itemsJSON, &amount, &status, &reservationDone, &paymentState)
	
	if err != nil || status != "pending" {
		return
	}

	// 1. Резерв на складе
	if !reservationDone {
		if !reserveInventory(ctx, id, itemsJSON) {
			requeue(id, 2*time.Second)
			return
		}
		db.Exec(ctx, "UPDATE orders SET reservation_done = true WHERE id = $1", id)
	}

	// 2. Оплата
	payStatus, err := chargePayment(ctx, id, amount)
	if err != nil {
		db.Exec(ctx, "UPDATE orders SET payment_state = 'unknown' WHERE id = $1", id)
		requeue(id, 2*time.Second)
		return
	}

	// 3. Установка финального статуса
	if payStatus == "captured" {
		// ИСПОЛЬЗУЕМ ТРАНЗАКЦИЮ (Transactional Outbox)
		tx, err := db.Begin(ctx)
		if err != nil {
			requeue(id, 2*time.Second)
			return
		}
		defer tx.Rollback(ctx) // Откатится, если не сделаем Commit

		// Меняем статус
		_, err = tx.Exec(ctx, "UPDATE orders SET status = 'paid', payment_state = 'charged' WHERE id = $1", id)
		if err != nil {
			requeue(id, 2*time.Second)
			return
		}

		// Кладем событие в Outbox
		eventID := newUUID()
		event := OrderPaidEvent{
			ID:         eventID,
			Type:       "order.paid",
			OrderID:    id,
			OccurredAt: time.Now().UTC(),
		}
		payload, _ := json.Marshal(event)
		
		_, err = tx.Exec(ctx, "INSERT INTO outbox (event_id, event_type, payload) VALUES ($1, $2, $3)", eventID, "order.paid", payload)
		if err != nil {
			requeue(id, 2*time.Second)
			return
		}

		// Фиксируем транзакцию (теперь и статус paid, и событие в базе)
		if err := tx.Commit(ctx); err != nil {
			requeue(id, 2*time.Second)
			return
		}

	} else if payStatus == "declined" {
		releaseInventory(ctx, id)
		db.Exec(ctx, "UPDATE orders SET status = 'rejected', payment_state = 'declined' WHERE id = $1", id)
	}
}

func requeue(id string, delay time.Duration) {
	time.AfterFunc(delay, func() {
		select {
		case orderQueue <- id:
		default:
		}
	})
}

func recoverPendingOrders() {
	rows, err := db.Query(context.Background(), "SELECT id FROM orders WHERE status = 'pending'")
	if err != nil {
		return
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err == nil {
			orderQueue <- id
		}
	}
}

// --- Логика исходящих событий (Outbox Worker) ---

func outboxWorker() {
	for {
		// Раз в секунду проверяем таблицу outbox
		time.Sleep(1 * time.Second)

		rows, err := db.Query(context.Background(), "SELECT event_id, payload FROM outbox ORDER BY created_at LIMIT 10")
		if err != nil {
			continue
		}

		for rows.Next() {
			var eventID string
			var payload []byte
			if err := rows.Scan(&eventID, &payload); err != nil {
				continue
			}

			// Пытаемся отправить в шину
			if sendEventToBus(payload) {
				// Если успешно — удаляем из outbox
				db.Exec(context.Background(), "DELETE FROM outbox WHERE event_id = $1", eventID)
			}
		}
		rows.Close()
	}
}

func sendEventToBus(payload []byte) bool {
	req, _ := http.NewRequest(http.MethodPost, eventsURL+"/events", bytes.NewReader(payload))
	req.Header.Set("Content-Type", "application/json")
	
	resp, err := httpClient.Do(req)
	if err != nil {
		return false // Шина недоступна, попробуем позже
	}
	defer resp.Body.Close()

	// 202 Accepted — шина приняла событие
	if resp.StatusCode == 202 {
		return true
	}
	return false
}

// --- Логика входящих событий (Webhook) ---

func handleDeliveryWebhook(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", 405)
		return
	}

	var event DeliveryConfirmedEvent
	if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
		http.Error(w, "bad request", 400)
		return
	}

	ctx := context.Background()
	
	// Транзакция для безопасной идемпотентности
	tx, err := db.Begin(ctx)
	if err != nil {
		http.Error(w, "internal error", 500)
		return
	}
	defer tx.Rollback(ctx)

	// Пытаемся вставить ID события в таблицу обработанных
	tag, err := tx.Exec(ctx, "INSERT INTO processed_events (event_id) VALUES ($1) ON CONFLICT (event_id) DO NOTHING", event.ID)
	if err != nil {
		http.Error(w, "db error", 500)
		return
	}

	// Если тег говорит, что вставлено 0 строк — значит, мы уже обрабатывали это событие
	if tag.RowsAffected() == 0 {
		w.WriteHeader(http.StatusOK) // Отвечаем 200, чтобы шина отстала
		return
	}

	// Если мы здесь, значит событие новое. Начисляем бонусы!
	_, err = tx.Exec(ctx, 
		"INSERT INTO user_bonuses (user_id, points) VALUES ($1, $2) ON CONFLICT (user_id) DO UPDATE SET points = user_bonuses.points + EXCLUDED.points",
		event.UserID, event.BonusPoints)
	if err != nil {
		http.Error(w, "db error", 500)
		return
	}

	if err := tx.Commit(ctx); err != nil {
		http.Error(w, "db error", 500)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func getUserBonus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", 405)
		return
	}

	// URL: /users/{id}/bonus. Нам нужно вырезать {id}
	path := r.URL.Path
	// Проверка, что путь заканчивается на /bonus
	if len(path) < 7 || path[len(path)-6:] != "/bonus" {
		http.Error(w, "not found", 404)
		return
	}
	userID := path[7 : len(path)-6]

	var points int64
	err := db.QueryRow(r.Context(), "SELECT points FROM user_bonuses WHERE user_id = $1", userID).Scan(&points)
	if err != nil {
		// Если нет в базе, значит начислений не было, возвращаем 0
		points = 0 
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]int64{"points": points})
}

// --- Логика ворот (Gate) ---

func waitGate() {
	for {
		gateMu.Lock()
		if time.Now().After(gateOpenUntil) {
			gateMu.Unlock()
			return
		}
		waitTime := time.Until(gateOpenUntil)
		gateMu.Unlock()
		time.Sleep(waitTime)
	}
}

func recordPaymentSuccess() {
	gateMu.Lock()
	defer gateMu.Unlock()
	consecutiveFailures = 0
	gateOpenUntil = time.Time{}
}

func recordPaymentTimeout() {
	gateMu.Lock()
	defer gateMu.Unlock()
	consecutiveFailures++
	if consecutiveFailures >= 3 {
		gateOpenUntil = time.Now().Add(10 * time.Second)
		consecutiveFailures = 0
	}
}

func reserveInventory(ctx context.Context, orderID string, items []byte) bool {
	body := map[string]interface{}{"order_id": orderID, "items": json.RawMessage(items)}
	b, _ := json.Marshal(body)
	resp, err := httpClient.Post(inventoryURL+"/reservations", "application/json", bytes.NewReader(b))
	if err != nil || resp.StatusCode >= 500 {
		return false
	}
	return true
}

func releaseInventory(ctx context.Context, orderID string) {
	req, _ := http.NewRequest(http.MethodDelete, inventoryURL+"/reservations/"+orderID, nil)
	httpClient.Do(req)
}

func chargePayment(ctx context.Context, orderID string, amount int64) (string, error) {
	waitGate()

	body := PaymentReq{OrderID: orderID, AmountCents: amount, Currency: "RUB"}
	b, _ := json.Marshal(body)

	req, _ := http.NewRequest(http.MethodPost, paymentURL+"/payments", bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", orderID)

	resp, err := httpClient.Do(req)
	if err != nil {
		var netErr net.Error
		if errors.As(err, &netErr) && netErr.Timeout() {
			recordPaymentTimeout()
		}
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode == 402 {
		recordPaymentSuccess()
		return "declined", nil
	}
	if resp.StatusCode == 201 || resp.StatusCode == 200 {
		recordPaymentSuccess()
		return "captured", nil
	}
	
	return "", fmt.Errorf("payment returned %d", resp.StatusCode)
}
