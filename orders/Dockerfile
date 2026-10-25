FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY *.go ./
RUN CGO_ENABLED=0 go build -trimpath -o /out/orders .

FROM alpine:3.19
COPY --from=build /out/orders /orders
EXPOSE 8080
ENTRYPOINT ["/orders"]
