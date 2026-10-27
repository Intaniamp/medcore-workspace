# STAGE 1: Compilation Environment (Builder)
FROM golang:1.26-alpine3.19 AS builder

RUN apk add --no-cache ca-certificates git tzdata

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download && go mod verify

COPY . . 


RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags="-w -s -extldflags '-static'" \
    -o /bin/medcore-appointment-app main.go

# STAGE 2: Minimalist Production Runtime
FROM  alpine:3.19 AS runner

RUN addgroup -g 10001 -S medcore && \
    adduser -u 10001 -S medcore -G medcore

RUN apk add --no-cache ca-certificates tzdata

WORKDIR /app

COPY --from=builder /bin/medcore-appointment-app /app/medcore-appointment-app

RUN chown -R medcore:medcore /app

USER medcore:medcore

EXPOSE 8080

ENTRYPOINT ["/app/medcore-appointment-app"]