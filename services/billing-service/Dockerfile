# ==========================================
# STAGE 1: Compilation Environment (Builder)
# ==========================================
FROM golang:1.22-alpine3.19 AS builder

# 1. Pasang dependensi sertifikat dan git
RUN apk add --no-cache ca-certificates git tzdata

WORKDIR /src

# 2. Cache Dependency Layer: Salin go.mod & go.sum terlebih dahulu
COPY go.mod go.sum ./
RUN go mod download && go mod verify

# 3. Salin seluruh source code aplikasi
COPY . .

# 4. Kompilasi Static Binary
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags="-w -s -extldflags '-static'" \
    -o /bin/medcore-billing-app main.go

# ==========================================
# STAGE 2: Minimalist Production Runtime
# ==========================================
FROM alpine:3.19 AS runner

# 1. Keamanan: Buat unprivileged user khusus (non-root)
RUN addgroup -g 10001 -S medcore && \
    adduser -u 10001 -S medcore -G medcore

# 2. Pasang CA-certificates dan zona waktu UTC
RUN apk add --no-cache ca-certificates tzdata

WORKDIR /app

# 3. Salin executable biner dari stage builder
COPY --from=builder /bin/medcore-billing-app /app/medcore-billing-app

# 4. Ganti kepemilikan direktori kerja ke unprivileged user
RUN chown -R medcore:medcore /app

# 5. Beralih ke non-root user
USER medcore:medcore

# 6. Eksekusi biner langsung (Worker tidak memerlukan EXPOSE port)
ENTRYPOINT ["/app/medcore-billing-app"]