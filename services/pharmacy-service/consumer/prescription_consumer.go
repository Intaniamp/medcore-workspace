package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

type EventEnvelope struct {
	EventID string `json:"event_id"`
	Payload struct {
		AppointmentID string `json:"appointment_id"`
		Prescription  struct {
			PrescriptionID string `json:"prescription_id"`
			Items          []struct {
				DrugCode string `json:"drug_code"`
				Quantity int    `json:"quantity"`
			} `json:"items"`
		} `json:"prescription"`
	} `json:"payload"`
}

type ProcessedEventRecord struct {
	EventID     string    `bson:"event_id"`
	ProcessedAt time.Time `bson:"processed_at"`
	Status      string    `bson:"status"`
}

type StockExhaustedEvent struct {
	EventID    string                `json:"event_id"`
	EventName  string                `json:"event_name"`
	OccurredAt string                `json:"occurred_at"`
	Producer   string                `json:"producer"`
	Payload    StockExhaustedPayload `json:"payload"`
}

type StockExhaustedPayload struct {
	AppointmentID     string `json:"appointment_id"`
	PrescriptionID    string `json:"prescription_id"`
	DrugCode          string `json:"drug_code"`
	RequestedQuantity int    `json:"requested_quantity"`
	AvailableQuantity int    `json:"available_quantity"`
	Reason            string `json:"reason"`
}

func StartPrescriptionConsumer(amqpURI string, mongoClient *mongo.Client) {
	conn, err := amqp.Dial(amqpURI)
	if err != nil {
		log.Fatalf("[Pharmacy Consumer] Gagal koneksi broker: %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("[Pharmacy Consumer] Gagal membuka channel: %v", err)
	}
	defer ch.Close()

	err = ch.ExchangeDeclare(
		"medcore.clinical.events",
		"topic",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("[Pharmacy Consumer] Gagal deklarasi exchange: %v", err)
	}

	args := amqp.Table{
		"x-dead-letter-exchange": "medcore.deadletter.exchange",
	}

	q, err := ch.QueueDeclare(
		"pharmacy.prescription.order.q",
		true,
		false,
		false,
		false,
		args,
	)
	if err != nil {
		log.Fatalf("[Pharmacy Consumer] Gagal deklarasi queue: %v", err)
	}

	err = ch.QueueBind(
		q.Name,
		"clinical.consultation.*",
		"medcore.clinical.events",
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("[Pharmacy Consumer] Gagal binding queue: %v", err)
	}

	err = ch.Qos(1, 0, false)
	if err != nil {
		log.Fatalf("[Pharmacy Consumer] Gagal set QoS: %v", err)
	}

	msgs, err := ch.Consume(
		q.Name,
		"pharmacy-worker",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("[Pharmacy Consumer] Gagal subscribe: %v", err)
	}

	log.Printf("[Pharmacy Consumer] Menunggu pesan resep pada antrean: %s", q.Name)

	forever := make(chan bool)

	go func() {
		for d := range msgs {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			// Kirimkan ch (channel) agar fungsi ini bisa menerbitkan event balasan
			ProcessPrescriptionWithDeduplication(ctx, mongoClient, ch, d)
			cancel()
		}
	}()

	<-forever
}

func ProcessPrescriptionWithDeduplication(ctx context.Context, mongoClient *mongo.Client, ch *amqp.Channel, d amqp.Delivery) {
	var evt EventEnvelope
	if err := json.Unmarshal(d.Body, &evt); err != nil {
		d.Nack(false, false)
		return
	}

	// Cek Poison Message (Langkah 2.2)
	for _, item := range evt.Payload.Prescription.Items {
		if item.Quantity <= 0 {
			log.Printf("[Pharmacy Consumer] Terdeteksi Poison Message! Kuantitas tidak valid: %d. Mengirim NACK ke DLQ.", item.Quantity)
			d.Nack(false, false)
			return
		}
	}

	eventColl := mongoClient.Database("medcore_pharmacy_db").Collection("processed_events")

	// 1. Cek apakah Event ID sudah pernah diproses sebelumnya (Langkah 2.3)
	var existing ProcessedEventRecord
	err := eventColl.FindOne(ctx, bson.M{"event_id": evt.EventID}).Decode(&existing)
	if err == nil {
		log.Printf("[Deduplication Guard] Event %s terdeteksi duplikat! Melewati eksekusi bisnis dan langsung mengirim ACK.", evt.EventID)
		d.Ack(false)
		return
	}

	// 2. Eksekusi Logika Bisnis & Skenario Kompensasi Stok Kosong (Langkah 2.4)
	log.Printf("[Business Logic] Memproses stok resep untuk Event ID: %s", evt.EventID)

	stockExhausted := false
	for _, item := range evt.Payload.Prescription.Items {
		if item.DrugCode == "MED-AMX-500" {
			stockExhausted = true
			// Simulasi stok fisik MED-AMX-500 di gudang kosong (0)
			log.Printf("[Pharmacy Warning] Stok fisik obat %s di gudang KOSONG (0)!", item.DrugCode)
			log.Printf("[Compensation Publisher] Menerbitkan event balasan 'medcore.event.pharmacy.stock.exhausted' ke Exchange...")

			compEvent := StockExhaustedEvent{
				EventID:    fmt.Sprintf("%s.stock-exhausted.%s", evt.EventID, item.DrugCode),
				EventName:  "medcore.event.pharmacy.stock.exhausted",
				OccurredAt: time.Now().UTC().Format(time.RFC3339),
				Producer:   "pharmacy-service",
				Payload: StockExhaustedPayload{
					AppointmentID:     evt.Payload.AppointmentID,
					PrescriptionID:    evt.Payload.Prescription.PrescriptionID,
					DrugCode:          item.DrugCode,
					RequestedQuantity: item.Quantity,
					AvailableQuantity: 0,
					Reason:            "STOCK_EXHAUSTED_IN_WAREHOUSE",
				},
			}

			body, err := json.Marshal(compEvent)
			if err != nil {
				log.Printf("[Error] Gagal serialisasi event kompensasi: %v. Requeue pesan.", err)
				d.Nack(false, true)
				return
			}

			err = ch.PublishWithContext(
				ctx,
				"medcore.clinical.events",                // Exchange tujuan
				"medcore.event.pharmacy.stock.exhausted", // Routing Key kompensasi
				false,
				false,
				amqp.Publishing{
					DeliveryMode: amqp.Persistent,
					ContentType:  "application/json",
					MessageId:    compEvent.EventID,
					Type:         compEvent.EventName,
					Timestamp:    time.Now().UTC(),
					Body:         body,
				},
			)
			if err != nil {
				log.Printf("[Error] Gagal menerbitkan event kompensasi: %v. Requeue pesan.", err)
				d.Nack(false, true)
				return
			} else {
				log.Printf("[Compensation Publisher] Berhasil menerbitkan event kompensasi untuk Appointment ID: %s", evt.Payload.AppointmentID)
			}
		}
	}

	// 3. Catat Event ID ke database riwayat proses (Atomic Insert)
	status := "PROCESSED"
	if stockExhausted {
		status = "COMPENSATED_OUT_OF_STOCK"
	}

	_, err = eventColl.InsertOne(ctx, ProcessedEventRecord{
		EventID:     evt.EventID,
		ProcessedAt: time.Now(),
		Status:      status,
	})
	if err != nil {
		log.Printf("[Error] Gagal mencatat deduplikasi: %v. Requeue pesan.", err)
		d.Nack(false, true)
		return
	}

	d.Ack(false)
	log.Printf("[Pharmacy Consumer] Transaksi selesai sempurna dan Event %s dicatat.", evt.EventID)
}
