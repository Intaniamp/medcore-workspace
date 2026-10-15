package main

import (
	"encoding/json"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

type StockExhaustedEvent struct {
	EventID    string `json:"event_id"`
	EventName  string `json:"event_name"`
	OccurredAt string `json:"occurred_at"`
	Producer   string `json:"producer"`
	Payload    struct {
		AppointmentID     string `json:"appointment_id"`
		PrescriptionID    string `json:"prescription_id"`
		DrugCode          string `json:"drug_code"`
		RequestedQuantity int    `json:"requested_quantity"`
		AvailableQuantity int    `json:"available_quantity"`
		Reason            string `json:"reason"`
	} `json:"payload"`
}

// StartStockExhaustedConsumer menerima event stok habis dari Pharmacy Service.
func StartStockExhaustedConsumer(amqpURI string) {
	conn, err := amqp.Dial(amqpURI)
	if err != nil {
		log.Printf("[Compensation Consumer] Gagal terhubung ke RabbitMQ: %v", err)
		return
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Printf("[Compensation Consumer] Gagal membuka channel: %v", err)
		return
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
		log.Printf("[Compensation Consumer] Gagal membuat exchange: %v", err)
		return
	}

	queue, err := ch.QueueDeclare(
		"appointment.stock.exhausted.q",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Printf("[Compensation Consumer] Gagal membuat queue: %v", err)
		return
	}

	err = ch.QueueBind(
		queue.Name,
		"medcore.event.pharmacy.stock.exhausted",
		"medcore.clinical.events",
		false,
		nil,
	)
	if err != nil {
		log.Printf("[Compensation Consumer] Gagal menghubungkan queue: %v", err)
		return
	}

	messages, err := ch.Consume(
		queue.Name,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Printf("[Compensation Consumer] Gagal menerima pesan: %v", err)
		return
	}

	log.Println("[Compensation Consumer] Menunggu event stok habis...")

	for msg := range messages {
		var event StockExhaustedEvent

		if err := json.Unmarshal(msg.Body, &event); err != nil {
			log.Printf("[Compensation Consumer] Event tidak valid: %v", err)
			_ = msg.Nack(false, false)
			continue
		}

		if event.Payload.AppointmentID == "" {
			log.Println("[Compensation Consumer] Appointment ID kosong, event ditolak.")
			_ = msg.Nack(false, false)
			continue
		}

		log.Printf("[Compensation Consumer] Event diterima: %s", event.EventName)
		log.Printf("[Compensation Consumer] Appointment ID: %s", event.Payload.AppointmentID)
		log.Printf("[Compensation Consumer] Obat: %s", event.Payload.DrugCode)
		log.Printf("[Compensation Consumer] Stok tersedia: %d", event.Payload.AvailableQuantity)

		// Simulasi perubahan status, belum menyimpan ke database.
		log.Printf(
			"[Appointment Service] Status antrean %s diubah menjadi PENDING_MEDICATION_SUBSTITUTION",
			event.Payload.AppointmentID,
		)

		if err := msg.Ack(false); err != nil {
			log.Printf("[Compensation Consumer] Gagal ACK pesan: %v", err)
		}
	}
}
