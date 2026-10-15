package consumer

import (
	"encoding/json"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
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

func StartPrescriptionConsumer(amqpURI string) {
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

	// 1. Deklarasikan Queue durable khusus Farmasi
	q, err := ch.QueueDeclare(
		"pharmacy.prescription.order.q", // Nama Queue
		true,                            // Durable
		false,                           // Auto-delete
		false,                           // Exclusive
		false,                           // No-wait
		nil,                             // Arguments

	)
	if err != nil {
		log.Fatalf("[Pharmacy Consumer] Gagal deklarasi queue: %v", err)
	}

	// 2. Ikat Queue ke Exchange dengan Binding Pattern
	err = ch.QueueBind(
		q.Name,
		"clinical.consultation.*", // Routing pattern
		"medcore.clinical.events", // Exchange asal
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("[Pharmacy Consumer] Gagal binding queue: %v", err)
	}

	// 3. Pasang Prefetch Count (QoS = 1 agar beban seimbang)
	err = ch.Qos(1, 0, false)
	if err != nil {
		log.Fatalf("[Pharmacy Consumer] Gagal set QoS: %v", err)
	}
	msgs, err := ch.Consume(
		q.Name,
		"pharmacy-worker", // Consumer tag
		false,             // autoAck = FALSE (Wajib Manual Ack!)
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
			var evt EventEnvelope
			if err := json.Unmarshal(d.Body, &evt); err != nil {
				log.Printf("[Pharmacy Consumer] Format pesan tidak valid, NACK tanpa requeue: %v", err)
				d.Nack(false, false)
				continue
			}

			log.Println("---------------------------------------------------------")
			log.Printf("[Pharmacy Consumer] Menerima Resep Baru!")
			log.Printf(" Event ID        : %s", evt.EventID)
			log.Printf(" Appointment ID  : %s", evt.Payload.AppointmentID)
			log.Printf(" Prescription ID : %s", evt.Payload.Prescription.PrescriptionID)
			for _, item := range evt.Payload.Prescription.Items {
				log.Printf(" -> Item: %s | Qty: %d", item.DrugCode, item.Quantity)
			}

			log.Println("---------------------------------------------------------")

			// Konfirmasi ke RabbitMQ bahwa pesan selesai diproses
			d.Ack(false)
			log.Println("[Pharmacy Consumer] Pesan berhasil di-ACK.")
		}
	}()

	<-forever
}
