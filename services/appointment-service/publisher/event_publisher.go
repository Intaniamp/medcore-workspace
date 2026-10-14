
package publisher

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	exchangeName = "medcore.clinical.events"
	routingKey   = "clinical.consultation.completed"
)

// EventPublisher mengatur koneksi dan pengiriman event ke RabbitMQ.
type EventPublisher struct {
	conn    *amqp.Connection
	channel *amqp.Channel
}

// PrescriptionItem berisi informasi obat dalam resep.
type PrescriptionItem struct {
	DrugCode string `json:"drug_code"`
	Quantity int    `json:"quantity"`
	Dosage   string `json:"dosage"`
}

// PrescriptionData berisi informasi resep.
type PrescriptionData struct {
	PrescriptionID string            `json:"prescription_id"`
	Items          []PrescriptionItem `json:"items"`
}

// BillingData berisi ringkasan tagihan konsultasi.
type BillingData struct {
	ConsultationFee float64 `json:"consultation_fee"`
	TriageUrgency   string  `json:"triage_urgency"`
}

// ConsultationPayload berisi data konsultasi yang selesai.
type ConsultationPayload struct {
	AppointmentID      string           `json:"appointment_id"`
	PatientID          string           `json:"patient_id"`
	DoctorID           string           `json:"doctor_id"`
	ConsultationStatus string           `json:"consultation_status"`
	Prescription       PrescriptionData `json:"prescription"`
	BillingSummary     BillingData      `json:"billing_summary"`
}

// ConsultationCompletedEvent adalah event yang dikirim ke RabbitMQ.
type ConsultationCompletedEvent struct {
	EventID    string               `json:"event_id"`
	EventName  string               `json:"event_name"`
	OccurredAt string               `json:"occurred_at"`
	Producer   string               `json:"producer"`
	Payload    ConsultationPayload  `json:"payload"`
}

// NewEventPublisher membuat koneksi ke RabbitMQ.
func NewEventPublisher(amqpURI string) (*EventPublisher, error) {
	conn, err := amqp.Dial(amqpURI)
	if err != nil {
		return nil, fmt.Errorf("gagal terhubung ke RabbitMQ: %w", err)
	}

	channel, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("gagal membuka channel RabbitMQ: %w", err)
	}

	err = channel.ExchangeDeclare(
		exchangeName,
		"topic",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		channel.Close()
		conn.Close()
		return nil, fmt.Errorf("gagal membuat exchange: %w", err)
	}

	log.Println("Publisher berhasil terhubung ke RabbitMQ")

	return &EventPublisher{
		conn:    conn,
		channel: channel,
	}, nil
}

// PublishConsultationCompleted mengirim event konsultasi selesai.
func (p *EventPublisher) PublishConsultationCompleted(
	ctx context.Context,
	event ConsultationCompletedEvent,
) error {
	body, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("gagal mengubah event menjadi JSON: %w", err)
	}

	err = p.channel.PublishWithContext(
		ctx,
		exchangeName,
		routingKey,
		true,
		false,
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "application/json",
			MessageId:    event.EventID,
			Timestamp:    time.Now(),
			Type:         event.EventName,
			Body:         body,
		},
	)
	if err != nil {
		return fmt.Errorf("gagal mengirim event: %w", err)
	}

	log.Printf("Event %s berhasil dikirim ke RabbitMQ", event.EventID)
	return nil
}

// Close menutup channel dan koneksi RabbitMQ.
func (p *EventPublisher) Close() {
	if p.channel != nil {
		_ = p.channel.Close()
	}
	if p.conn != nil {
		_ = p.conn.Close()
	}
}
