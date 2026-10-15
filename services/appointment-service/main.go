package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"appointment-service/publisher"

	"github.com/google/uuid"
)

func main() {
	amqpURI := "amqp://admin_medcore:SecuredPassRabbit2026!@localhost:5672/medcore_vhost"

	log.Println("[Appointment Service] Menginisialisasi koneksi messaging...")

	pub, err := publisher.NewEventPublisher(amqpURI)
	if err != nil {
		log.Fatalf("Inisialisasi publisher gagal: %v", err)
	}
	defer pub.Close()

	// Simulasi event ketika konsultasi selesai
	simulatedEvent := publisher.ConsultationCompletedEvent{
		EventID:    fmt.Sprintf("evt-%s", uuid.New().String()),
		EventName:  "ConsultationCompletedEvent",
		OccurredAt: time.Now().UTC().Format(time.RFC3339),
		Producer:   "appointment-service",
		Payload: publisher.ConsultationPayload{
			AppointmentID:      "b1111111-1111-1111-1111-111111111111",
			PatientID:          "a0000000-0000-0000-0000-000000000001",
			DoctorID:           "b0000000-0000-0000-0000-000000000001",
			ConsultationStatus: "COMPLETED",
			Prescription: publisher.PrescriptionData{
				PrescriptionID: "RX-2026-9901",
				Items: []publisher.PrescriptionItem{
					{
						DrugCode: "MED-AMX-500",
						Quantity: 15,
						Dosage:   "3x1 sehari sesudah makan",
					},
				},
			},
			BillingSummary: publisher.BillingData{
				ConsultationFee: 150000.0,
				TriageUrgency:   "GREEN",
			},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	log.Println("[Appointment Service] Menerbitkan event konsultasi ke RabbitMQ...")

	if err := pub.PublishConsultationCompleted(ctx, simulatedEvent); err != nil {
		log.Fatalf("Gagal mempublikasikan event: %v", err)
	}

	log.Println("[Appointment Service] Proses publikasi selesai sempurna.")
}
