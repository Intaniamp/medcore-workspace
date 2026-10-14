package main

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/google/uuid"

	"appointment-service/publisher"
)

func main() {
	amqpURI := os.Getenv("AMQP_URI")
	if amqpURI == "" {
		amqpURI = "amqp://admin_medcore:SecuredPassRabbit2026!@localhost:5672/medcore_vhost"
	}

	eventPublisher, err := publisher.NewEventPublisher(amqpURI)
	if err != nil {
		log.Fatalf("Gagal membuat publisher: %v", err)
	}
	defer eventPublisher.Close()

	event := publisher.ConsultationCompletedEvent{
		EventID:    uuid.NewString(),
		EventName:  "ConsultationCompletedEvent",
		OccurredAt: time.Now().UTC().Format(time.RFC3339),
		Producer:   "appointment-service",
		Payload: publisher.ConsultationPayload{
			AppointmentID:      "b1111111-1111-1111-1111-111111111111",
			PatientID:          "a0000000-0000-0000-0000-000000000001",
			DoctorID:           "b0000000-0000-0000-0000-000000000001",
			ConsultationStatus: "COMPLETED",
			Prescription: publisher.PrescriptionData{
				PrescriptionID: "rx-2026-9901",
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

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := eventPublisher.PublishConsultationCompleted(ctx, event); err != nil {
		log.Fatalf("Gagal mengirim event konsultasi: %v", err)
	}

	log.Println("Pengiriman event konsultasi selesai.")
}
