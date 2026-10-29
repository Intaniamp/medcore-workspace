package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	pb "pharmacy-service/pb"

	"pharmacy-service/consumer"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type server struct {
	pb.UnimplementedPharmacyServiceServer
	mongoClient *mongo.Client
}

type DrugDoc struct {
	DrugCode   string  `bson:"drug_code"`
	Name       string  `bson:"name"`
	UnitPrice  float64 `bson:"unit_price"`
	TotalStock int32   `bson:"total_stock"`
}

func normalizeAMQPURI(uri string) string {
	if strings.HasSuffix(uri, "/medcore_vhost") && !strings.HasSuffix(uri, "/%2Fmedcore_vhost") && !strings.HasSuffix(uri, "//medcore_vhost") {
		return strings.TrimSuffix(uri, "/medcore_vhost") + "/%2Fmedcore_vhost"
	}
	return uri
}

func (s *server) CheckDrugAvailability(ctx context.Context, req *pb.CheckDrugRequest) (*pb.CheckDrugResponse, error) {
	time.Sleep(2 * time.Second)
	log.Printf("[gRPC Server] Memeriksa stok obat: %s, jumlah diminta: %d", req.GetDrugCode(), req.GetQuantityNeeded())

	if req.GetQuantityNeeded() > 5000 {
		return nil, status.Errorf(codes.ResourceExhausted, "Permintaan melebihi batas kuota farmasi")
	}

	coll :=
		s.mongoClient.Database("medcore_pharmacy_db").Collection("drugs")

	var drug DrugDoc
	err := coll.FindOne(ctx, bson.M{"drug_code": req.GetDrugCode()}).Decode(&drug)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, status.Errorf(codes.NotFound, "Obat dengan kode %s tidak ditemukan dalam inventaris", req.GetDrugCode())
		}
		return nil, status.Errorf(codes.Internal, "Database error: %v", err)
	}

	if drug.TotalStock < req.GetQuantityNeeded() {
		return &pb.CheckDrugResponse{
			DrugCode:     drug.DrugCode,
			IsAvailable:  false,
			CurrentStock: drug.TotalStock,
			UnitPrice:    drug.UnitPrice,
			Message:      fmt.Sprintf("Stok tidak mencukupi. Tersedia: %d, Dibutuhkan: %d", drug.TotalStock, req.GetQuantityNeeded()),
		}, nil
	}

	return &pb.CheckDrugResponse{
		DrugCode:     drug.DrugCode,
		IsAvailable:  true,
		CurrentStock: drug.TotalStock,
		UnitPrice:    drug.UnitPrice,
		Message:      "Stok obat mencukupi",
	}, nil
}

func main() {
	amqpURI := os.Getenv("RABBITMQ_URI")
	if amqpURI == "" {
		amqpURI = "amqp://admin_medcore:SecuredPassRabbit2026!@localhost:5672/%2Fmedcore_vhost"
	}
	amqpURI = normalizeAMQPURI(amqpURI)

	// Koneksi ke MongoDB kontainer
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	mongoURI := os.Getenv("MONGO_URI")
	if mongoURI == "" {
		mongoURI = "mongodb://adm_pharmacy_svc:SecuredPassPharm2026!@localhost:27017/medcore_pharmacy_db?authSource=admin"
	}
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoURI))
	if err != nil {
		log.Fatalf("Gagal terhubung ke MongoDB: %v", err)
	}

	go consumer.StartPrescriptionConsumer(amqpURI, client)
	go startRESTServer(client)

	grpcPort := os.Getenv("GRPC_PORT")
	if grpcPort == "" {
		grpcPort = "50051"
	}

	lis, err := net.Listen("tcp", ":"+grpcPort)
	if err != nil {
		log.Fatalf("Gagal membuka port %s: %v", grpcPort, err)
	}

	s := grpc.NewServer()
	pb.RegisterPharmacyServiceServer(s, &server{mongoClient: client})

	log.Println("==================================================")
	log.Printf("MedCore Pharmacy gRPC Service aktif pada port :%s", grpcPort)
	log.Println("==================================================")
	if err := s.Serve(lis); err != nil {
		log.Fatalf("Gagal menjalankan gRPC server: %v", err)
	}
}

func startRESTServer(mongoClient *mongo.Client) {
	restPort := os.Getenv("REST_PORT")
	if restPort == "" {
		restPort = "8081"
	}

	http.HandleFunc("/api/v1/drugs/check", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodPost {
			http.Error(w, "Metode not allowed", http.StatusMethodNotAllowed)
			return
		}

		drugCode := r.URL.Query().Get("drug_code")
		coll := mongoClient.Database("medcore_pharmacy_db").Collection("drugs")

		var drug DrugDoc
		err := coll.FindOne(r.Context(), bson.M{"drug_code": drugCode}).Decode(&drug)
		if err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]string{"error": "Obat tidak ditemukan"})
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"drug_code":     drug.DrugCode,
			"is_available":  drug.TotalStock >= 10,
			"current_stock": drug.TotalStock,
			"unit_price":    drug.UnitPrice,
			"message":       "Stok obat mencukupi",
		})
	})

	log.Printf("MedCore Pharmacy REST API aktif pada port :%s", restPort)
	http.ListenAndServe(":"+restPort, nil)
}
