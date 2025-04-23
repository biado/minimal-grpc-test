package main

import (
        "context"
        "database/sql"
        "fmt"
        "log"
        "net"
        "google.golang.org/grpc"
        "google.golang.org/grpc/metadata"
        //"google.golang.org/grpc/reflection"

        "net/http"
        "github.com/grpc-ecosystem/grpc-gateway/v2/runtime"

	_ "github.com/lib/pq"
        pb "minimaltest/minimaltest"
)

const (
        dbname     = "minimaltest"
        user       = "bjorn"
        pwd        = ""
        db_host    = "localhost"
        db_port    = 5432
        sv_host    = "localhost"
        sv_port    = 50051
        BATCH_SIZE = 5000
)


type MinimalTestServer struct {
        pb.UnimplementedMinimalTestServer
	db *sql.DB
}

// UnaryInterceptor to inject metadata
func metadataInterceptor(
	ctx context.Context, req interface{}, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler,
) (interface{}, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		md = metadata.New(nil)
	}
	newCtx := metadata.NewIncomingContext(ctx, md)
	return handler(newCtx, req)
}

func NewMinimalTestServer(connStr string) (*MinimalTestServer, error) {
        db, err := sql.Open("postgres", connStr)
        if err != nil {
                return nil, fmt.Errorf("failed to connect to the database: %w", err)
        }
        fmt.Println("new minimal test server created")

        // Ensure the database connection is valid
        if err := db.Ping(); err != nil {
                db.Close()
                return nil, fmt.Errorf("failed to ping the database: %w", err)
        }

        return &MinimalTestServer{
                db: db,
        }, nil
}

// Close closes the database connection.
func (s *MinimalTestServer) Close() {
        s.db.Close()
}

func (s *MinimalTestServer) PutPair(ctx context.Context, request *pb.PutPairRequest) (*pb.PairResponse, error) {
        row := s.db.QueryRow("SELECT * FROM public.kv WHERE key = $1", request.Pair.Id)

        var existingPair pb.Pair
        err := row.Scan(&existingPair.Id, &existingPair.Value)
        if err != nil && err != sql.ErrNoRows {
                return &pb.PairResponse{ErrorMessage: fmt.Sprintf("Failed to fetch media from database: %s", err)}, nil
	}

	var queryString = ""
        if err == nil {
	        queryString = "UPDATE public.kv SET value = $2 WHERE key = $1 RETURNING *;"
	} else {
	        queryString = "INSERT INTO public.kv (key, value) VALUES ($1, $2) RETURNING *;"
	}

	var insertedPair pb.Pair
        row = s.db.QueryRow(queryString, request.Pair.Id, request.Pair.Value)
        err = row.Scan(&insertedPair.Id, &insertedPair.Value)
        if err != nil {
                return &pb.PairResponse{ErrorMessage: fmt.Sprintf("Failed to insert media into database: %s", err)}, nil
        }

        return &pb.PairResponse{Pair: &insertedPair, }, nil
}

func (s *MinimalTestServer) GetPair(ctx context.Context, request *pb.GetPairRequest) (*pb.PairResponse, error) {
        row := s.db.QueryRow("SELECT * FROM public.kv WHERE key = $1", request.Id)

        var existingPair pb.Pair
        err := row.Scan(&existingPair.Id, &existingPair.Value)
        if err != nil {
                return &pb.PairResponse{ErrorMessage: fmt.Sprintf("Failed to fetch media from database: %s", err)}, nil
	}

        return &pb.PairResponse{Pair: &existingPair, }, nil
}

func runRESTServer() {
	// Start REST Gateway
        ctx := context.Background()
        ctx, cancel := context.WithCancel(ctx)
        defer cancel()

	mux := runtime.NewServeMux()
	opts := []grpc.DialOption{grpc.WithInsecure()}

        err := pb.RegisterMinimalTestHandlerFromEndpoint(context.Background(), mux, ":50051", opts)
	if err != nil {
		log.Fatalf("Failed to register gateway: %v", err)
	}

	log.Println("REST API listening on :8080")
	log.Fatal(http.ListenAndServe(":8080", mux))
}
      
      
func main() {
        conn_str := fmt.Sprintf("dbname=%s user=%s password=%s host=%s port=%d sslmode=disable",
                dbname,  // dbname
                user,    // user
                pwd,     // password
                db_host, // host
                db_port) // port

        server, err := NewMinimalTestServer(conn_str)
        if err != nil {
                log.Fatalf("Error creating server: %v", err)
        }
        defer server.Close()

	// Create and register the implementation of the gRPC server
        grpc_server := grpc.NewServer(grpc.UnaryInterceptor(metadataInterceptor), )
        pb.RegisterMinimalTestServer(grpc_server, server)


	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}
	go func() {
		log.Println("gRPC Server listening on :50051")
		if err := grpc_server.Serve(lis); err != nil {
			log.Fatalf("Failed to serve: %v", err)
		}
	}()

        runRESTServer()
}
