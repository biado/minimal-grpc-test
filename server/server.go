package main

import (
        "context"
        "database/sql"
        "fmt"
        "log"
        "net"
        //"net/http"

	//"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
        //"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc"

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

        return &pb.PairResponse{
                Pair: &insertedPair,
        }, nil
}

func (s *MinimalTestServer) GetPair(ctx context.Context, request *pb.GetPairRequest) (*pb.PairResponse, error) {
        row := s.db.QueryRow("SELECT * FROM public.kv WHERE key = $1", request.Id)

        var existingPair pb.Pair
        err := row.Scan(&existingPair.Id, &existingPair.Value)
        if err != nil && err != sql.ErrNoRows {
                return &pb.PairResponse{ErrorMessage: fmt.Sprintf("Failed to fetch media from database: %s", err)}, nil
	}

        return &pb.PairResponse{
                Pair: &existingPair,
        }, nil
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

        // Create a TCP listener for the gRPC server
        lis, err := net.Listen("tcp", fmt.Sprintf("%s:%d", sv_host, sv_port))
        if err != nil {
                log.Fatalf("failed to listen: %v", err)
        }

	// Create and register the implementation of the gRPC server
        grpc_server := grpc.NewServer()
        pb.RegisterMinimalTestServer(grpc_server, server)
        log.Println("gRPC server listening on port 50051")
        if err := grpc_server.Serve(lis); err != nil {
                log.Fatalf("failed to serve: %v", err)
        }

        // // Create a client connection to the gRPC server we just started
        // // This is where the gRPC-Gateway proxies the requests
        // conn, err := grpc.NewClient(
        //         "0.0.0.0:50001",
        //         grpc.WithTransportCredentials(insecure.NewCredentials()),
        // )
        // if err != nil {
        //         log.Fatalln("Failed to dial server:", err)
        // }

        // gwmux := runtime.NewServeMux()
        // err = pb.RegisterMinimalTestHandlerFromEndpoint(context.Background(), gwmux, conn)
	// if err != nil {
        //         log.Fatalln("Failed to register gateway:", err)
        // }

        // gwServer := &http.Server{
        //         Addr:    ":50001",
        //         Handler: gwmux,
        // }

        // log.Println("Serving gRPC-Gateway on http://localhost:50051")
        // log.Fatalln(gwServer.ListenAndServe())

}
