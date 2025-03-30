package main

import (
        "context"
        "log"
	"time"
	"flag"

	"google.golang.org/grpc"
        "google.golang.org/grpc/credentials/insecure"
        pb "minimaltest/minimaltest"
)


var (
        serverAddr         = flag.String("addr", "localhost:50051", "The server address in the format of host:port")
)


func PutPair(client pb.MinimalTestClient, pair *pb.Pair) {
        ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
        defer cancel()
        retPair, err := client.PutPair(ctx, &pb.PutPairRequest{Pair: pair})
        if err != nil {
                log.Fatalf("client.PutPair failed: %v", err)
        }
        log.Println(retPair)
}

func GetPair(client pb.MinimalTestClient, id int32) {
        ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
        defer cancel()
        retPair, err := client.GetPair(ctx, &pb.GetPairRequest{Id: id})
        if err != nil {
                log.Fatalf("client.GetPair failed: %v", err)
        }
        log.Println(retPair)
}


func main() {
     var opts[]grpc.DialOption
      opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
      
        conn, err := grpc.NewClient(*serverAddr, opts...)
        if err != nil {
                log.Fatalf("fail to dial: %v", err)
        }
        defer conn.Close()
        client := pb.NewMinimalTestClient(conn)

	PutPair(client, &pb.Pair{Id: 1, Value: "Testing"})
	PutPair(client, &pb.Pair{Id: 2, Value: "Also"})
	GetPair(client, 1)
}
