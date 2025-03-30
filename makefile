proto:	
	protoc  -I ./minimaltest \
		--go_out=./minimaltest --go_opt=paths=source_relative \
		--go-grpc_out=./minimaltest --go-grpc_opt=paths=source_relative \
		./minimaltest/minimaltest.proto

clean:
	rm -f *~ */*~
