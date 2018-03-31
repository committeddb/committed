# Use goreman to run `go get github.com/mattn/goreman`
# ./committed1: committed --cluster http://127.0.0.1:8080 --port 8080
committed1: ./committed --cluster http://127.0.0.1:8080,http://127.0.0.1:8081,http://127.0.0.1:8082 --port 8080
committed2: ./committed --cluster http://127.0.0.1:8080,http://127.0.0.1:8081,http://127.0.0.1:8082 --port 8081
committed3: ./committed --cluster http://127.0.0.1:8080,http://127.0.0.1:8081,http://127.0.0.1:8082 --port 8082
