## Build

### Build image

`docker build -t life_cell:latest .`

## Run

### Start kafka cluster

(from `docker` dir)
- `docker compose --profile kafka up -d`

### Create kafka topics

(from `docker` dir)
- `docker compose --profile topics up`

### Send initial state

`cargo run --bin send_start -- --kafka-brokers localhost:9092,localhost:9093,localhost:9094 --rust-log debug --game-size 10x10`

### Start life cells

(from `docker` dir)
- `docker compose --profile life_game up -d`
