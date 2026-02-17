## Build

### Build image

`docker build -t life_cell:latest .`

## Run

### Create kafka topics

(from `docker` dir)
- `docker compose --profile setup up`

### Start life cells

(from `docker` dir)
- `docker compose --profile dev up`
