
## Setup
This project contains
- a `docker-compose.yaml` for starting a redis container
- the rust library `bullmq-rs`
- and a typescript server for testing interoperability

```
docker compose up -d

cd tsserver
npm install
npm run start

# Now you can conect to redis
docker exec -it bullmq-rs-redis-1 redis-cli
```
