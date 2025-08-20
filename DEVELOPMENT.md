
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

## Testing
There are two kind of tests. Doc and unit tests and integration tests
requiring a running redis. All integration tests should depend on redis and named
with a `redis_` prefix. By default redis is expected and all tests are run.
If you want only to test non-redis tests, run `cargo test -- --skip redis`.  
If you only want to run integration tests run `cargo test redis`.
