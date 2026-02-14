
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
You can use `cargo test`, but the result is more compact with `cargo nextest`.
If you want only to test non-redis tests, run `cargo nextest run -- --skip redis`.  
If you only want to run integration tests run `cargo nextest run redis`.  
Tests containing `time` indicate that they test time-sensitive stuff based on timers.
If Redis has a small block an something takes longer than expected, this might throw an
error. This should _never_ happen though. Flakey tests are reeeeaal bad.

If tests crash and they are not cleaned, reset your redis with
`docker compose down -v && docker compose up -d`.
