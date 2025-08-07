# Bullboard Server

## How does it work?
If you use BullMQ with a service, you can get bull board for it, by simply starting
this server and plugging it into the same Redis.

For configuration, there are the following env variables:

- `REDIS_URL` - must be defined
- `QUEUES` - csv queue names, case sensitive, empty for auto detection
- `QUEUE_PREFIX` - by default, queues have the prefix `bull`, usually not changed
- `PORT` - where to serve the board

## Local Development
This project uses bun.

To install dependencies:

```bash
bun install
```

To run:

```bash
bun run index.ts
```
