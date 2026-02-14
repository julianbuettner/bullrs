import express from "express";
import { Queue as QueueMQ } from "bullmq";
import { createBullBoard } from "@bull-board/api";
import { BullMQAdapter } from "@bull-board/api/bullMQAdapter";
import { ExpressAdapter } from "@bull-board/express";
import IORedis from 'ioredis';

const PORT = parseInt(process.env.PORT ?? "3000");
const QUEUES = process.env.QUEUES ?? "";
const QUEUE_PREFIX = process.env.QUEUE_PREFIX ?? "bull"
const REDIS_URL = process.env.REDIS_URL ?? process.env.REDIS_URI;

if (!REDIS_URL) {
  throw new Error("REDIS_URL not defined. Quit.");
}

const connection = new IORedis(REDIS_URL);

function sleep(ms: number) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function queueNames() {
  if (QUEUES) {
    return QUEUES.split(",").map((s) => s.trim());
  } else {
    console.log("QUEUES csv env is not defined. Detect queues in Redis DB.");
  }

  const res = await connection.keys(`${QUEUE_PREFIX}:*:meta`);
  const q = res.map((fullName) => fullName.slice(QUEUE_PREFIX.length + 1, fullName.length - ':meta'.length));
  const nameListDebug = q.join(", ");
  console.log(`Found the followwing queues: ${nameListDebug}.`);

  return q;
}

async function main() {
  const names = await queueNames();
  const queues = names.map((name) => new QueueMQ(name, { connection }));

  const serverAdapter = new ExpressAdapter();
  serverAdapter.setBasePath("/queues");

  const { addQueue, removeQueue, setQueues, replaceQueues } = createBullBoard({
    queues: queues.map((q) => new BullMQAdapter(q)),
    serverAdapter: serverAdapter,
  });

  const app = express();

  app.use("/queues", serverAdapter.getRouter());

  const server = app.listen(PORT, () => {
    console.log(`Running on ${PORT}...`);
    console.log("For the UI, open http://localhost:3000/queues");
  });

  function shutdown() {
    server.close(() => {
      console.log('Closed out remaining connections');
      process.exit(0);
    });
  }

  process.on('SIGTERM', shutdown);
  process.on('SIGINT', shutdown);

  while (true) {
    await sleep(30000);
    const newQueueNames = await queueNames();
    for (const newQueueName of newQueueNames) {
      if (!names.includes(newQueueName)) {
        console.log(`Add newly scanned queue ${newQueueName}.`)
        const newQ = new BullMQAdapter(new QueueMQ(newQueueName, { connection }));
        addQueue(newQ);
      }
    }
  }

}

main()
