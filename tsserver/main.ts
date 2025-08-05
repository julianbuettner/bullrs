import { Queue, Worker } from 'bullmq';

// Create a new connection in every instance
const myQueue = new Queue('myqueue', {
    connection: {
        host: '127.0.0.1',
        port: 6379,
    },
});

// const myWorker = new Worker('myqueue', async job => {
//     console.log(`Received job ${job.name}: ${JSON.stringify(job)} `)
// }, {
//     connection: {
//         host: '127.0.0.1',
//         port: 6379,
//     },
// });

async function main() {
    await myQueue.setGlobalConcurrency(32);
    const job = await myQueue.add("HIIII", 99, { keepLogs: 22, attempts: 99, continueParentOnFailure: true, lifo: false });
    const jobs = await myQueue.addBulk([
        { name: 'A', data: 'foobar', opts: { delay: 500 }},
        { name: 'B', data: 'foobar', opts: { delay: 500 }},
        { name: 'C', data: 'foobar', opts: { delay: 500 }},
        { name: 'D', data: 'foobar', opts: { delay: 500 }},
    ]);
    console.log(JSON.stringify(jobs));
// const job = await myQueue.add("JobX", { "a": 1, "b": 2 }, {
//     delay: 10 * 1000,
//     attempts: 33,
//     deduplication: { id: 'asddasd' },
//     keepLogs: 128,
//     lifo: false,
//     backoff: 1000000,
//     priority: 33,
//     debounce: { id: 'sdasd', ttl: 10000 },
// });
// await job.changeDelay(120 * 1000);
console.log("TS Script ran to completion");
}

main()
