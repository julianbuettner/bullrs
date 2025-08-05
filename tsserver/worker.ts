import { Queue, Worker } from 'bullmq';

// Create a new connection in every instance
const myQueue = new Queue('myqueue', {
    connection: {
        host: '127.0.0.1',
        port: 6379,
    },
});

const myWorker = new Worker('pinkpony', async job => {
    console.log(`Received job ${job.name}: ${JSON.stringify(job)} `);
    await job.updateProgress("Some Progress is happening here...");
    await job.log("Something happened here");
    throw new Error();
    return 42;
}, {
    connection: {
        host: '127.0.0.1',
        port: 6379,
    },
});

async function main() {
    console.log("Worker should be running...");
}

main()
