import { PrismaClient } from "@prisma/client";
import {Kafka} from "kafkajs"

const client = new PrismaClient();


const TOPIC = "zap-events"

const kafka = new Kafka({
    clientId: 'Outbox-processor',
    brokers: ['localhost:9092']
  })


async function main() {
    const producer = kafka.producer()
    await producer.connect()
    while(1){
        const pendingRows = await client.zapRunOutbox.findMany({
            where:{},
            take:10
        })
        await producer.send({
            topic:TOPIC,
            messages: pendingRows.map(x=>({value:JSON.stringify({zapRunId:x.zapRunId,stage:0})}))
        })
        await client.zapRunOutbox.deleteMany({
            where:{
                id:{
                    in:pendingRows.map(x=>x.id)
                }
            }
        })
    }
}

main();