import { TestEvent } from './../interfaces/index';
import { Kafka, Consumer, logLevel, LogEntry } from 'kafkajs';
import { client, protobuf as proto } from '@engyalo/kafka-ts';

/*

	Consumer dependencies:
	1.- kafka consumer
	2.- params: { messageHandler, deadLetterHandler},
	3.- keyDeserializer: 
	4.- valueDeserializer: JsonDeserializer ( in this particular exercise);
	
*/

function createKafka() {
	const kafka = new Kafka({
		clientId: 'client1',
		brokers: ['localhost:9092'],
		logLevel: logLevel.INFO,
		logCreator: () => (entry: LogEntry) => {
			console.log(entry);
		},
	});
	return kafka;
}
// eslint-disable-next-line @typescript-eslint/ban-types
async function messageHandler<T extends object>(payload: client.MessagePayload<string, proto.ProtobufAlike<T>>): Promise<void> {
	console.log(`Messge: ${JSON.stringify(payload)}`);
	console.log(`Type of message: ${payload?.value?.constructor?.name}`);
}
// eslint-disable-next-line @typescript-eslint/ban-types
async function deadLetterHandler<T extends object>(payload: client.MessagePayload<string, proto.ProtobufAlike<T>>): Promise<void> {
	console.log(`Dead letter: ${JSON.stringify(payload)}`);
}

export async function main() {
	const kafkaConsumer = createKafka().consumer({ groupId: 'group-id' });
	const consumer = await setupConsumer<TestEvent>(kafkaConsumer, 'plainJson', messageHandler, deadLetterHandler, new client.JsonDeserializer<TestEvent>());
	const signalTraps = ['SIGTERM', 'SIGINT', 'SIGUSR2'];
	signalTraps.forEach((type) => {
		process.once(type, async () => {
			try {
				console.log('Disconnected consumer');
				await consumer.disconnect();
			} finally {
				process.kill(process.pid, type);
			}
		});
	});
	// running the consumer
	await consumer.run();
}
// eslint-disable-next-line @typescript-eslint/ban-types
export async function setupConsumer<T extends object>(
	kafkaConsumer: Consumer,
	topic: string,
	messageHandler: (payload: client.MessagePayload<string, proto.ProtobufAlike<T>>) => Promise<void>,
	deadLetterHandler: (payload: client.MessagePayload<string, proto.ProtobufAlike<T>>) => Promise<void>,
	valueDeserializer: client.JsonDeserializer<T>
) {
	const consumer = new client.Consumer(
		kafkaConsumer,
		{
			messageHandler,
			deadLetterHandler,
		},
		new client.StringDeserializer(),
		valueDeserializer
	);
	await consumer.connect();
	await consumer.subscribe(topic, true);
	return consumer;
}
