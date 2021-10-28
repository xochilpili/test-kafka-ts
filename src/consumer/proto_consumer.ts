import { TestEvent } from './../interfaces/index';
import { Kafka, logLevel, LogEntry, Consumer } from 'kafkajs';
import { client, protobuf as proto } from '@engyalo/kafka-ts';
import { root } from '@engyalo/schemas';

function createKafka() {
	const kafka = new Kafka({
		clientId: 'client-1',
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
	console.log(`Message: ${JSON.stringify(payload)}`);
	console.log(`Message type: ${payload?.value?.constructor?.name}`);
}
// eslint-disable-next-line @typescript-eslint/ban-types
async function deadLetterHandler<T extends object>(payload: client.MessagePayload<string, proto.ProtobufAlike<T>>): Promise<void> {
	console.log((payload.error as string).toString());
	console.log(`DeadLetter: ${JSON.stringify(payload)}`);
}

function deserializer(): proto.ProtobufDeserializer {
	const fakeTypeMap = new Map<string, Map<string, number[]>>();
	const typeMap = new Map<string, number[]>();
	// fakeTypeMap.set('PublishWorkflowEvent', typeMap.set('com.yalo.schemas.events.applications.PublishWorkflowEvent', [0]));
	fakeTypeMap.set('test1', typeMap.set('com.yalo.schemas.events.applications.PublishWorkflowEvent', [0]));
	const entityResolver = new proto.MessageIndexEntityResolver<proto.ProtobufAlike<TestEvent>>(root, '', fakeTypeMap);

	return new proto.ProtobufDeserializer(entityResolver);
}

export async function main(): Promise<void> {
	const kafkaConsumer = createKafka().consumer({ groupId: 'group-ir' });
	const consumer = await setupProtoConsumer<proto.ProtobufAlike<TestEvent>>(kafkaConsumer, 'test1', messageHandler, deadLetterHandler, deserializer());
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
	await consumer.run();
}
// eslint-disable-next-line @typescript-eslint/ban-types
export async function setupProtoConsumer<T extends object>(
	kafkaConsumer: Consumer,
	topic: string,
	messageHandler: (payload: client.MessagePayload<string, proto.ProtobufAlike<T>>) => Promise<void>,
	deadLetterHandler: (payload: client.MessagePayload<string, proto.ProtobufAlike<T>>) => Promise<void>,
	valueDeserializer: proto.ProtobufDeserializer
) {
	const consumer = new client.Consumer<string, proto.ProtobufAlike<T>>(kafkaConsumer, { messageHandler, deadLetterHandler }, new client.StringDeserializer(), valueDeserializer);
	await consumer.connect();
	await consumer.subscribe(topic, true);
	return consumer;
}
