import { Kafka, Producer, logLevel, LogEntry } from 'kafkajs';
import { client, protobuf as proto } from '@engyalo/kafka-ts';
import { TestEvent } from '../interfaces';

/*
		producer dependencies
		1.- kafka
		2.- keySerializer -> 
			1.- StringSerializer
			2.- JsonSerializer
			3.- MultiSerializer
		3.- valueSerializer
			1.- StringSerializer
			2.- JsonSerializer
			3.- MultiSerializer
			A.- If profobuf, then protobufSerializer -> 
				1.- schemaResolver -> MessageNameSchemaResolver || TopicNameSchemaResolver
					1.- root < from schemas
					2.- searchSchemaNamespace: com.yalo.schemas.events.*
					3.- protoIndexes, this array it's a map from 'schema namespace' to 'proto file' a method included in schemas library
					4.- serializationType: ValueSerialization | KeySerialization < enum
					5.- registry -> SchemaRegistryClient -> confluent's constructor
					6.- subjectResolver : only in case of using MessageNameSchemaResolver, this must be implemented in way that returns a protobuf schema depending of the topic
		4.- topicResolver -> can be null --optional-
			1.- This can be a MessageNameTopicResolver, it's a method --not included-- that resolves the topic name based of protobuf schema and/or passed a defaultTopic in the case that wasn't resolved
	*/

/*

	1.- Test with JSON string only --
*/
const date = new Date();
const myTestEvent: TestEvent = {
	correlationId: 'abc',
	eventName: 'myDummyEvent',
	workflowId: 'workflow-id',
	workflow: {
		_id: '123',
		name: 'dummy-workflow',
		language: 'es',
		stepSequence: 2,
		status: 'ACTIVE',
		createdAt: date,
		updatedAt: date,
		publishedAt: date,
	},
};

export async function main(): Promise<void> {
	const kafka = new Kafka({
		clientId: 'client1',
		brokers: ['localhost:9092'],
		logLevel: logLevel.INFO,
		logCreator: () => (entry: LogEntry) => {
			console.log(entry);
		},
	});
	const serializer = new client.JsonSerializer();
	const producer = await setupProducerInjected(kafka.producer(), serializer);

	await producer.sendToTopic('plainJson', 'key', myTestEvent);
	console.log('message should be sent!');
	await producer.disconnect();
}

// eslint-disable-next-line @typescript-eslint/ban-types
export async function setupProducerInjected<T extends object>(producer: Producer, serializer: client.Serializer<T>): Promise<client.Producer<string, proto.ProtobufAlike<T>>> {
	const p = new client.Producer<string, proto.ProtobufAlike<T>>(
		producer, // 1.- kafka producer
		new client.StringSerializer(), // 2.- keySerializer
		serializer, // 3.- JsonSerializer
		undefined
	);
	// connect the producer
	await p.connect();
	// return the producer
	return p;
}
