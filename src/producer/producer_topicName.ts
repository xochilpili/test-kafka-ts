import { Kafka, logLevel, LogEntry, Producer } from 'kafkajs';
import { com, mapProtoFiles, populateMetadata, root } from '@engyalo/schemas';
import { SchemaRegistry } from '@kafkajs/confluent-schema-registry';
import { client, protobuf as proto } from '@engyalo/kafka-ts';
import { createEvent, populateSource } from '../shared/utils';
import { Type } from 'protobufjs';

// eslint-disable-next-line @typescript-eslint/ban-types
class MessageNameTopicResolver<T extends object> implements client.TopicResolver<unknown, proto.ProtobufAlike<T>> {
	private defaultTopic: string;
	constructor(defaultTopic: string) {
		this.defaultTopic = defaultTopic;
	}

	public async resolveTopic(_key: unknown | null, value: proto.ProtobufAlike<T>): Promise<string> {
		if (value?.constructor?.prototype?.$type instanceof Type) {
			return value.constructor.name;
		}
		return this.defaultTopic;
	}
}

// eslint-disable-next-line @typescript-eslint/ban-types
function constructSchemaResolver<T extends object>(): proto.TopicNameSchemaResolver<T> {
	const registry = new SchemaRegistry({ host: 'http://localhost:8081' });
	const protoIndexes = new Map<string, number[]>();

	mapProtoFiles().forEach((filePath) => {
		filePath.forEach((indexes, type) => {
			protoIndexes.set(type, indexes);
		});
	});

	const schemaResolver = new proto.TopicNameSchemaResolver(root, 'com.yalo.schemas.events', protoIndexes, client.SerializationType.ValueSerialization, registry);
	return schemaResolver;
}
// eslint-disable-next-line @typescript-eslint/ban-types
function constructSerializer<T extends object>(schemaResolver: proto.TopicNameSchemaResolver<T>): proto.ProtobufSerializer<T> {
	return new proto.ProtobufSerializer(schemaResolver, (event: proto.ProtobufAlike<T>) => populateMetadata(event, populateSource));
}

export async function main(): Promise<void> {
	const kafka = new Kafka({
		clientId: 'client1',
		brokers: ['localhost:9092'],
		logLevel: logLevel.INFO,
		logCreator: () => (entry: LogEntry) => {
			console.log(entry);
		},
	});
	const schemaResolver = constructSchemaResolver<proto.ProtobufAlike<com.yalo.schemas.events.applications.IPublishWorkflowEvent>>();
	const protoSerializer = constructSerializer<proto.ProtobufAlike<com.yalo.schemas.events.applications.IPublishWorkflowEvent>>(schemaResolver);
	const producer = await setupProtoProducer<proto.ProtobufAlike<com.yalo.schemas.events.applications.IPublishWorkflowEvent>>(kafka.producer(), protoSerializer);
	//

	await producer.send(null, createEvent());
	console.log('message sent');
	await producer.disconnect();
	console.log('disconnect producer');
}

// eslint-disable-next-line @typescript-eslint/ban-types
export async function setupProtoProducer<T extends object>(kafkaProducer: Producer, protobufSerializer: proto.ProtobufSerializer<T>): Promise<client.Producer<string, T>> {
	const producer = new client.Producer<string, proto.ProtobufAlike<com.yalo.schemas.events.applications.IPublishWorkflowEvent>>(
		kafkaProducer,
		new client.StringSerializer(),
		protobufSerializer,
		new MessageNameTopicResolver<T>('PubishWorkflowEvent')
	);
	await producer.connect();
	return producer;
}
