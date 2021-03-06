import { SchemaRegistry } from '@kafkajs/confluent-schema-registry';
import { root, com, mapProtoFiles, populateMetadata } from '@engyalo/schemas';
import { client, protobuf as proto } from '@engyalo/kafka-ts';
import { TestEvent } from '../interfaces';
import { Kafka, Producer, logLevel, LogEntry } from 'kafkajs';
import { Type } from 'protobufjs';
import { createEvent, populateSource } from '../shared/utils';
// import { Type } from 'protobufjs';

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


Strategies

	TopicName: <TopicName>...protobuf.file</TopicName> - using this strategy, then SchemaResolver should use TopicNameSchemaResolver
	RecordName: <fully.qualified.name>...protobuf.file</fully.qualified.name> - using this strategy, then should use MessageNameSchemaResolver


*/

/* test using protobuf  */

// eslint-disable-next-line @typescript-eslint/ban-types
class SubjectResolver<T extends object> implements proto.SubjectResolver<T> {
	resolveSubject(topic: string, _msg: proto.ProtobufAlike<T>, _serializationType: client.SerializationType): string {
		switch (topic) {
			case 'test1':
				return 'com.yalo.schemas.events.applications.PublishWorkflowEvent';
			default:
				throw new Error('unexpected topic ');
		}
	}
}

// eslint-disable-next-line @typescript-eslint/ban-types
function constructSchemaResolver<T extends object>(): proto.MessageNameSchemaResolver<T> {
	const registry = new SchemaRegistry({ host: 'http://localhost:8081' });
	const protoIndexes = new Map<string, number[]>();

	mapProtoFiles().forEach((filePath) => {
		filePath.forEach((indexes, type) => {
			protoIndexes.set(type, indexes);
		});
	});

	const schmaResolver = new proto.MessageNameSchemaResolver<T>(
		root,
		'com.yalo.schemas.events.applications',
		protoIndexes,
		client.SerializationType.ValueSerialization,
		registry,
		new SubjectResolver<proto.ProtobufAlike<com.yalo.schemas.events.applications.IPublishWorkflowEvent>>()
	);

	return schmaResolver;
}

// eslint-disable-next-line @typescript-eslint/ban-types
function constructSerializer<T extends object>(schemaResolver: proto.MessageNameSchemaResolver<T>): proto.ProtobufSerializer<T> {
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

	await producer.sendToTopic('test1', 'key', createEvent());
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
		undefined
	);
	await producer.connect();
	return producer;
}
