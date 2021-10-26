import { TestEvent } from './../src/interfaces/index';
import kafkajs, { Kafka, KafkaConfig, LogEntry, logLevel } from 'kafkajs';
import * as schemaRegistry from '@kafkajs/confluent-schema-registry';
import { root, populateMetadata, mapProtoFiles } from '@engyalo/schemas';
import { client, protobuf as proto } from '@engyalo/kafka-ts';
import { setupProtoProducer } from '../src/producer/proto_producer';
import { SchemaRegistry } from '@kafkajs/confluent-schema-registry';

describe('Proto Producer Test', () => {
	let mockKafka;
	let mockRegistry: SchemaRegistry;
	let schemaGetSchema;
	let schemaGetLatestId;
	let kafkaProducer: any;
	let kafkaProducerConnect: any;
	let kafkaProducerSend: any;
	let kafkaProducerDisconnect: any;
	let kafka: Kafka;

	beforeAll(async () => {
		mockKafka = jest.spyOn(kafkajs, 'Kafka').mockImplementation(
			() =>
				({
					constructor: (config: KafkaConfig) => jest.fn().mockReturnValue(config),
					admin: jest.fn().mockResolvedValue(true as any),
					consumer: jest.fn().mockResolvedValue(true as any),
					producer: (kafkaProducer = jest.fn().mockImplementation(() => ({
						connect: (kafkaProducerConnect = jest.fn().mockResolvedValue(true as any)),
						send: (kafkaProducerSend = jest.fn().mockResolvedValue([{ partition: 1, baseOffset: 1 }])),
						disconnect: (kafkaProducerDisconnect = jest.fn().mockResolvedValue(true as any)),
					}))),
				} as any)
		);
		kafka = new Kafka({
			clientId: 'fake-client-id',
			brokers: ['fake-broker:9091'],
			logLevel: logLevel.INFO,
			logCreator: () => (event: LogEntry) => {
				console.log(event);
			},
		});
		mockRegistry = jest.spyOn(schemaRegistry, 'SchemaRegistry').mockImplementation(
			(config) =>
				({
					getSchema: (schemaGetSchema = jest.fn().mockReturnValue(true as any)),
					getLatestSchemaId: (schemaGetLatestId = jest.fn().mockReturnValue(1)),
				} as any)
		) as any;
	});

	afterAll(async () => {
		jest.restoreAllMocks();
	});

	it('should create a producer instance', async () => {
		const producer = kafka.producer();
		await producer.connect();
		expect(kafkaProducer).toHaveBeenCalled();
		expect(kafkaProducerConnect).toHaveBeenCalled();
	});

	it('should steup a proto producer', async () => {
		const mockProtoSerializer = jest.fn().mockImplementationOnce(() => jest.fn().mockReturnValue(true as any));
		await setupProtoProducer(kafka.producer(), mockProtoSerializer());
		expect(kafkaProducer).toHaveBeenCalled();
		expect(kafkaProducerConnect).toHaveBeenCalled();
	});

	it('should setup a proto producer', async () => {
		const mockDate = new Date('2021-01-01T00:00:00.000Z');
		jest.spyOn(global, 'Date').mockImplementation(() => mockDate as unknown as string);
		Date.now = jest.fn(() => mockDate.getTime());
		// generate fake indexes
		const protoIndexes = new Map<string, number[]>();
		protoIndexes.set('com.yalo.schemas.events.applications.PublishWorkflowEvent', [1]);
		// mock subjectResolver and return a fake FQN namespace subject
		const mockSubjectResolver = jest.fn().mockImplementation((topic: string) => ({
			resolveSubject: jest.fn().mockReturnValue('com.yalo.schemas.events.applications.PublishWorkflowEvent'),
		}));

		const registry = new SchemaRegistry({ host: 'http://fake:8081' });
		const mockSchemaResolver = new proto.MessageNameSchemaResolver(
			root,
			'com.yalo.schemas.events.applications',
			protoIndexes,
			client.SerializationType.ValueSerialization,
			registry,
			mockSubjectResolver()
		);

		const populateSource = () => {};
		// mocking protobufSerializer
		const mockProtoSerializer = new proto.ProtobufSerializer(
			mockSchemaResolver,
			(event: proto.ProtobufAlike<any>) => event
		);

		const fakeEvent: proto.ProtobufAlike<TestEvent> = {
			eventName: 'publishedWorkflow',
			correlationId: 'correlation-id',
			workflowId: 'workflow-id',
			workflow: {
				_id: '123',
				name: 'fake-workflow',
				language: 'es',
				stepSequence: 2,
				status: 'ACTIVE',
				createdAt: {
					seconds: new Date().getSeconds(),
					nanos: Date.now(),
				},
				updatedAt: {
					seconds: new Date().getSeconds(),
					nanos: Date.now(),
				},
				publishedAt: {
					seconds: new Date().getSeconds(),
					nanos: Date.now(),
				},
			},
		};
		/*
		root.lookupType(‘fully.qualified.message.Name’).encode(value) gives you the same buffer you would get if you call serializer.serialize(value), but without the header.  Similarly, 
		root.lookupType(‘fully.qualified.message.Name’).decode(buffer) will deserialize a buffer back into a protobuf.  However, the buffer must skip the header since decode(buffer) knows nothing about the header structure.
		0 value = 0
		-1 value = 1
		1 value = 2
		-2 value = 3
		2 value = 4
		[0,0,0,0,1,2,0]
		*/
		const m = await mockProtoSerializer.serialize('fake-topic', { ...fakeEvent });
		const a = root.lookupType('com.yalo.schemas.events.applications.PublishWorkflowEvent').encode(fakeEvent);

		const header = Buffer.from([0x0, 0x0, 0x0, 0x0, 0x1, 0x2, 0x2]);
		const payload = Buffer.concat([header, a.finish()]);
		const keySerializer = Buffer.from('key', 'utf-8');
		const expected = {
			messages: [
				{
					key: keySerializer,
					value: m,
				},
			],
			topic: 'fake-topic',
		};
		const producer = await setupProtoProducer(kafka.producer(), mockProtoSerializer);
		await producer.sendToTopic('fake-topic', 'key', fakeEvent);
		expect(kafkaProducer).toHaveBeenCalled();
		expect(kafkaProducerConnect).toHaveBeenCalled();
		expect(kafkaProducerSend).toHaveBeenCalledWith(expected);
		expect(m?.slice(7, m?.length)).toStrictEqual(a.finish());
		expect(payload).toStrictEqual(m);
	});
});
