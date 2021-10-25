import { TestEvent } from './../src/interfaces/index';
import kafkajs, { Kafka, KafkaConfig, LogEntry, logLevel } from 'kafkajs';
import * as schemaRegistry from '@kafkajs/confluent-schema-registry';
import { root, populateMetadata } from '@engyalo/schemas';
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
					getLatestSchemaId: (schemaGetLatestId = jest.fn().mockReturnValue([0])),
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
		protoIndexes.set('com.yalo.schemas.events.applications.PublishWorkflowEvent', [0]);
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
		/*const fakeEvent: proto.ProtobufAlike<TestEvent> = {
			eventName: 'publishedWorkflow',
			correlationId: 'correlation-id',
			workflowId: 'workflow-id',
			workflow: {
				_id: '123',
				name: 'fake-workflow',
				language: 'es',
				stepSequence: 2,
				status: 'ACTIVE',
				createdAt: new Date(),
				updatedAt: new Date(),
				publishedAt: new Date(),
			},
		};
		const fakeEventMeta = {
			metadata: {
				source: {},
				timestamp: {
					nanos: 0,
					seconds: '1609459200',
				},
			},
			...fakeEvent,
			id: '123',
			partnerId: '',
			customerId: '',
			channels: [],
			sessionTimeHrs: 0,
			activities: [],
			scopes: [],
			triggers: [],
		};*/
		const keySerializer = Buffer.from('key', 'utf-8');
		const valueSerializer = Buffer.from(JSON.stringify({ name: 'test' }), 'utf-8');
		const expected = {
			messages: [
				{
					key: keySerializer,
					value: valueSerializer,
				},
			],
			topic: 'fake-topic',
		};
		const producer = await setupProtoProducer(kafka.producer(), mockProtoSerializer);
		await producer.sendToTopic('fake-topic', 'key', { name: 'test' });
		expect(kafkaProducer).toHaveBeenCalled();
		expect(kafkaProducerConnect).toHaveBeenCalled();
		expect(kafkaProducerSend).toHaveBeenCalledWith(expected);
	});
});
