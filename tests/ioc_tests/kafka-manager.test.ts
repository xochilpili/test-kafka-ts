import 'reflect-metadata';
import { KAFKA_TYPES } from './../../src/ioc/types/kafka_types';
import { appContainer } from '../../src/inversify.config';
import { Kafka, Producer } from 'kafkajs';
import { KAFKA_MANAGER_TYPES } from './../../src/ioc/types/kafka-manager_types';
import { PRODUCER_TYPES } from './../../src/ioc/types/producer_types';
import { IKafkaManager, TestEvent } from './../../src/interfaces/index';
import * as schemaRegistry from '@kafkajs/confluent-schema-registry';
import { protobuf as proto } from '@engyalo/kafka-ts';

describe('IoC Kafka Manager Tests', () => {
	let kafkaProducerConnect: any;
	let kafkaProducerSend: any;
	let kafkaProducerDisconnect: any;
	let mockKafkaProducer: any;
	let mockSchemaRegistry: any, registryGetSchema: any, registryGetLatestId: any;

	beforeAll(() => {
		mockKafkaProducer = jest.spyOn(Kafka.prototype, 'producer').mockImplementation(
			() =>
				({
					connect: (kafkaProducerConnect = jest.fn().mockResolvedValue(true)),
					send: (kafkaProducerSend = jest.fn().mockResolvedValue([{ partition: 1, baseOffset: 1 }])),
					disconnect: (kafkaProducerDisconnect = jest.fn().mockResolvedValue(true)),
				} as any)
		);
		mockSchemaRegistry = jest.spyOn(schemaRegistry, 'SchemaRegistry').mockImplementation(
			(config) =>
				({
					getSchema: (registryGetSchema = jest.fn().mockReturnValue(true as any)),
					getLatestSchemaId: (registryGetLatestId = jest.fn().mockReturnValue(1)),
				} as any)
		) as any;
	});

	afterAll(() => {
		jest.restoreAllMocks();
	});

	/*
	
	// This part is commented becuase was a test for KafkaManager with constructor:
	//	constructor(@inject(Kafka) private kafka: Kafka){} 
	//	for testing proposes
	
	it('should mock kafka injected to module', async () => {
		appContainer.rebind<Producer>(KAFKA_TYPES.KafkaProducer).toConstantValue(new Kafka({ clientId: 'a', brokers: ['n'] }).producer());
		appContainer.rebind<schemaRegistry.SchemaRegistry>(KAFKA_TYPES.KafkaSchemaRegisterClient).toConstantValue(new schemaRegistry.SchemaRegistry({ host: 'http://fake-url:8081' }));
		const kafkaManager = appContainer.get<IKafkaManager>(KAFKA_MANAGER_TYPES.KafkaManager);

		await kafkaManager.connect();
		await kafkaManager.sendToTopic('topic', { name: 'my-value' } as any);

		expect(mockKafkaProducer).toHaveBeenCalled();
		expect(mockSchemaRegistry).toHaveBeenCalled();
		expect(mockSchemaRegistry).toHaveBeenCalledWith({ host: 'http://fake-url:8081' });
		expect(kafkaProducerConnect).toHaveBeenCalled();
		expect(kafkaProducerSend).toHaveBeenCalledWith({ messages: [{ value: Buffer.from(JSON.stringify({ name: 'my-value' }), 'utf-8') }], topic: 'topic' });
	});
	
	*/

	it('should mock kafka when injected to kafkaManager', async () => {
		const mockDate = new Date('2021-01-01T00:00:00.000Z');
		Date.now = jest.fn(() => mockDate.getTime());
		appContainer.rebind<schemaRegistry.SchemaRegistry>(KAFKA_TYPES.KafkaSchemaRegisterClient).toConstantValue(new schemaRegistry.SchemaRegistry({ host: 'http://fake-registry:8081' }));
		appContainer.rebind<Producer>(KAFKA_TYPES.KafkaProducer).toConstantValue(new Kafka({ clientId: 'fake-client', brokers: ['fake-broker:9092'] }).producer());
		const kafkaManager = appContainer.get<IKafkaManager>(KAFKA_MANAGER_TYPES.KafkaManager);

		const fakeEvent = {
			eventName: 'publishedWorkflow',
			correlationId: 'correlationId',
			workflowId: 'workflow-id',
			workflow: {
				_id: '1234',
				name: 'workflow-name',
				language: 'es',
				stepSequence: 1,
				status: 'ACTIVE',
				createdAt: mockDate,
				updatedAt: mockDate,
				publishedAt: mockDate,
			},
		};

		const [strSerializer, protoSerializer, jsonSerializer] = appContainer.getAll<proto.ProtobufSerializer<any>>(PRODUCER_TYPES.Serializer);

		const m = await protoSerializer.serialize('test1', { ...fakeEvent });

		const expected = {
			messages: [
				{
					key: Buffer.from('key', 'utf-8'),
					value: m,
				},
			],
			topic: 'test1',
		};
		await kafkaManager.connect();
		await kafkaManager.sendToTopic('test1', { ...fakeEvent });

		expect(mockKafkaProducer).toHaveBeenCalled();
		expect(mockSchemaRegistry).toHaveBeenCalled();
		expect(mockSchemaRegistry).toHaveBeenCalledWith({ host: 'http://fake-registry:8081' });
		expect(kafkaProducerConnect).toHaveBeenCalled();
		expect(kafkaProducerSend).toHaveBeenCalledWith(expected);
		expect(kafkaProducerDisconnect).toHaveBeenCalledTimes(1);
	});
});
