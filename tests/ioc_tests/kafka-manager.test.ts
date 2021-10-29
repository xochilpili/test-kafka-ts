import 'reflect-metadata';
import { KAFKA_TYPES } from './../../src/ioc/types/kafka_types';
import { appContainer } from '../../src/inversify.config';
import { Kafka, Producer } from 'kafkajs';
import { KAFKA_MANAGER_TYPES } from './../../src/ioc/types/kafka-manager_types';
import { IKafkaManager } from './../../src/interfaces/index';

describe('IoC Kafka Manager Tests', () => {
	let kafkaProducerConnect: any;
	let kafkaProducerSend: any;
	let kafkaProducerDisconnect: any;
	let mockKafkaProducer: any;
	//let kafka: Kafka;
	beforeAll(() => {
		mockKafkaProducer = jest.spyOn(Kafka.prototype, 'producer').mockImplementation(
			() =>
				({
					connect: (kafkaProducerConnect = jest.fn().mockResolvedValue(true)),
					send: (kafkaProducerSend = jest.fn().mockResolvedValue([{ partition: 1, baseOffset: 1 }])),
					disconnect: (kafkaProducerDisconnect = jest.fn().mockResolvedValue(true)),
				} as any)
		);
	});

	afterAll(() => {
		jest.restoreAllMocks();
	});

	it('should mock kafka injected to module', async () => {
		appContainer.rebind<Producer>(KAFKA_TYPES.KafkaProducer).toConstantValue(new Kafka({ clientId: 'a', brokers: ['n'] }).producer());
		const kafkaManager = appContainer.get<IKafkaManager>(KAFKA_MANAGER_TYPES.KafkaManager);
		await kafkaManager.connect();
		await kafkaManager.sendToTopic('topic', { name: 'my-value' } as any);
		expect(mockKafkaProducer).toHaveBeenCalled();
		expect(kafkaProducerConnect).toHaveBeenCalled();
		expect(kafkaProducerSend).toHaveBeenCalledWith({ messages: [{ value: Buffer.from(JSON.stringify({ name: 'my-value' }), 'utf-8') }], topic: 'topic' });
	});
});
