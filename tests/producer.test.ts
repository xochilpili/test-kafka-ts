import kafkajs, { Kafka, KafkaConfig, LogEntry, logLevel } from 'kafkajs';
import { setupProducerInjected } from '../src/producer/producer';
import { client } from '@engyalo/kafka-ts';

describe('Producer Test', () => {
	let mockKafka;
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
	});

	it('should create a producer instance', async () => {
		const producer = kafka.producer();
		await producer.connect();
		expect(kafkaProducer).toHaveBeenCalled();
		expect(kafkaProducerConnect).toHaveBeenCalled();
	});

	it('should setup a producer', async () => {
		const mockJsonSerializer = jest.fn().mockImplementationOnce(() => ({
			serialize: jest.fn().mockReturnValue({ name: 'fake-value' }),
		}));

		await setupProducerInjected(kafka.producer(), mockJsonSerializer());
		expect(kafkaProducer).toHaveBeenCalled();
		expect(kafkaProducerConnect).toHaveBeenCalled();
	});

	it('should send message to topic', async () => {
		const keySerializer = Buffer.from('key', 'utf-8');
		const valueSerializer = Buffer.from(JSON.stringify({ name: 'fake-value-1' }), 'utf-8');
		const expected = {
			messages: [
				{
					key: keySerializer,
					value: valueSerializer,
				},
			],
			topic: 'test1',
		};
		const producer = await setupProducerInjected(kafka.producer(), new client.JsonSerializer());
		await producer.sendToTopic('test1', 'key', { name: 'fake-value-1' });
		expect(kafkaProducer).toHaveBeenCalled();
		expect(kafkaProducerConnect).toHaveBeenCalled();
		expect(kafkaProducerSend).toHaveBeenCalledWith(expected);
	});
});
