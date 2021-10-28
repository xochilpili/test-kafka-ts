import kafkajs, { Kafka, logLevel, LogEntry, KafkaConfig } from 'kafkajs';
import { client, protobuf as proto } from '@engyalo/kafka-ts';
import * as mainConsumer from '../src/consumer/proto_consumer';

describe('Proto Consumer Test', () => {
	let kafkaConsumer: any, kafkaConsumerConnect: any, kafkaConsumerSubscribe: any, kafkaConsumerRun: any, kafkaConsumerRunEach: any, kafkaConsumerCommitOffsets: any, kafkaConsumerDisconnect: any;
	let kafkaProducer: any;
	let kafka: Kafka;

	beforeAll(() => {
		const mockKafka = jest.spyOn(kafkajs, 'Kafka').mockImplementationOnce(() => ({
			constructor: (config: KafkaConfig) => jest.fn().mockReturnValue(config),
			producer: (kafkaProducer = jest.fn().mockResolvedValue(Promise.resolve())),
			consumer: (kafkaConsumer = jest.fn().mockImplementation(() => ({
				connect: (kafkaConsumerConnect = jest.fn().mockResolvedValue(true as any)),
				subscribe: (kafkaConsumerSubscribe = jest.fn().mockResolvedValue(true as any)),
				run: jest.fn().mockImplementation((args) => {
					kafkaConsumerRunEach = args;
				}),
				events: {
					GROUP_JOIN: 'GROUP_JOIN',
				},
				commitOffsets: (kafkaConsumerCommitOffsets = jest.fn().mockResolvedValue(true as any)),
				disconnect: (kafkaConsumerDisconnect = jest.fn().mockResolvedValue(true as any)),
			}))),
			admin: jest.fn().mockResolvedValue(true as any),
			logger: jest.fn().mockResolvedValueOnce(Promise.resolve()),
		}));

		kafka = new Kafka({
			clientId: 'fake-client-id',
			brokers: ['fake-broker:9091'],
			logLevel: logLevel.INFO,
			logCreator: () => (event: LogEntry) => {
				console.log(event);
			},
		});
	});

	afterAll(async () => {
		jest.restoreAllMocks();
	});

	it('should create a kafka consumer instance', async () => {
		const consumer = kafka.consumer({ groupId: 'fake-group-id' });
		await consumer.connect();
		await consumer.subscribe({ topic: 'fake-topic', fromBeginning: true });
		await consumer.disconnect();
		expect(kafkaConsumerConnect).toHaveBeenCalled();
		expect(kafkaConsumerSubscribe).toHaveBeenCalledWith({ topic: 'fake-topic', fromBeginning: true });
		expect(kafkaConsumerDisconnect).toHaveBeenCalledWith();
	});

	it('should setup a consumer', async () => {
		const messageHandler = jest.fn().mockImplementation((payload: client.MessagePayload<string, proto.ProtobufAlike<any>>) => jest.fn().mockResolvedValue(payload));
		const deadLetterHandler = jest.fn().mockImplementation((payload: client.MessagePayload<string, proto.ProtobufAlike<any>>) => jest.fn().mockResolvedValue(payload));
	});
});
