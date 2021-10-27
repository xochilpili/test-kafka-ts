import kafkajs, { Kafka, logLevel, LogEntry, KafkaConfig } from 'kafkajs';
import * as mainConsumer from '../src/consumer/consumer';
import { client, protobuf as proto } from '@engyalo/kafka-ts';

describe('Consumer Tests', () => {
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
		const consumer = await mainConsumer.setupConsumer(kafka.consumer({ groupId: 'fake-group-id' }), 'fake-topic', messageHandler, deadLetterHandler, new client.JsonDeserializer<any>());
		await consumer.disconnect();
		expect(kafkaConsumerConnect).toHaveBeenCalled();
		expect(kafkaConsumerSubscribe).toHaveBeenCalledWith({ topic: 'fake-topic', fromBeginning: true });
		expect(kafkaConsumerDisconnect).toHaveBeenCalled();
	});

	it('should commitOffset when handled deadLetter invalid formatted message', async () => {
		const messageHandler = jest.fn().mockImplementation((payload: client.MessagePayload<string, proto.ProtobufAlike<any>>) => jest.fn().mockResolvedValue(payload));
		const deadLetterHandler = jest.fn().mockImplementation((payload: client.MessagePayload<string, proto.ProtobufAlike<any>>) => jest.fn().mockResolvedValue(payload));
		const consumer = await mainConsumer.setupConsumer(kafka.consumer({ groupId: 'fake-group-id' }), 'fake-topic', messageHandler, deadLetterHandler, new client.JsonDeserializer<any>());
		await consumer.run();

		// fake consume BAD Json message
		await kafkaConsumerRunEach.eachMessage({
			topic: 'fake-topic',
			partition: 1,
			message: {
				value: 'BAD',
				offset: 1,
			},
		});
		expect(messageHandler).not.toHaveBeenCalled();
		expect(kafkaConsumerCommitOffsets).toBeCalled();
		expect(deadLetterHandler).toHaveBeenCalled();
		expect(deadLetterHandler).toHaveBeenCalledWith(expect.objectContaining({ offset: 1, partition: 1, topic: 'fake-topic' }));
		expect(kafkaConsumerCommitOffsets).toHaveBeenCalledWith([{ offset: 1, partition: 1, topic: 'fake-topic' }]);
	});

	it('should call messageHandler when consuming message', async () => {
		const messageHandler = jest.fn().mockImplementation((payload: client.MessagePayload<string, proto.ProtobufAlike<any>>) => jest.fn().mockResolvedValue(JSON.stringify(payload)));
		const deadLetterHandler = jest.fn().mockImplementation((payload: client.MessagePayload<string, proto.ProtobufAlike<any>>) => jest.fn().mockResolvedValue(Promise.resolve(payload)));
		const consumer = await mainConsumer.setupConsumer(kafka.consumer({ groupId: 'fake-group-id' }), 'fake-topic', messageHandler, deadLetterHandler, new client.JsonDeserializer<any>());
		// start to consume
		await consumer.run();

		// fake consume VALID Json message
		await kafkaConsumerRunEach.eachMessage({
			topic: 'fake-topic',
			partition: 1,
			message: {
				key: 'fake-key',
				value: JSON.stringify({ eventName: 'fake-event', workflowId: 'fake-workflow-id' }),
				offset: 1,
			},
		});

		await consumer.disconnect();

		expect(deadLetterHandler).not.toHaveBeenCalled();
		expect(kafkaConsumerCommitOffsets).toHaveBeenCalled();
		expect(kafkaConsumerCommitOffsets).toHaveBeenCalledWith([{ offset: 1, partition: 1, topic: 'fake-topic' }]);
		expect(messageHandler).toHaveBeenCalled();
		expect(messageHandler).toHaveBeenCalledWith(expect.objectContaining({ value: { eventName: 'fake-event', workflowId: 'fake-workflow-id' } }));
		expect(kafkaConsumerDisconnect).toHaveBeenCalled();
	});
});
