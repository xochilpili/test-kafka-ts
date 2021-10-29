import { Kafka, Message, Producer } from 'kafkajs';
import { inject, injectable } from 'inversify';
import { IKafkaManager, TestEvent } from 'src/interfaces';
import { KAFKA_TYPES } from './types/kafka_types';
import { client, protobuf as proto } from '@engyalo/kafka-ts';

@injectable()
export class KafkaManager implements IKafkaManager {
	constructor(@inject(KAFKA_TYPES.KafkaProducer) private producer: Producer) {
		//console.log('Testing kafka producer injected', this.producer); << if you see mockConstructor means that worked...
	}

	async connect(): Promise<void> {
		await this.producer.connect();
	}

	async sendToTopic(topic: string, value: proto.ProtobufAlike<TestEvent>): Promise<void> {
		console.log('send to topic', topic, value);
		const msg: Message = {
			value: Buffer.from(JSON.stringify(value), 'utf-8'),
		};
		await this.producer.send({ topic, messages: [msg] });
	}
}
