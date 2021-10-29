/* import { SchemaRegistry } from '@kafkajs/confluent-schema-registry';
import { Kafka, Message, Producer } from 'kafkajs';
import { KAFKA_TYPES } from './types/kafka_types'; */
import { inject, injectable, named } from 'inversify';
import { IKafkaManager, TestEvent } from 'src/interfaces';

import { client, protobuf as proto } from '@engyalo/kafka-ts';
import { PRODUCER_TYPES } from './types/producer_types';

@injectable()
export class KafkaManager implements IKafkaManager {
	/* constructor(@inject(KAFKA_TYPES.KafkaProducer) private producer: Producer, @inject(KAFKA_TYPES.KafkaSchemaRegisterClient) private schemaRegistry: SchemaRegistry) {
		console.log('Testing kafka producer injected', this.producer); // << if you see mockConstructor when tests means that worked...
		console.log('Testing schemaRegistry injected', this.schemaRegistry); //
	} */
	constructor(@inject(PRODUCER_TYPES.Producer) @named('protobuf') private producer: client.Producer<string, proto.ProtobufAlike<TestEvent>>) {
		console.log('Testing client.producer', this); //
	}

	async connect(): Promise<void> {
		await this.producer.connect();
	}

	async sendToTopic(topic: string, value: proto.ProtobufAlike<TestEvent>): Promise<void> {
		console.log('send to topic', topic, value);
		/*
			// test to use when injected KafkaProducer directly instead of client.Producer
			const msg: Message = {
				value: Buffer.from(JSON.stringify(value), 'utf-8'),
			};
		 	await this.producer.send({ topic, messages: [msg] });
		*/
		await this.producer.sendToTopic(topic, 'key', value);
		await this.producer.disconnect();
	}
}
