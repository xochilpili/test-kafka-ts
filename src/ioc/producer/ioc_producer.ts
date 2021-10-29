import { KafkaManager } from './../kafka-manager';
import { IKafkaManager } from './../../interfaces/index';
import { appContainer } from '../../inversify.config';
import { createEvent } from '../../shared/utils';

export async function main(): Promise<void> {
	const kafkaManager = appContainer.get<IKafkaManager>(KafkaManager);
	await kafkaManager.connect();
	await kafkaManager.sendToTopic('fake-topic', createEvent());
}
