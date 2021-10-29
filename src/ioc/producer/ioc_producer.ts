import { KafkaManager } from './../kafka-manager';
import { IKafkaManager } from './../../interfaces/index';
import { appContainer } from '../../inversify.config';

export async function main(): Promise<void> {
	const kafkaManager = appContainer.get<IKafkaManager>(KafkaManager);

	kafkaManager.sendToTopic('fake-topic', {
		eventName: 'fake-evnt',
		workflowId: 'workflowId',
		workflow: {
			_id: 'id',
			name: 'name',
		} as any,
	} as any);
}
