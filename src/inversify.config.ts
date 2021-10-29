import { Container } from 'inversify';
import { KafkaManager } from './ioc/kafka-manager';
import { kafkaModule } from './ioc/modules/kafka.module';
import { producerModule } from './ioc/modules/producer.module';
import { KAFKA_MANAGER_TYPES } from './ioc/types/kafka-manager_types';
import { IKafkaManager } from './interfaces/index';

const appContainer = new Container({
	defaultScope: 'Request',
	autoBindInjectable: true,
});

appContainer.load(kafkaModule, producerModule);
appContainer.bind<IKafkaManager>(KAFKA_MANAGER_TYPES.KafkaManager).to(KafkaManager).inSingletonScope();

export { appContainer };
