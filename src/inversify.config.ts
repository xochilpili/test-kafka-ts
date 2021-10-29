import { Container } from 'inversify';
// import { KAFKA_TYPES } from './ioc/types/kafka_types';
import { KafkaManager } from './ioc/kafka-manager';
import { kafkaModule } from './ioc/modules/kafka_module';
import { KAFKA_MANAGER_TYPES } from './ioc/types/kafka-manager_types';
import { IKafkaManager } from './interfaces/index';

const appContainer = new Container({
	defaultScope: 'Request',
	autoBindInjectable: true,
});

appContainer.load(kafkaModule);
appContainer.bind<IKafkaManager>(KAFKA_MANAGER_TYPES.KafkaManager).to(KafkaManager).inSingletonScope();

export { appContainer };
