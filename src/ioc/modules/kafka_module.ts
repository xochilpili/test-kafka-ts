import { Kafka, KafkaConfig, logLevel, LogEntry, Producer } from 'kafkajs';
import { ContainerModule, decorate, inject, injectable, interfaces } from 'inversify';
import { KAFKA_TYPES } from '../types/kafka_types';

export const kafkaModule = new ContainerModule((bind: interfaces.Bind) => {
	// bind kafkaConfig to a dynamic value
	bind<KafkaConfig>(KAFKA_TYPES.KafkaConfig).toDynamicValue(() => {
		const kafkaConfig: KafkaConfig = {
			clientId: 'client-id',
			brokers: ['localhost:9092'],
			logLevel: logLevel.INFO,
			logCreator: () => (entry: LogEntry) => {
				console.log('original', entry);
			},
		};

		return kafkaConfig;
	});

	// make kafka injectable
	decorate(injectable(), Kafka);
	// inject kafkaConfig to kafka's constructor as first argument; like: new Kafka(kafkaConfig);
	decorate(inject(KAFKA_TYPES.KafkaConfig), Kafka, 0);
	// bind kafka to Kafka as singleton: like const kafka = new Kafka(kafkaConfig) but in singleton
	bind<Kafka>(KAFKA_TYPES.Kafka).to(Kafka).inSingletonScope();
	bind<Producer>(KAFKA_TYPES.KafkaProducer).toDynamicValue((context: interfaces.Context) => {
		const kafka = context.container.get<Kafka>(KAFKA_TYPES.Kafka);
		return kafka.producer();
	});
});
