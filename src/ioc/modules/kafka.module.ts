import { Kafka, KafkaConfig, logLevel, LogEntry, Producer } from 'kafkajs';
import { ContainerModule, decorate, inject, injectable, interfaces, optional } from 'inversify';
import { KAFKA_TYPES } from '../types/kafka_types';
import { SchemaRegistryAPIClientArgs } from '@kafkajs/confluent-schema-registry/dist/api';
import { SchemaRegistry } from '@kafkajs/confluent-schema-registry';

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

	/*
		-- Removed since in producer_module, we're getting an instance for Kafka.producer();
	// bind a dynamic value to kafka's producer, what i dont get here is that with this we can use kafka instance as a factory:
	// need to go deep with this Factory thing...
	bind<Producer>(KAFKA_TYPES.KafkaProducer).toDynamicValue((context: interfaces.Context) => {
		const kafka = context.container.get<Kafka>(KAFKA_TYPES.Kafka);
		return kafka.producer();
	});
	
	*/
	// bind SchemaRegistryArgs for schemaRegistry's constructor
	bind<SchemaRegistryAPIClientArgs>(KAFKA_TYPES.KafkaSchemaRegisterClientArgs).toDynamicValue(() => {
		return { host: 'http://localhost:8081' };
	});

	// annotate SchemaRegistry so constructor will be injected correctly
	decorate(injectable(), SchemaRegistry);
	// inject SchemaRegistry's config to SchemaRegistry constructor as 1 argument: like new SchemaRegistry(config);
	decorate(inject(KAFKA_TYPES.KafkaSchemaRegisterClientArgs), SchemaRegistry, 0);
	// optional extra-options
	decorate(optional(), SchemaRegistry, 1);
	// optionally SchemaRegistry's options to SchemaRegistry constructor as 2 argument: like new SchemaRegistry(config, { options });
	decorate(inject(KAFKA_TYPES.KafkaSchemaRegisterClientOpts), SchemaRegistry, 1);
	// bind schemaRegistry to SchemaRegistryClient as singleton
	bind<SchemaRegistry>(KAFKA_TYPES.KafkaSchemaRegisterClient).to(SchemaRegistry).inSingletonScope();
});
