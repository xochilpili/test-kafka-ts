import { KAFKA_TYPES } from './../types/kafka_types';
import { PRODUCER_TYPES } from './../types/producer_types';
import { ContainerModule, decorate, inject, injectable, interfaces, namedConstraint, optional, tagged, taggedConstraint, targetName, traverseAncerstors } from 'inversify';
import { root, mapProtoFiles, populateMetadata } from '@engyalo/schemas';
import { client, protobuf as proto } from '@engyalo/kafka-ts';
import { TestEvent } from '../../interfaces';
import { populateSource } from '../../shared/utils';
import { Kafka } from 'kafkajs';

/*

Producer
    
    1.- uses KafkaProducer instance to actually send final message.
    2.- uses keySerializer to serialize message key
    3.- uses valueSerializer to serialize message value, can be: 
            1.- StringSerializer
			2.- JsonSerializer
			3.- MultiSerializer
			A.- If profobuf, then protobufSerializer -> 
				1.- schemaResolver -> MessageNameSchemaResolver || TopicNameSchemaResolver
					1.- root < from schemas
					2.- searchSchemaNamespace: com.yalo.schemas.events.*
					3.- protoIndexes, this array it's a map from 'schema namespace' to 'proto file' a method included in schemas library
					4.- serializationType: ValueSerialization | KeySerialization < enum
					5.- registry -> SchemaRegistryClient -> confluent's constructor
					6.- subjectResolver : only in case of using MessageNameSchemaResolver, this must be implemented in way that returns a protobuf schema depending of the topic
    4.- uses optional topicResolver to figure out topic to send message to, otherwise requires use of sendToTopic.
        1.- This can be a MessageNameTopicResolver, it's a method --not included-- that resolves the topic name based of protobuf schema and/or passed a defaultTopic in the case that wasn't resolved
*/

export const producerModule = new ContainerModule((bind: interfaces.Bind) => {
	// bind protobuf root
	bind(PRODUCER_TYPES.ProducerRoot).toConstantValue(root);
	// bind ProtobufSearchNamespace
	bind(PRODUCER_TYPES.ProtobufSearchNamespace).toConstantValue('com.yalo.schemas.events');
	// bind keySerialization to SerializationType when target tagged with KeySerialization
	bind(PRODUCER_TYPES.SerializationType).toConstantValue(client.SerializationType.KeySerialization).whenTargetTagged(PRODUCER_TYPES.SerializationType, client.SerializationType.KeySerialization);
	// bind valueSerialization to SerializationType when targt tagged with ValueSerialization
	bind(PRODUCER_TYPES.SerializationType).toConstantValue(client.SerializationType.ValueSerialization).whenTargetTagged(PRODUCER_TYPES.SerializationType, client.SerializationType.ValueSerialization);

	// bind protobuf indexesMap as singleton so it doesnt get recomputed
	bind(PRODUCER_TYPES.ProtobufIndexesMap)
		.toDynamicValue(() => {
			const protoIndexes = new Map<string, number[]>();
			mapProtoFiles().forEach((filePath) => {
				filePath.forEach((indexes, type) => {
					protoIndexes.set(type, indexes);
				});
			});
			return protoIndexes;
		})
		.inSingletonScope();

	bind(PRODUCER_TYPES.SubjectResolver)
		.toDynamicValue(() => {
			// eslint-disable-next-line @typescript-eslint/ban-types
			return new (class SubjectResolver<T extends object> implements proto.SubjectResolver<T> {
				resolveSubject(topic: string, _msg: proto.ProtobufAlike<T>, _serializationType: client.SerializationType): string {
					switch (topic) {
						case 'test1':
							return 'com.yalo.schemas.events.applications.PublishWorkflowEvent';
						default:
							throw new Error('unexpected topic ');
					}
				}
			})();
		})
		.inSingletonScope();

	// decorate MessageNameSchemaResolver with injectable and inject annotations
	decorate(injectable(), proto.ProtobufTypeResolver);
	decorate(injectable(), proto.MessageNameSchemaResolver);

	// MessageNameSchemaResolver dependencies
	/*
        0.- root
        1.- baseNamespace
        2.- msgIndexes
        3.- serializationType
        4.- SchemaRegistryClient
        5.- subjectResolver
     */
	decorate(inject(PRODUCER_TYPES.ProducerRoot), proto.MessageNameSchemaResolver, 0); // root
	decorate(inject(PRODUCER_TYPES.ProtobufSearchNamespace), proto.MessageNameSchemaResolver, 1); // baseNameSpace
	decorate(inject(PRODUCER_TYPES.ProtobufIndexesMap), proto.MessageNameSchemaResolver, 2); // msgIndexes
	// tag SerializationType
	decorate(tagged(PRODUCER_TYPES.SerializationType, client.SerializationType.ValueSerialization), proto.MessageNameSchemaResolver, 3); // just a tag for serializationType
	decorate(inject(PRODUCER_TYPES.SerializationType), proto.MessageNameSchemaResolver, 3); // serializationType
	decorate(inject(KAFKA_TYPES.KafkaSchemaRegisterClient), proto.MessageNameSchemaResolver, 4); // schemaRegistryClient
	decorate(inject(PRODUCER_TYPES.SubjectResolver), proto.MessageNameSchemaResolver, 5); // subjectResolver
	// bind SchemaResolver to MessageNameSchemaResolver
	bind(PRODUCER_TYPES.SchemaResolver).to(proto.MessageNameSchemaResolver).inSingletonScope();
	// EOF MessageNameSchemaResolver dependencies

	// initializeMetadata for ProtobufSerializer
	bind(PRODUCER_TYPES.PopulateMetadata).toConstantValue((event: proto.ProtobufAlike<TestEvent>) => populateMetadata(event, populateSource));

	// make stringSerializer injectable, dunno why this is here
	decorate(injectable(), client.StringSerializer);
	// bind StringSerializer to Serializer when target tagged wth KeySerialization
	bind(PRODUCER_TYPES.Serializer).to(client.StringSerializer).whenTargetTagged(PRODUCER_TYPES.SerializationType, client.SerializationType.KeySerialization);

	// make ProtobufSerializer injectable: dependencies:
	/*
        0.- schemaResolver
        1.- initializeMetadata
    */
	decorate(injectable(), proto.ProtobufSerializer);
	decorate(inject(PRODUCER_TYPES.SchemaResolver), proto.ProtobufSerializer, 0); // schemaResolver
	decorate(inject(PRODUCER_TYPES.PopulateMetadata), proto.ProtobufSerializer, 1); // initializeMetadata

	// bind ProtobufSerializer to Serializer when target tagged with ValueSerialization
	// AND parent is named 'protobuf'
	/// This means that, when in constructor we added @inject(PRODUCER_TYPES.Producer) @named('protobuf') private producer:
	//  When, added a @named decorator passing string value: 'protobuf', will instantiate ProtobufSerializer, otherwise: JsonSerializar or StringSerializer depending of customizations
	//  // TODO: Add unit-testing for those String & Json Serialization in producer
	const targetTaggedAndAncestorNamed = (targetTagName: string | number | symbol, targetTagValue: any, ancestorName: string) => (request: interfaces.Request) => {
		return taggedConstraint(targetTagName)(targetTagValue)(request) && traverseAncerstors(request, namedConstraint(ancestorName));
	};

	const targetTaggedAndNotAncestorNamed = (targetTagName: string | number | symbol, targetTagValue: any, ancestorName: string) => (request: interfaces.Request) => {
		return taggedConstraint(targetTagName)(targetTagValue)(request) && !traverseAncerstors(request, namedConstraint(ancestorName));
	};

	// resolves tag, when injected serialization type is injected and tagged with 'protobuf'
	bind(PRODUCER_TYPES.Serializer).to(proto.ProtobufSerializer).when(targetTaggedAndAncestorNamed(PRODUCER_TYPES.SerializationType, client.SerializationType.ValueSerialization, 'protobuf'));
	// JsonSerializer
	decorate(injectable(), client.JsonSerializer);
	bind(PRODUCER_TYPES.Serializer).to(client.JsonSerializer).when(targetTaggedAndNotAncestorNamed(PRODUCER_TYPES.SerializationType, client.SerializationType.ValueSerialization, 'protobuf'));

	// bind a dynamoc valie to KafkaProducer
	bind(KAFKA_TYPES.KafkaProducer)
		.toDynamicValue((context: interfaces.Context) => {
			const kafka: Kafka = context.container.get<Kafka>(KAFKA_TYPES.Kafka);
			return kafka.producer();
		})
		.inSingletonScope();
	// Client.Producer dependencies:
	/*  
        0.- Producer
        1.- keySerializer
        2.- valueSerializer
        3.- topicResolver
    */
	decorate(injectable(), client.Producer);
	decorate(inject(KAFKA_TYPES.KafkaProducer), client.Producer, 0); // producer
	// tag first serializer as keySerialization
	decorate(tagged(PRODUCER_TYPES.SerializationType, client.SerializationType.KeySerialization), client.Producer, 1); // keySerializer tag
	decorate(inject(PRODUCER_TYPES.Serializer), client.Producer, 1); // injected keySerializer
	decorate(tagged(PRODUCER_TYPES.SerializationType, client.SerializationType.ValueSerialization), client.Producer, 2); // valueSerializer tag
	decorate(inject(PRODUCER_TYPES.Serializer), client.Producer, 2); // injected keySerializer
	// make topicResolver optional
	decorate(optional(), client.Producer, 3); // topicResolver, this is optional when used JsonSerialization
	decorate(inject(PRODUCER_TYPES.TopicResolver), client.Producer, 3); // topicResolver
	bind(PRODUCER_TYPES.Producer).to(client.Producer);
});
