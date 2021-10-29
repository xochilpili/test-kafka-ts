import { protobuf as proto } from '@engyalo/kafka-ts';

interface ProtoTimestamp {
	seconds: number;
	// nanos: number;
}
export interface IWorkflow {
	_id: string;
	name: string;
	language: string;
	stepSequence: number;
	status: string;
	createdAt: Date;
	updatedAt: Date;
	publishedAt: Date;
}
export interface IWorkflowSec {
	_id: string;
	name: string;
	language: string;
	stepSequence: number;
	status: string;
	createdAt: ProtoTimestamp;
	updatedAt: ProtoTimestamp;
	publishedAt: ProtoTimestamp;
}

export interface TestEvent {
	correlationId: string;
	eventName: string;
	workflowId: string;
	workflow: IWorkflow;
}

export interface TestEventSec {
	correlationId: string;
	eventName: string;
	workflowId: string;
	workflow: IWorkflowSec;
}

export interface IKafkaManager {
	connect(): Promise<void>;
	sendToTopic(topic: string, value: proto.ProtobufAlike<TestEvent>): Promise<void>;
}
