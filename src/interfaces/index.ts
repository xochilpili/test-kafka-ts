interface ProtoTimestamp {
	seconds: number;
	nanos: number;
}
export interface IWorkflow {
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
