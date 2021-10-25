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

export interface TestEvent {
	correlationId: string;
	eventName: string;
	workflowId: string;
	workflow: IWorkflow;
}
