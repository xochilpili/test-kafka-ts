import { root, com } from '@engyalo/schemas';
import { TestEvent } from 'src/interfaces';

export function createEvent() {
	const PublicWorkflow = root.lookupType('com.yalo.schemas.events.applications.PublishWorkflowEvent');
	const Metadata = root.lookupType('com.yalo.schemas.events.common.Metadata');
	const today = new Date();
	const event = PublicWorkflow.create({
		metadata: Metadata.create({
			traceId: 'trace-id',
		}),
		eventName: 'publishedWorkflow',
		workflowId: 'workflow-id',
		workflow: {
			_id: '1234',
			name: 'workflow-name',
			language: 'es',
			stepSequence: 1,
			status: 'ACTIVE',
			createdAt: today,
			updatedAt: today,
			publishedAt: today,
		},
	});
	return event;
}

const myTestEvent: TestEvent = {
	correlationId: 'abc',
	eventName: 'publishedWorkflow',
	workflowId: 'workflow-id',
	workflow: {
		_id: '123',
		name: 'dummy-workflow',
		language: 'es',
		stepSequence: 2,
		status: 'ACTIVE',
		createdAt: new Date(),
		updatedAt: new Date(),
		publishedAt: new Date(1970, 1, 1),
	},
};

const Domain = root.lookupEnum('com.yalo.schemas.events.common.Domain');
export function populateSource(source: com.yalo.schemas.events.common.Metadata.ISource) {
	source.domain = Domain.values.APPLICATIONS;
	source.instance = 'instance';
	source.address = 'test-protobuf';
	source.service = 'localhost';
}
