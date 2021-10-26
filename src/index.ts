// import { main as consumerMain } from './consumer';
// import { main as producerMain } from './producer/producer';
import { main as protoMain } from './producer/proto_producer';

main().catch((error) => {
	console.error({ error });
	process.exit(1);
});

async function main() {
	if (process.argv.length === 2) {
		console.log(`You must specify 'producer' or 'consumer' as the first argument to ${process.argv[1]}`);
		process.exit(1);
	}
	const first = process.argv[2];
	if (first === 'producer') {
		console.log('starting producer');
		// await producerMain();
		await protoMain();
		/* } else if (first === 'consumer') {
		console.log('starting consumer');
		await consumerMain(); */
	} else {
		console.log(`You must specify 'producer' or 'consumer' as the first argument to ${process.argv[1]}`);
	}
}