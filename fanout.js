/* 
 * AWS Lambda Fan-Out Utility
 * 
 * Copyright 2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 * 
 *  http://aws.amazon.com/apache2.0
 * 
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 * 
 */

/* 
 * This AWS Lambda Node.js function receives records from an Amazon Kinesis Stream
 *  or an Amazon DynamoDB Stream, and sends them to other endpoints as defined in
 *  its configuration table (check configuration.js for details).
 */
'use strict';

// Modules
const transform = require('./lib/transformation.js');
const configuration = require('./lib/configuration.js');
const statistics = require('./lib/statistics.js');
const services = require('./lib/services.js');
const queue = require('./lib/queue.js');

// Service configuration
const config = {
	parallelTargets: configuration.getEnvironmentVariable('PARALLEL_TARGETS', 2), // Number of parallel targets for fan-out destination
	parallelPosters: configuration.getEnvironmentVariable('PARALLEL_POSTERS', 2), // Number of parallel posters for fan-out destination
	debug          : configuration.getEnvironmentVariable('DEBUG_MODE', "false") == "true"    // Activate debug messages
};
configuration.configure(config);
statistics.configure(config);
services.configure(config);

//********
// This function posts data to the specified service
//  If the target is marked as 'collapse', records will
//  be grouped in a single payload before being sent
function postToService(serviceReference, target, records) {
	const parallelPosters = target.parallel ? config.parallelPosters : 1;
	let   hasErrors = false;
	const definition = serviceReference.definition;
	const service = serviceReference.service;
	const limits = definition.limits;

	const maxRecords = limits.maxRecords;
	const maxSize = limits.maxSize;
	const maxUnitSize = limits.maxUnitSize;
	const includeKey = limits.includeKey;
	const listOverhead = limits.listOverhead;
	const recordOverhead = limits.recordOverhead;
	const interRecordOverhead = limits.interRecordOverhead;

	// Filter invalid records
	records = records.filter((record) => {
		const size = record.size + (includeKey ? Buffer.byteLength(record.key) : 0);
		const maxSizeWithOverhead = maxUnitSize - (listOverhead + recordOverhead);
		if(size > maxSizeWithOverhead) {
			console.error(`Record too large to be pushed to target '${target.id}' of type '${target.type}': record of ${size} bytes, maximum of ${maxSizeWithOverhead} bytes allowed`);
			hasErrors = true;
			return false;
		} else {
			return true;
		}
	});

	// Group records per block for sending
	const maxRecordsPerBlock = (target.collapse !== null) && (target.collapse !== "") && (target.collapse !== "none") ? maxRecords : 1;
	const blocks = [];
	let blockSize = listOverhead;
	let block = [];
	while(records.length > 0) {
		const record = records.shift();
		const recordSize = record.size + (includeKey ? record.key.length : 0) + recordOverhead + (block.length > 0 ? interRecordOverhead: 0);

		if(((blockSize + recordSize) > maxSize) || (block.length >= maxRecordsPerBlock)) {
			// Block full, start a new block
			blocks.push(block);
			block = [];
			blockSize = listOverhead;
		}

		// Add the record to the records to send
		blockSize = blockSize + recordSize;
		block.push(record);
	}
	if(block.length > 0) {
		blocks.push(block);
		block = [];
	}

	// Posts the blocks to the target services
	return queue(blocks, (block) => {
		return definition.send(service, target, block.records).catch((err) => {
			console.error(`An error occured while posting data to target '${target.id}' of type '${target.type}':`, err);
			hasErrors = true;
			return Promise.resolve(null);
		});
	}, parallelPosters).then(() => {
		serviceReference.dispose();
		if(hasErrors) {
			return Promise.reject(Error("Some errors have occured while posting data to AWS Services"));
		} else {
			return Promise.resolve(null);
		}
	}).catch((err) => {
		console.error(err);
		return Promise.reject(err);
	});
}

//********
// This function transfers an entire event to the underlying service
function interceptService(serviceReference, target, event) {
	return serviceReference.definition.intercept(serviceReference.service, target, event).then(() => {
		serviceReference.dispose();
	}).catch((err) => {
		serviceReference.dispose();
		return Promise.reject(err);
	});
}

//********
// This function manages the messages for a target
function sendMessages(eventSourceARN, target, event, stats) {
	if(config.debug) {
		console.log(`Processing target '${target.id}'`);
	}

	const start = Date.now();
	stats.addTick(`targets#${eventSourceARN}`);
	stats.register(`records#${eventSourceARN}#${target.destination}`, 'Records', 'stats', 'Count', eventSourceARN, target.destination);
	stats.addValue(`records#${eventSourceARN}#${target.destination}`, event.Records.length);

	services.get(target).then((serviceReference) => {
		const definition = serviceReference.definition;
		if(definition.intercept) {
			if(target.passthrough) {
				return transform(event.Records, target).then((transformedRecords) => {
					transformedRecords.forEach((record) => record.data = record.data.toString('base64'));
					return transformedRecords;
				}).then((transformedRecords) => {
					return interceptService(serviceReference, target, { Records: transformedRecords }, stats);
				});
			} else {
				return interceptService(serviceReference, target, event, stats);
			}
		} else if (definition.send) {
			return transform(event.Records, target).then((transformedRecords) => {
				return postToService(serviceReference, target, transformedRecords, stats);
			});
		} else {
			return Promise.reject(new Error(`Invalid module '${target.type}', it must export either an 'intercept' or a 'send' method`));
		}
	}).then(() => {
		if(config.debug) {
			const end = Date.now();
			const duration = Math.floor((end - start) / 10) / 100;
			console.log(`Target '${target.id}' for source '${eventSourceARN}' successfully processed ${event.Records.length} records in ${duration} seconds`);
		}
	}).catch((err) => {
		console.error(`Error while processing target '${target.id}': ${err.message}`);
		return Promise.reject(new Error("Error while processing target '" + target.id + "': " + err));
	});
}

//********
// This function reads a set of records from Amazon Kinesis or Amazon DynamoDB Streams and sends it to all subscribed parties
function fanOut(eventSourceARN, event, targets, stats) {
	if(targets.length === 0) {
		console.log("No output subscribers found for this event");
		return Promise.resolve(null);
	}

	const start     = Date.now();
	let   hasErrors = false;

	return queue(targets, (target) => {
		return sendMessages(eventSourceARN, target, event, stats).catch(() => {
			hasErrors = true;
			return Promise.resolve(null);
		});
	}, config.parallelTargets).then(() => {
		if(hasErrors) {
			console.error("Processing of targets for this event ended with errors,");
			return Promise.reject(new Error("Some processing errors occured"));
		} else {
			const end = Date.now();
			const duration = Math.floor((end - start) / 10) / 100;
			console.log(`Processing succeeded, processed ${event.Records.length} records for ${targets.length} targets in ${duration} seconds`);
			return Promise.resolve(null);
		}
	});
}

//********
// Lambda entry point. Loads the configuration and does the fanOut
exports.handler = function(event, context, callback) {
	const stats = statistics.create();
	stats.register('sources', 'Sources', 'counter', 'Count'); // source, destination
	stats.register('records', 'Records', 'counter', 'Count'); // source, destination

	if (config.debug) {
		console.log(`Starting process of ${event.Records.length} events`);
	}

	// Group records per source ARN
	const sources = {};

	// Kinesis Firehose, to be transformed at the event level
	if((! event.hasOwnProperty("Records")) && (event.hasOwnProperty('records'))) {
		event.Records = event.records.map((record) => {
			return {
				invocationId: event.invocationId,
				awsRegion: event.region,
				eventSource: "aws:firehose",
				eventSourceARN: event.deliveryStreamArn,
				firehose: record
			};
		});
	}

	event.Records.forEach(function(record) {
		const eventSourceARN = record.eventSourceARN || record.TopicArn;
		if(! sources.hasOwnProperty(eventSourceARN)) {
			stats.addTick('sources');
			stats.register(`records#${eventSourceARN}`, 'Records', 'counter', 'Count', eventSourceARN);
			stats.register(`targets#${eventSourceARN}`, 'Targets', 'counter', 'Count', eventSourceARN);
			sources[eventSourceARN] = { Records: [record] };
		} else {
			sources[eventSourceARN].Records.push(record);
		}
		stats.addTick(`records#${eventSourceARN}`);
	});

	const eventSourceARNs = Object.keys(sources);

	let hasErrors = false;

	queue(eventSourceARNs, (eventSourceARN) => {
		configuration.get(eventSourceARN, services.definitions).then((targets) => {
			return fanOut(eventSourceARN, sources[eventSourceARN], targets, stats);
		}).catch(() => {
			hasErrors = true;
		});
	}).then(() => {
		stats.publish();
	}).then(() => {
		if(hasErrors) {
			console.log("Done processing all subscribers for this event, no errors detected");
			callback(null);
		} else {
			console.error("Some processing errors occured, check logs");
			callback(Error("Some processing errors occured, check logs"));
		}
	});
};
