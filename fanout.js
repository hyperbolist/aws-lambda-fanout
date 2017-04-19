/* 
 * AWS Lambda Fan-Out Utility
 * 
 * Copyright 2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
const transform     = require('./lib/transformation.js');
const configuration = require('./lib/configuration.js');
const statistics    = require('./lib/statistics.js');
const services      = require('./lib/services.js');
const queue         = require('./lib/queue.js');
const util          = require('./lib/util.js');

util.ensureAlways(Promise.prototype);

// Service configuration
const config = {
	parallelTargets: util.getEnvNumber('PARALLEL_TARGETS', 2), // Number of parallel targets for fan-out destination
	parallelPosters: util.getEnvNumber('PARALLEL_POSTERS', 2), // Number of parallel posters for fan-out destination
	debug          : util.getEnvBool('DEBUG_MODE', false)      // Activate debug messages
};

const providedConfig = util.getEnvString('CONFIGURATION', '');
if(providedConfig.length > 0) {
	try {
		util.applyChanges(config, JSON.parse(providedConfig));
	} catch(e) {
		console.error("Invalid configuration element provided in environment variable CONFIGURATION, ignoring", e);
	}
}

configuration.configure(config);
services.configure(config);

module.exports.getConfig = () => {
	return config;
};

//********
// This function posts data to the specified service
//  If the target is marked as 'collapse', records will
//  be grouped in a single payload before being sent
function postToService(eventSourceARN, serviceReference, target, records, stats) {
	const parallelPosters = target.parallel ? config.parallelPosters : 1;
	let   hasErrors = false;
	const definition = serviceReference.definition;
	const service = serviceReference.service;
	const limits = definition.limits;

	const maxRecords          = limits.maxRecords;
	const maxSize             = limits.maxSize;
	const maxUnitSize         = limits.maxUnitSize;
	const includeKey          = limits.includeKey;
	const listOverhead        = limits.listOverhead;
	const recordOverhead      = limits.recordOverhead;
	const interRecordOverhead = limits.interRecordOverhead;

	// Filter invalid records
	records = records.filter((record) => {
		const size = record.size + (includeKey ? Buffer.byteLength(record.key) : 0);
		const maxSizeWithOverhead = maxUnitSize - (listOverhead + recordOverhead);
		if(size > maxSizeWithOverhead) {
			console.error(`Record with key '${record.key}' and sequence number '${record.sequenceNumber}' too large to be pushed to target '${target.id}' of type '${target.type}': record of ${size} bytes, maximum of ${maxSizeWithOverhead} bytes allowed`);
			hasErrors = true;
			return false;
		} else {
			return true;
		}
	});

	// Group records per block for sending
	const maxRecordsPerBlock = (target.collapse !== "none") ? maxRecords : 1;
	const blocks = [];
	let   blockSize = listOverhead;
	let   block = [];
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

	// We have some remaining entries, push them to a new block
	if(block.length > 0) {
		blocks.push(block);
		block = [];
	}

	// Posts the blocks to the target services
	return queue(blocks, (block) => {
		stats.addTick('Calls', eventSourceARN, target.destination);

		return definition.send(service, target, block).then(() => {
			stats.addTick('Success', eventSourceARN, target.destination);
		}).catch((err) => {
			stats.addTick('Error', eventSourceARN, target.destination);
			console.error(`An error occured while posting data to target '${target.id}' of type '${target.type}':`, err);
			hasErrors = true;
		});
	}, parallelPosters).always(() => {
		serviceReference.dispose();
	}).then(() => {
		if(hasErrors) {
			throw new Error("Some errors have occured while posting data to AWS Services");
		}
		return target;
	});
}

//********
// This function transfers an entire event to the underlying service
function interceptService(eventSourceARN, serviceReference, target, event, stats) {
	/* istanbul ignore next */
	if(config.debug) {
		console.log(`Processing records for target '${target.id}'`);
	}

	const start = Date.now();
	stats.addTick('Calls', eventSourceARN, target.destination);
	return serviceReference.definition.intercept(serviceReference.service, target, event).always(() => {
		serviceReference.dispose();
	}).then(() => {
		/* istanbul ignore next */
		if(config.debug) {
			const end = Date.now();
			const duration = Math.floor((end - start) / 10) / 100;
			console.log(`Target '${target.id}' for source '${eventSourceARN}' successfully processed the event in ${duration} seconds`);
		}
		stats.addTick('Success', eventSourceARN, target.destination);
	}).catch((err) => {
		stats.addTick('Error', eventSourceARN, target.destination);
		return Promise.reject(err);
	});
}

//********
// This function manages the messages for a target
function sendMessages(eventSourceARN, target, event, records, stats) {
	/* istanbul ignore next */
	if(config.debug) {
		console.log(`Processing records for target '${target.id}'`);
	}

	const start = Date.now();

	stats.register('Records', 'stats'  , 'Count', eventSourceARN, target.destination);
	stats.register('Calls'  , 'counter', 'Count', eventSourceARN, target.destination);
	stats.register('Success', 'counter', 'Count', eventSourceARN, target.destination);
	stats.register('Error'  , 'counter', 'Count', eventSourceARN, target.destination);

	stats.addTick('Targets', eventSourceARN);
	stats.addValue('Records', records.length, eventSourceARN, target.destination);

	return services.get(target).then((serviceReference) => {
		const definition = serviceReference.definition;
		if(definition.intercept) {
			return interceptService(eventSourceARN, serviceReference, target, event, stats);
		} else if (definition.send) {
			return transform.records(records, target).then((transformedRecords) => {
				if(transformedRecords.errors.length > 0) {
					for (let i = 0; i < transformedRecords.errors.length; ++i) {
						const e = transformedRecords.errors[i];
						console.error(`Error: ${e.message} for record with key '${e.record.key}': ${e.error}`);
					}
					return postToService(eventSourceARN, serviceReference, target, transformedRecords.success, stats).then(() => {
						throw new Error("Some events were not transformed, check log");
					});
				} else {
					return postToService(eventSourceARN, serviceReference, target, transformedRecords.success, stats);
				}
			});
		} else {
			throw new Error(`Invalid module '${target.type}', it must export either an 'intercept' or a 'send' method`);
		}
	}).then(() => {
		/* istanbul ignore next */
		if(config.debug) {
			const end = Date.now();
			const duration = Math.floor((end - start) / 10) / 100;
			console.log(`Target '${target.id}' for source '${eventSourceARN}' successfully processed ${records.length} records in ${duration} seconds`);
		}
		return target;
	});
}

//********
// Lambda entry point. Loads the configuration and does the fanOut
exports.handler = function(event, context, callback) {
	try	{
		const start   = Date.now();
		const stats   = statistics.create();
		const records = transform.extractRecords(event);

		stats.register('Calls'       , 'counter', 'Count');
		stats.register('Success'     , 'counter', 'Count');
		stats.register('Error'       , 'counter', 'Count');
		stats.register('InputRecords', 'stats', 'Count');

		stats.addTick('Calls');
		stats.addValue('InputRecords', records.length);

		if(records.length === 0) {
			console.log("No record to process");
			stats.publish();
			callback(null);
			return;
		}

		/* istanbul ignore next */
		if (config.debug) {
			console.log(`Processing ${records.length} events`);
		}

		let eventSourceARN = records[0].eventSourceARN;
		stats.register('Calls'       , 'counter', 'Count', eventSourceARN);
		stats.register('Success'     , 'counter', 'Count', eventSourceARN);
		stats.register('Error'       , 'counter', 'Count', eventSourceARN);
		stats.register('InputRecords', 'stats'  , 'Count', eventSourceARN);
		stats.register('Records'     , 'stats'  , 'Count', eventSourceARN);
		stats.register('Targets'     , 'counter', 'Count', eventSourceARN);

		stats.addTick('Calls', eventSourceARN);
		stats.addValue('InputRecords', records.length, eventSourceARN);

		let hasErrors = false;
		configuration.get(eventSourceARN, services.definitions).then((targets) => {
			if(targets.length > 0) {
				stats.addValue('Records', records.length, eventSourceARN);
			}
			return queue(targets, (target) => {
				return sendMessages(eventSourceARN, target, event, records, stats).catch((err) => {
					console.error(`Error while processing target '${target.id}': ${err.stack}`);
					hasErrors = true;
					return null;
				});
			}, config.parallelTargets);
		}).then((targets) => {
			if(hasErrors) {
				stats.addTick('Success');
				stats.addTick('Success', eventSourceARN);
			} else {
				stats.addTick('Error');
				stats.addTick('Error', eventSourceARN);
			}
			return targets;
		}).catch((err) => {
			stats.addTick('Error');
			stats.addTick('Error', eventSourceARN);
			return Promise.reject(err);
		}).always(() => {
			stats.publish();
		}).then((targets) => {
			const end = Date.now();
			const duration = Math.floor((end - start) / 10) / 100;
			if(hasErrors) {
				throw new Error(`Processing completed with errors, processed ${records.length} records for ${targets.length} targets in ${duration} seconds`);
			}

			console.log(`Processing succeeded, processed ${records.length} records for ${targets.length} targets in ${duration} seconds`);
			callback(null);
		}).catch((err) => {
			console.error(err);
			callback(err);
		});
	} catch (e) {
		console.error("An exception occured when procesing the event", e);
		callback(new Error("An exception occured when procesing the event"));
	}
};
