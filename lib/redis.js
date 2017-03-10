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
 * This Node.js library manages the Amazon ElastiCache Redis protocol.
 */
'use strict';

// Modules
const queue  = require('./queue.js');
const crc16  = require('./crc16.js');
const common = require("./redis-common.js");

//********
// This function queries redis for the list of endpoints
//
// --> Documentation: http://docs.aws.amazon.com/AmazonElastiCache/latest/UserGuide/AutoDiscovery.AddingToYourClientLibrary.html
exports.servers = (endpoint, options) => {
	/* istanbul ignore next */
	const debug = options && options.hasOwnProperty('debug') && (options.debug == true);
	const parts = endpoint.split(':');

	// Send specific message to server
	/* istanbul ignore next */
	if(debug) {
		console.log(`Connecting to ${parts[0]}:${parts[1]}`);
	}

	let client = null;
	return common.connect(parts[0], parts[1]).then((c) => {
		client = c;
	}).then(() => {
		return common.sendRequestAndReadResponse(client, ["CLUSTER", "SLOTS"]);
	}).then((response) => {
		/* istanbul ignore next */
		if(debug) {
			console.log("Response received from server: ", JSON.stringify(response, null, '\t'));
		}

		if(response instanceof Error) {
			throw response;
		}
		if(! Array.isArray(response)) {
			throw new Error("Invalid response, expecting a slot list as an array");
		}
		return response.map((entry) => {
			if((! Array.isArray(entry)) || (entry.length < 3) || (typeof entry[0] != "number") || (typeof entry[1] != "number")) {
				throw new Error(`Invalid response element, should be an array containing contain an integer for the slot start, an integer for the slot end, and a list of nodes storing these slots`);
			}

			const slot = { start: entry[0], end: entry[1], master: null, slaves: [] };
			const nodes = entry.slice(2);

			let first = true;
			nodes.forEach((node) => {
				if((! Array.isArray(node)) || (typeof node[0] != "string") || (typeof node[1] != "number")) {
					throw new Error(`Invalid response element, expecting an array containing a host name and a port number`);
				}

				let id = null;
				if((node.length >= 3) && (typeof node[2] == "string")) {
					id = node[2];
				} else {
					id = `${node[0]}:${node[1]}`;
				}
				const server = { id: id, host: node[0], port: node[1] };
				if(first) {
					slot.master = server;
					first = false;
				} else {
					slot.slaves.push(server);
				}
			});
			return slot;
		});
	}).then((servers) => {
		client.end();
		return Promise.resolve(servers);
	}).catch((err) => {
		if(client) {
			client.end();
		}
		return Promise.reject(err);
	});
};

//********
// This function generates a command to be sent to redis
//  - slots:   the configuration for the Redis Slots
//  - records: a list of {key:<string>, data:<string>} records to be sent
//  - options: options for the setter
//
// --> Documentation: http://redis.io/topics/protocol
exports.set = function(slots, records, options) {
	if((! Array.isArray(slots)) || slots.length == 0) {
		throw new Error("You must specify a non empty list of Redis slots");
	}
	if((! Array.isArray(records)) || records.length == 0) {
		throw new Error("You must specify a non empty list of records to store in Redis");
	}
	for(let i = 0; i < records.length; ++i) {
		if((! records[i].hasOwnProperty('key')) || ((typeof records[i].key) != "string")) {
			throw new Error("All records must have a 'key' property as a string");
		}
		if(! records[i].hasOwnProperty('data')) {
			throw new Error("All records must have a 'data' property as a string or buffer");
		} else if((typeof records[i].data) == "string") {
			records[i].data = new Buffer(records[i].data, 'utf-8');
		} else if (! Buffer.isBuffer(records[i].data)) {
			throw new Error("All records must have a 'data' property as a string or buffer");
		}
	}

	/* istanbul ignore next */
	//const debug = options && options.hasOwnProperty('debug') && (options.debug == true);
	const parallel = (options && options.hasOwnProperty('parallel') && ((typeof options.parallel) == "number") && (!isNaN(options.parallel)) && (options.parallel >= 1)) ? options.parallel : 1;

	const allNodes = [];
	const nodes = {};
	records.forEach((record) => {
		const key = common.extractHashTag(record.key);
		const hash = crc16.xmodem(key) % 16384;
		const slot = slots.find((s) => (s.start <= hash && s.end >= hash));

		if(slot === undefined) {
			throw new Error(`Unable to find suitable slot for current record with id: ${key}`);
		}

		const node = slot.master;
		if(! nodes.hasOwnProperty(node.id)) {
			const nodeData = { host: node.host, port: node.port, id: node.id, records: [] };
			nodes[node.id] = nodeData;
			allNodes.push(nodeData);
		}
		nodes[node.id].records.push(record);
	});

	return queue(allNodes, (node) => {
		let client = null;
		return common.connect(node.host, node.port).then((c) => {
			client = c;
		}).then(() => {
			const query = ["MSET"];
			node.records.forEach(function(record) {
				query.push(record.key);
				query.push(record.data);
			});
			return common.sendRequestAndReadResponse(client, query);
		}).then((response) => {
			if((typeof response != "string") || (response != "OK")) {
				throw new Error(`Error occured while sending item to Redis: ${response}`);
			}
		}).then(() => {
			client.end();
		}).catch((err) => {
			if(client) {
				client.end();
			}
			return Promise.reject(err);
		});
	}, parallel);
};
