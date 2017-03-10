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
 * This Node.js library manages the Amazon ElastiCache Memcached protocol.
 */
'use strict';

// Modules
const queue = require('./queue.js');
const crypto = require('crypto');
const crc16 = require('./crc16.js');
const common = require('./memcached-common.js');

//********
// This function queries memcached for the list of endpoints
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
		return common.sendRequestAndReadResponse(client, "config get cluster\r\n");
	}).then((response) => {
		if(response.length != 2) {
			throw new Error("Invalid response, expecting a CONFIG entry");
		}
		if(response[0].code != "CONFIG") {
			throw new Error(`Invalid response element ${response[0].code}, expecting a CONFIG entry`);
		}
		const configData = response[0].value.toString('utf-8').split("\n");
		if(configData.length != 3) {
			throw new Error(`Invalid configuration data ${JSON.stringify(configData)}`);
		}
		const servers = configData[1].split(' ');
		return servers.map((server) => {
			const serverParts = server.split('|');
			if(serverParts.length != 3) {
				throw new Error(`Invalid server entry "${server}"`);
			}
			return { id: `${serverParts[1]}:${serverParts[2]}`, host: serverParts[1], port: serverParts[2] };
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

function addBuffers(a, b) {
	const result = new Buffer(a.length);
	let carry = 0;
	for(let i = (result.length - 1); i >= 0; --i) {
		const sum = carry + a[i] + b[i];
		if(sum >= 256) {
			result[i] = (sum - 256);
			carry = 1;
		} else {
			result[i] = sum;
			carry = 0;
		}
	}
	return result;
}

function cryptoHash(keys, shards, algo) {
	let first = true;
	let length = null;
	let step = null;

	return keys.map((key) => {
		const digest = crypto.createHash(algo).update(key).digest();

		if(first) {
			first = false;
			length = digest.length;
			let remainder = 1;
			step = new Buffer(length);
			for(let i = 0; i < step.length; ++i) {
				const value = (remainder * 256);
				remainder = value % shards;
				step[i] = (value - remainder) / shards;
			}
		}

		let end = step;
		for(let i = 0; i < (shards - 1); ++i) {
			if(digest.compare(end) < 0) {
				return i;
			}
			end = addBuffers(end, step);
		}
		return shards - 1;
	});
}

const hashingMethods = {
	md5: (keys, shards) => {
		return cryptoHash(keys, shards, 'md5');
	},

	redis: (keys, shards) => {
		return keys.map((key) => {
			const digest = crc16.xmodem(key) % 16384;
			const step = Math.floor(16384 / shards);
			let end = step;
			for(let i = 0; i < (shards - 1); ++i) {
				if(digest < end) {
					return i;
				}
				end = end + step;
			}
			return shards - 1;
		});
	}
};

function extractHashTag(key) {
	const bracketIndex = key.indexOf("{");
	if (bracketIndex != -1) {
		const endBracket = key.indexOf("}", bracketIndex+1);
		if((endBracket != -1) && (endBracket != (bracketIndex + 1))) {
			return key.substring(bracketIndex + 1, endBracket);  
		}
	}
	return key;
}

//********
// This function generates a command to be sent to memcached
//  - servers: a list of servers to use for ElastiCache
//  - records: a list of {key:<string>, data:<string>} records to be sent
//
// --> Documentation: https://github.com/memcached/memcached/blob/master/doc/protocol.txt
exports.set = function(servers, records, options) {
	if((! Array.isArray(servers)) || servers.length == 0) {
		throw new Error("You must specify a non empty list of memcached servers");
	}
	if((! Array.isArray(records)) || records.length == 0) {
		throw new Error("You must specify a non empty list of records to store in memcached");
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
	const debug      = (options && options.hasOwnProperty('debug')) ? (options.debug === true) : false;
	const expiration = (options && options.hasOwnProperty('expiration') && ((typeof options.expiration) == "number") && (!isNaN(options.expiration)) && (options.expiration >= 0)) ? options.expiration : 0;
	const hashName   = (options && options.hasOwnProperty('hash')) ? options.hash : 'redis';
	const hashTags   = (options && options.hasOwnProperty('hashTags')) ? (options.hashTags === true) : true;
	const parallel   = (options && options.hasOwnProperty('parallel') && ((typeof options.parallel) == "number") && (!isNaN(options.parallel)) && (options.parallel >= 1)) ? options.parallel : 1;

	if(! hashingMethods.hasOwnProperty(hashName)) {
		throw new Error(`Unable to identify requested hashing algorithm: ${hashName}`);
	}

	let hashKeys = records.map((r) => r.key);
	if (hashTags) {
		hashKeys = hashKeys.map((key) => extractHashTag(key));
	}
	const hashes = hashingMethods[hashName](hashKeys, servers.length);
	let responses = [];

	const allBuckets = [];
	const buckets  = {};
	for(let i = 0; i < records.length; ++i) {
		const hash = hashes[i];
		const server = servers[hash];
		const id = server.id;
		if(! buckets.hasOwnProperty(id)) {
			const serverData = { host: server.host, port: server.port, id: id, records: [] };
			buckets[id] = serverData;
			allBuckets.push(serverData);
		}

		const response = { record: records[i], hash: hash, server: server, processed: false, error: null };
		responses.push(response);
		buckets[id].records.push(response);
	}

	return queue(allBuckets, (bucket) => {
		let client = null;
		return common.connect(bucket.host, bucket.port).then((c) => {
			client = c;
		}).then(() => {
			return queue(bucket.records, (entry) => {
				const record = entry.record;
				const request = Buffer.concat([
					new Buffer(`set ${record.key} 0 ${expiration} ${record.data.length}\r\n`, 'utf-8'),
					record.data,
					new Buffer("\r\n", 'utf-8')
				]);
				return common.sendRequestAndReadResponse(client, request).then((response) => {
					if((response.length != 1) || (response[0].code != "STORED")) {
						throw new Error(`Unexpected response when publishing record ${record.key} to server ${bucket.id}: ${JSON.stringify(response)}`);
					} else {
						entry.processed = true;
					}
				}).catch((err) => {
					/* istanbul ignore next */
					if(debug) {
						console.error(new Error(`Error publishing record ${record.key} to server ${bucket.id}: ${err}`));
					}
					entry.processed = true;
					entry.error = err;
					return Promise.reject(err);
				});
			}, 1);
		}).then(() => {
			client.end();
		}).catch((err) => {
			client.end();
			return Promise.reject(err);
		});
	}, parallel).then(() => {
		return Promise.resolve(responses);
	}).catch(() => {
		const error = new Error("Errors occured when processing elements");
		error.responses = responses;
		return Promise.reject(error);
	});
};