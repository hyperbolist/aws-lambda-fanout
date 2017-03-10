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
 * This Node.js script tests the features from the redis-host.js script
 */

/* global describe:false, it:false */

'use strict';

const rc = require('../lib/redis-common.js');
const rh = require('../test-hosts/redis-host.js');
const crc16 = require('../lib/crc16.js');
const uuid = require('uuid');
const assert = require('assert');
const sinon = require('sinon');

let config = null;
let discoverClient = null;
let slots = [];
const server = rh({ nodes: 4, slotsPerNode: 2, replicas: 1 })
	.then((c) => config = c)
	.then(() => rc.connect('localhost', config.processPorts[0]))
	.then((c) => discoverClient = c)
	.then(() => rc.sendRequestAndReadResponse(discoverClient, ["CLUSTER", "SLOTS"]))
	.then((response) => {
		slots = response.map((entry) => {
			const slot = { start: entry[0], end: entry[1], master: null, slaves: [] };
			const nodes = entry.slice(2);
			let first = true;
			nodes.forEach((node) => {
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
	}).then(() => discoverClient.end());

function slotFor(key) {
	const hashKey = rc.extractHashTag(key);
	const hash = crc16.xmodem(hashKey) % 16384;

	for(let i = 0; i < slots.length; ++i) {
		if((slots[i].start <= hash) && (slots[i].end >= hash)) {
			return slots[i];
		}
	}

	throw new Error(`Unable to find suitable slot for current record with key: ${key}`);
}

function nodeWithoutSlotFor(key) {
	const slot = slotFor(key);
	const slotNodes = [slot.master].concat(slot.slaves);

	for(let i = 0; i < slots.length; ++i) {
		const nodes = [slots[i].master].concat(slots[i].slaves);
		for(let j = 0; j < nodes.length; ++j) {
			const node = nodes[j];
			if(! slotNodes.find((n) => n.id == node.id)) {
				return node;
			}
		}
	}

	throw new Error(`Unable to find suitable node *not* containing a slot for current record with key: ${key}`);
}

describe('redis-host', () => {
	describe('CLUSTER SLOTS', () => {
		it('default should provide the server list for a single slot on a single server (via buffers)', (done) => {
			let client = null;
			let localConfig = null;
			rh().then((c) => {
				localConfig = c;
				return rc.connect('localhost', localConfig.processPorts[0]);
			}).then((c) => {
				client = c;
			}).then(() => {
				return rc.sendRequestAndReadResponse(client, [new Buffer("CLUSTER", 'utf-8'), new Buffer("SLOTS", 'utf-8')]);
			}).then((response) => {
				assert.strictEqual(response.length, 1);
				assert(Array.isArray(response[0]));
				assert.strictEqual(response[0].length, 3);
				assert.strictEqual(response[0][0], 0);
				assert.strictEqual(response[0][1], 16383);
				assert.strictEqual(response[0][2].length, 3);
				assert.strictEqual(response[0][2][0], "127.0.0.1");
				assert.strictEqual(response[0][2][1], localConfig.processPorts[0]);
				assert.strictEqual(typeof response[0][2][2], "string");
				return response;
			}).then(() => {
				client.end();
				localConfig.stop();
				done();
			}).catch((err) => {
				client.end();
				localConfig.stop();
				done(err);
			});
		});

		it('default should provide the server list for a single slot on a single server', (done) => {
			let client = null;
			let localConfig = null;
			rh().then((c) => {
				localConfig = c;
				return rc.connect('localhost', localConfig.processPorts[0]);
			}).then((c) => {
				client = c;
			}).then(() => {
				return rc.sendRequestAndReadResponse(client, ["CLUSTER", "SLOTS"]);
			}).then((response) => {
				assert.strictEqual(response.length, 1);
				assert(Array.isArray(response[0]));
				assert.strictEqual(response[0].length, 3);
				assert.strictEqual(response[0][0], 0);
				assert.strictEqual(response[0][1], 16383);
				assert.strictEqual(response[0][2].length, 3);
				assert.strictEqual(response[0][2][0], "127.0.0.1");
				assert.strictEqual(response[0][2][1], localConfig.processPorts[0]);
				assert.strictEqual(typeof response[0][2][2], "string");
				return response;
			}).then(() => {
				client.end();
				localConfig.stop();
				done();
			}).catch((err) => {
				client.end();
				localConfig.stop();
				done(err);
			});
		});

		it('should provide the server list for a single slot on a single server', (done) => {
			let client = null;
			let localConfig = null;
			rh({ nodes: 1, slotsPerNode: 1 }).then((c) => {
				localConfig = c;
				return rc.connect('localhost', localConfig.processPorts[0]);
			}).then((c) => {
				client = c;
			}).then(() => {
				return rc.sendRequestAndReadResponse(client, ["CLUSTER", "SLOTS"]);
			}).then((response) => {
				assert.strictEqual(response.length, 1);
				assert(Array.isArray(response[0]));
				assert.strictEqual(response[0].length, 3);
				assert.strictEqual(response[0][0], 0);
				assert.strictEqual(response[0][1], 16383);
				assert.strictEqual(response[0][2].length, 3);
				assert.strictEqual(response[0][2][0], "127.0.0.1");
				assert.strictEqual(response[0][2][1], localConfig.processPorts[0]);
				assert.strictEqual(typeof response[0][2][2], "string");
				return response;
			}).then(() => {
				client.end();
				localConfig.stop();
				done();
			}).catch((err) => {
				client.end();
				localConfig.stop();
				done(err);
			});
		});

		it('should provide the server list for a two nodes cluster with one slot each', (done) => {
			let client = null;
			let localConfig = null;
			rh({ nodes: 2, slotsPerNode: 1 }).then((c) => {
				localConfig = c;
				return rc.connect('localhost', localConfig.processPorts[0]);
			}).then((c) => {
				client = c;
			}).then(() => {
				return rc.sendRequestAndReadResponse(client, ["CLUSTER", "SLOTS"]);
			}).then((response) => {
				assert.strictEqual(response.length, 2);
				assert(Array.isArray(response[0]));
				assert.strictEqual(response[0].length, 3);
				assert.strictEqual(response[0][0], 0);
				assert.strictEqual(response[0][1], (16384/2)-1);
				assert.strictEqual(response[0][2][0], "127.0.0.1");
				assert.strictEqual(response[0][2][1], localConfig.processPorts[0]);
				assert.strictEqual(typeof response[0][2][2], "string");

				assert(Array.isArray(response[1]));
				assert.strictEqual(response[1].length, 3);
				assert.strictEqual(response[1][0], (16384/2));
				assert.strictEqual(response[1][1], 16383);
				assert.strictEqual(response[1][2][0], "127.0.0.1");
				assert.strictEqual(response[1][2][1], localConfig.processPorts[1]);
				assert.strictEqual(typeof response[1][2][2], "string");
				return response;
			}).then(() => {
				client.end();
				localConfig.stop();
				done();
			}).catch((err) => {
				client.end();
				localConfig.stop();
				done(err);
			});
		});

		it('should provide the server list for a two nodes cluster with two slots each', (done) => {
			let client = null;
			let localConfig = null;
			rh({ nodes: 2, slotsPerNode: 2 }).then((c) => {
				localConfig = c;
				return rc.connect('localhost', localConfig.processPorts[0]);
			}).then((c) => {
				client = c;
			}).then(() => {
				return rc.sendRequestAndReadResponse(client, ["CLUSTER", "SLOTS"]);
			}).then((response) => {
				assert.strictEqual(response.length, 4);
				assert(Array.isArray(response[0]));
				assert.strictEqual(response[0].length, 3);
				assert.strictEqual(response[0][0], 0);
				assert.strictEqual(response[0][1], (16384/4)-1);
				assert.strictEqual(response[0][2][0], "127.0.0.1");
				assert.strictEqual(response[0][2][1], localConfig.processPorts[0]);
				assert.strictEqual(typeof response[0][2][2], "string");

				assert(Array.isArray(response[1]));
				assert.strictEqual(response[1].length, 3);
				assert.strictEqual(response[1][0], (16384/4));
				assert.strictEqual(response[1][1], 2*(16384/4)-1);
				assert.strictEqual(response[1][2][0], "127.0.0.1");
				assert.strictEqual(response[1][2][1], localConfig.processPorts[1]);

				assert.strictEqual(typeof response[1][2][2], "string");
				assert(Array.isArray(response[2]));
				assert.strictEqual(response[2].length, 3);
				assert.strictEqual(response[2][0], 2*(16384/4));
				assert.strictEqual(response[2][1], 3*(16384/4)-1);
				assert.strictEqual(response[2][2][0], "127.0.0.1");
				assert.strictEqual(response[2][2][1], localConfig.processPorts[0]);
				assert.strictEqual(typeof response[2][2][2], "string");

				assert(Array.isArray(response[3]));
				assert.strictEqual(response[3].length, 3);
				assert.strictEqual(response[3][0], 3*(16384/4));
				assert.strictEqual(response[3][1], 16383);
				assert.strictEqual(response[3][2][0], "127.0.0.1");
				assert.strictEqual(response[3][2][1], localConfig.processPorts[1]);
				assert.strictEqual(typeof response[3][2][2], "string");
				return response;
			}).then(() => {
				client.end();
				localConfig.stop();
				done();
			}).catch((err) => {
				client.end();
				localConfig.stop();
				done(err);
			});
		});

		it('should provide the server list for a two nodes cluster with two slots each and a replica', (done) => {
			let client = null;
			let localConfig = null;
			rh({ nodes: 2, slotsPerNode: 2, replicas: 1 }).then((c) => {
				localConfig = c;
				return rc.connect('localhost', localConfig.processPorts[0]);
			}).then((c) => {
				client = c;
			}).then(() => {
				return rc.sendRequestAndReadResponse(client, ["CLUSTER", "SLOTS"]);
			}).then((response) => {
				assert.strictEqual(response.length, 4);
				assert(Array.isArray(response[0]));
				assert.strictEqual(response[0].length, 4);
				assert.strictEqual(response[0][0], 0);
				assert.strictEqual(response[0][1], (16384/4)-1);
				assert.strictEqual(response[0][2][0], "127.0.0.1");
				assert.strictEqual(response[0][2][1], localConfig.processPorts[0]);
				assert.strictEqual(typeof response[0][2][2], "string");
				assert.strictEqual(response[0][3][0], "127.0.0.1");
				assert.strictEqual(response[0][3][1], localConfig.processPorts[1]);
				assert.strictEqual(typeof response[0][3][2], "string");

				assert(Array.isArray(response[1]));
				assert.strictEqual(response[1].length, 4);
				assert.strictEqual(response[1][0], (16384/4));
				assert.strictEqual(response[1][1], 2*(16384/4)-1);
				assert.strictEqual(response[1][2][0], "127.0.0.1");
				assert.strictEqual(response[1][2][1], localConfig.processPorts[1]);
				assert.strictEqual(typeof response[1][2][2], "string");
				assert.strictEqual(response[1][3][0], "127.0.0.1");
				assert.strictEqual(response[1][3][1], localConfig.processPorts[0]);
				assert.strictEqual(typeof response[1][3][2], "string");

				assert(Array.isArray(response[2]));
				assert.strictEqual(response[2].length, 4);
				assert.strictEqual(response[2][0], 2*(16384/4));
				assert.strictEqual(response[2][1], 3*(16384/4)-1);
				assert.strictEqual(response[2][2][0], "127.0.0.1");
				assert.strictEqual(response[2][2][1], localConfig.processPorts[0]);
				assert.strictEqual(typeof response[2][2][2], "string");
				assert.strictEqual(response[2][3][0], "127.0.0.1");
				assert.strictEqual(response[2][3][1], localConfig.processPorts[1]);
				assert.strictEqual(typeof response[2][3][2], "string");

				assert(Array.isArray(response[3]));
				assert.strictEqual(response[3].length, 4);
				assert.strictEqual(response[3][0], 3*(16384/4));
				assert.strictEqual(response[3][1], 16383);
				assert.strictEqual(response[3][2][0], "127.0.0.1");
				assert.strictEqual(response[3][2][1], localConfig.processPorts[1]);
				assert.strictEqual(typeof response[3][2][2], "string");
				assert.strictEqual(response[3][3][0], "127.0.0.1");
				assert.strictEqual(response[3][3][1], localConfig.processPorts[0]);
				assert.strictEqual(typeof response[3][3][2], "string");
				return response;
			}).then(() => {
				client.end();
				localConfig.stop();
				done();
			}).catch((err) => {
				client.end();
				localConfig.stop();
				done(err);
			});
		});
	});

	describe('PING', () => {
		it('empty ping', (done) => {
			let client = null;
			server.then(() => {
				return rc.connect('localhost', config.processPorts[0]);
			}).then((c) => {
				client = c;
			}).then(() => {
				return rc.sendRequestAndReadResponse(client, ["PING"]);
			}).then((response) => {
				assert(Buffer.isBuffer(response));
				assert.deepEqual(response, new Buffer("PONG", 'utf-8'));
				return response;
			}).then(() => {
				client.end();
				done();
			}).catch((err) => {
				client.end();
				done(err);
			});
		});

		it('ping in two frames (server hanging)', (done) => {
			let client = null;
			server.then(() => {
				return rc.connect('localhost', config.processPorts[0]);
			}).then((c) => {
				client = c;
			}).then(() => {
				client.write("*1\r\n$4\r\nPING\r\n*2\r\n$4\r\nPING\r\n$2\r\nOK");
				return rc.sendRequestAndReadResponse(client, null);
			}).then((response) => {
				assert(Buffer.isBuffer(response));
				assert.deepEqual(response, new Buffer("PONG", 'utf-8'));
				return response;
			}).then(() => {
				client.write("\r\n");
				return rc.sendRequestAndReadResponse(client, null);
			}).then((response) => {
				assert(Buffer.isBuffer(response));
				assert.deepEqual(response, new Buffer("OK", 'utf-8'));
				return response;
			}).then(() => {
				client.end();
				done();
			}).catch((err) => {
				client.end();
				done(err);
			});
		});

		it('ping in two frames (client hanging)', (done) => {
			let client = null;
			server.then(() => {
				return rc.connect('localhost', config.processPorts[0]);
			}).then((c) => {
				client = c;
			}).then(() => {
				client.write("*1\r\n$4\r\nPING\r\n*2\r\n$4\r\nPING\r\n$2\r\nOK\r\n$4\r\nPING\r\n$2\r\nKO\r\n");
				return rc.sendRequestAndReadResponse(client, null);
			}).then((response) => {
				assert(Buffer.isBuffer(response));
				assert.deepEqual(response, new Buffer("PONG", 'utf-8'));
				return response;
			}).then(() => {
				return rc.sendRequestAndReadResponse(client, null);
			}).then((response) => {
				assert(Buffer.isBuffer(response));
				assert.deepEqual(response, new Buffer("OK", 'utf-8'));
				return response;
			}).then(() => {
				client.end();
				done();
			}).catch((err) => {
				client.end();
				done(err);
			});
		});

		it('invalid PING', (done) => {
			let client = null;
			server.then(() => {
				return rc.connect('localhost', config.processPorts[0]);
			}).then((c) => {
				client = c;
			}).then(() => {
				return rc.sendRequestAndReadResponse(client, [ "PING", "Test", "Test" ]);
			}).then((response) => {
				assert(response instanceof Error);
				assert.strictEqual(response.message, "ERR The 'PING' command only accepts up to one message");
			}).then(() => {
				client.write("*2\r\n$4\r\nPING\r\n+Test\r\n");
				return rc.sendRequestAndReadResponse(client, null);
			}).then((response) => {
				assert(response instanceof Error);
				assert.strictEqual(response.message, "ERR The message of a 'PING' command must be a bulk string");
			}).then(() => {
				client.end();
				done();
			}).catch((err) => {
				done(err);
			});
		});
	});

	describe('ECHO', () => {
		it('empty echo', (done) => {
			let client = null;
			server.then(() => {
				return rc.connect('localhost', config.processPorts[0]);
			}).then((c) => {
				client = c;
			}).then(() => {
				return rc.sendRequestAndReadResponse(client, ["ECHO"]);
			}).then((response) => {
				assert(response instanceof Error);
				assert.strictEqual(response.message, "ERR The 'ECHO' command only accepts one message");
				return response;
			}).then(() => {
				client.end();
				done();
			}).catch((err) => {
				client.end();
				done(err);
			});
		});

		it('echo with one message', (done) => {
			let client = null;
			server.then(() => {
				return rc.connect('localhost', config.processPorts[0]);
			}).then((c) => {
				client = c;
			}).then(() => {
				return rc.sendRequestAndReadResponse(client, ["ECHO", "Test"]);
			}).then((response) => {
				assert(Buffer.isBuffer(response));
				assert.deepEqual(response, new Buffer("Test", 'utf-8'));
				return response;
			}).then(() => {
				client.end();
				done();
			}).catch((err) => {
				client.end();
				done(err);
			});
		});

		it('invalid ECHO', (done) => {
			let client = null;
			server.then(() => {
				return rc.connect('localhost', config.processPorts[0]);
			}).then((c) => {
				client = c;
			}).then(() => {
				return rc.sendRequestAndReadResponse(client, [ "ECHO", "Test", "Test" ]);
			}).then((response) => {
				assert(response instanceof Error);
				assert.strictEqual(response.message, "ERR The 'ECHO' command only accepts one message");
				return response;
			}).then(() => {
				client.write("*2\r\n$4\r\nECHO\r\n+Test\r\n");
				return rc.sendRequestAndReadResponse(client, null);
			}).then((response) => {
				assert(response instanceof Error);
				assert.strictEqual(response.message, "ERR The message of a 'ECHO' command must be a bulk string");
				return response;
			}).then(() => {
				client.end();
				done();
			}).catch((err) => {
				done(err);
			});
		});
	});

	describe('CLUSTER', () => {
		it('empty cluster', (done) => {
			let client = null;
			server.then(() => {
				return rc.connect('localhost', config.processPorts[0]);
			}).then((c) => {
				client = c;
			}).then(() => {
				return rc.sendRequestAndReadResponse(client, ["CLUSTER"]);
			}).then((response) => {
				assert(response instanceof Error);
				assert.strictEqual(response.message, "ERR Redis 'CLUSTER' command expects a sub-command");
				return response;
			}).then(() => {
				client.end();
				done();
			}).catch((err) => {
				client.end();
				done(err);
			});
		});

		it('unimplemented cluster command', (done) => {
			let client = null;
			server.then(() => {
				return rc.connect('localhost', config.processPorts[0]);
			}).then((c) => {
				client = c;
			}).then(() => {
				return rc.sendRequestAndReadResponse(client, ["CLUSTER", "NODES"]);
			}).then((response) => {
				assert(response instanceof Error);
				assert.strictEqual(response.message, "ERR Unknown command 'CLUSTER NODES'");
				return response;
			}).then(() => {
				client.end();
				done();
			}).catch((err) => {
				client.end();
				done(err);
			});
		});

		it('invalid ECHO', (done) => {
			let client = null;
			server.then(() => {
				return rc.connect('localhost', config.processPorts[0]);
			}).then((c) => {
				client = c;
			}).then(() => {
				client.write("*2\r\n$7\r\nCLUSTER\r\n+Test\r\n");
				return rc.sendRequestAndReadResponse(client, null);
			}).then((response) => {
				assert(response instanceof Error);
				assert.strictEqual(response.message, "ERR The sub-command element of a 'CLUSTER' command must be a bulk string");
				return response;
			}).then(() => {
				client.end();
				done();
			}).catch((err) => {
				done(err);
			});
		});
	});

	describe('Errors', () => {
		it('invalid data sent', (done) => {
			let client = null;
			server.then(() => {
				return rc.connect('localhost', config.processPorts[0]);
			}).then((c) => {
				client = c;
			}).then(() => {
				client.write("@Just Gibberish Data\r\n");
				return rc.sendRequestAndReadResponse(client, null);
			}).then((response) => {
				assert(response instanceof Error);
				assert.strictEqual(response.message, "ERR Invalid request sent Unknown object type '@'");
			}).then(() => {
				client.end();
				done();
			}).catch((err) => {
				done(err);
			});
		});

		it('invalid query, should be a buffer', (done) => {
			let client = null;
			server.then(() => {
				return rc.connect('localhost', config.processPorts[0]);
			}).then((c) => {
				client = c;
			}).then(() => {
				client.write("*1\r\n+PING\r\n");
				return rc.sendRequestAndReadResponse(client, null);
			}).then((response) => {
				assert(response instanceof Error);
				assert.strictEqual(response.message, "ERR The first element of a command must be a buffer");
			}).then(() => {
				client.end();
				done();
			}).catch((err) => {
				done(err);
			});
		});

		it('invalid queries', (done) => {
			let client = null;
			server.then(() => {
				return rc.connect('localhost', config.processPorts[0]);
			}).then((c) => {
				client = c;
			}).then(() => {
				return rc.sendRequestAndReadResponse(client, "Query");
			}).then(() => {
				client.end();
				done(new Error("Expected error"));
			}).catch((response) => {
				client.end();
				assert(response instanceof Error);
				assert.strictEqual(response.message, "All requests must be arrays");
				done();
			}).catch((err) => {
				done(err);
			});
		});

		it('invalid query, should be a buffer', (done) => {
			let client = null;
			server.then(() => {
				return rc.connect('localhost', config.processPorts[0]);
			}).then((c) => {
				client = c;
			}).then(() => {
				client.write("*1\r\n+PING\r\n");
				return rc.sendRequestAndReadResponse(client, null);
			}).then((response) => {
				assert(response instanceof Error);
				assert.strictEqual(response.message, "ERR The first element of a command must be a buffer");
			}).then(() => {
				client.end();
				done();
			}).catch((err) => {
				done(err);
			});
		});

		it('unknown query', (done) => {
			let client = null;
			server.then(() => {
				return rc.connect('localhost', config.processPorts[0]);
			}).then((c) => {
				client = c;
			}).then(() => {
				return rc.sendRequestAndReadResponse(client, ["BLAH"]);
			}).then((response) => {
				assert(response instanceof Error);
				assert.strictEqual(response.message, "ERR received unsupported command 'BLAH'");
			}).then(() => {
				client.end();
				done();
			}).catch((err) => {
				done(err);
			});
		});
	});

	describe('Edge cases', () => {
		it('data hanging in the pipe', (done) => {
			let client = null;
			server.then(() => {
				return rc.connect('localhost', config.processPorts[0]);
			}).then((c) => {
				client = c;
			}).then(() => {
				// Send 1 response plus a bit of the second
				config.nextResponses.push(new Buffer("+PANG\r\n+PUNG", "utf-8"));
				// Send the remaining data
				config.nextResponses.push(new Buffer("\r\n", "utf-8"));
			}).then(() => {
				return rc.sendRequestAndReadResponse(client, ["PING"]);
			}).then((response) => {
				assert.strictEqual(response, "PANG");
			}).then(() => {
				return rc.sendRequestAndReadResponse(client, ["PING"]);
			}).then((response) => {
				assert.strictEqual(response, "PUNG");
			}).then(() => {
				client.end();
				done();
			}).catch((err) => {
				done(err);
			});
		});

		it('invalid server response', (done) => {
			let client = null;
			server.then(() => {
				return rc.connect('localhost', config.processPorts[0]);
			}).then((c) => {
				client = c;
			}).then(() => {
				// Send an invalid response
				config.nextResponses.push(new Buffer("@PANG\r\n", "utf-8"));
			}).then(() => {
				return rc.sendRequestAndReadResponse(client, ["PING"]);
			}).then(() => {
				client.end();
				done(new Error("An exception should have been raised"));
			}).catch((err) => {
				assert(err instanceof Error);
				assert.strictEqual(err.message, "Unknown object type '@'");
				done();
			}).catch((err) => {
				done(err);
			});
		});

		it('client does not accept encoding change', (done) => {
			let client = null;
			server.then(() => {
				return rc.connect('localhost', config.processPorts[0]);
			}).then((c) => {
				client = c;
				client.setEncoding('utf-8');
			}).then(() => {
				client.end();
				done(Error("An error should have been raised"));
			}).catch((err) => {
				client.end();
				assert(err instanceof Error);
				assert.strictEqual(err.message, "You can not change the encoding for a redis client");
				done();
			}).catch((err) => {
				done(err);
			});
		});

		it('should handle exit', (done) => {
			let client = null;
			let localConfig = null;
			rh().then((c) => {
				localConfig = c;
				localConfig.on('stopped', () => {
					done();
				});
			}).then(() => {
				return rc.connect('localhost', localConfig.processPorts[0]);
			}).then((c) => {
				client = c;
			}).then(() => {
				return rc.sendRequestAndReadResponse(client, ["EXIT"]);
			}).catch((err) => {
				done(err);
				localConfig.stop();
			});
		});

		it('should handle quit', (done) => {
			let client = null;
			let localConfig = null;
			rh().then((c) => {
				localConfig = c;
				localConfig.on('stopped', () => {
					done();
				});
			}).then(() => {
				return rc.connect('localhost', localConfig.processPorts[0]);
			}).then((c) => {
				client = c;
			}).then(() => {
				return rc.sendRequestAndReadResponse(client, ["QUIT"]);
			}).catch((err) => {
				done(err);
				localConfig.stop();
			});
		});
	});

	describe('GET/SET', () => {
		it('get on non-existent key', (done) => {
			let client = null;
			const key = uuid();
			server.then(() => {
				const node = slotFor(key).master;
				return rc.connect(node.host, node.port);
			}).then((c) => {
				client = c;
			}).then(() => {
				return rc.sendRequestAndReadResponse(client, ["GET", key]);
			}).then((response) => {
				assert.strictEqual(response, null);
				return response;
			}).then(() => {
				client.end();
				done();
			}).catch((err) => {
				if(client) {
					client.end();
				}
				done(err);
			});
		});

		it('set on new key', (done) => {
			let client = null;
			const key = uuid();
			server.then(() => {
				const node = slotFor(key).master;
				return rc.connect(node.host, node.port);
			}).then((c) => {
				client = c;
			}).then(() => {
				return rc.sendRequestAndReadResponse(client, ["SET", key, `AZERTY${key}`]);
			}).then((response) => {
				assert.strictEqual(response, "OK");
				return response;
			}).then(() => {
				return rc.sendRequestAndReadResponse(client, ["GET", key]);
			}).then((response) => {
				assert.deepEqual(response, new Buffer(`AZERTY${key}`, 'utf-8'));
				return response;
			}).then(() => {
				client.end();
				done();
			}).catch((err) => {
				if(client) {
					client.end();
				}
				done(err);
			});
		});

		it('set on master get on slave', (done) => {
			let client = null;
			const key = uuid();
			server.then(() => {
				const node = slotFor(key).master;
				return rc.connect(node.host, node.port);
			}).then((c) => {
				client = c;
			}).then(() => {
				return rc.sendRequestAndReadResponse(client, ["SET", key, `AZERTY${key}`]);
			}).then((response) => {
				assert.strictEqual(response, "OK");
				return response;
			}).then(() => {
				client.end();
				const node = slotFor(key).slaves[0];
				return rc.connect(node.host, node.port);
			}).then((c) => {
				client = c;
			}).then(() => {
				return rc.sendRequestAndReadResponse(client, ["GET", key]);
			}).then((response) => {
				assert.deepEqual(response, new Buffer(`AZERTY${key}`, 'utf-8'));
				return response;
			}).then(() => {
				client.end();
				done();
			}).catch((err) => {
				if(client) {
					client.end();
				}
				done(err);
			});
		});

		it('set on new key without override', (done) => {
			let client = null;
			const key = uuid();
			server.then(() => {
				const node = slotFor(key).master;
				return rc.connect(node.host, node.port);
			}).then((c) => {
				client = c;
			}).then(() => {
				return rc.sendRequestAndReadResponse(client, ["SET", key, `AZERTY${key}`, "NX"]);
			}).then((response) => {
				assert.strictEqual(response, "OK");
				return response;
			}).then(() => {
				return rc.sendRequestAndReadResponse(client, ["GET", key]);
			}).then((response) => {
				assert.deepEqual(response, new Buffer(`AZERTY${key}`, 'utf-8'));
				return response;
			}).then(() => {
				client.end();
				done();
			}).catch((err) => {
				if(client) {
					client.end();
				}
				done(err);
			});
		});

		it('set on new key without create', (done) => {
			let client = null;
			const key = uuid();
			server.then(() => {
				const node = slotFor(key).master;
				return rc.connect(node.host, node.port);
			}).then((c) => {
				client = c;
			}).then(() => {
				return rc.sendRequestAndReadResponse(client, ["SET", key, `AZERTY${key}`, "XX"]);
			}).then((response) => {
				assert.strictEqual(response, null);
				return response;
			}).then(() => {
				return rc.sendRequestAndReadResponse(client, ["GET", key]);
			}).then((response) => {
				assert.strictEqual(response, null);
				return response;
			}).then(() => {
				client.end();
				done();
			}).catch((err) => {
				if(client) {
					client.end();
				}
				done(err);
			});
		});

		it('set on existing key', (done) => {
			let client = null;
			const key = uuid();
			server.then(() => {
				const node = slotFor(key).master;
				return rc.connect(node.host, node.port);
			}).then((c) => {
				client = c;
			}).then(() => {
				return rc.sendRequestAndReadResponse(client, ["SET", key, `AZERTY${key}`]);
			}).then((response) => {
				assert.strictEqual(response, "OK");
				return response;
			}).then(() => {
				return rc.sendRequestAndReadResponse(client, ["SET", key, `QWERTY${key}`]);
			}).then((response) => {
				assert.strictEqual(response, "OK");
				return response;
			}).then(() => {
				return rc.sendRequestAndReadResponse(client, ["GET", key]);
			}).then((response) => {
				assert.deepEqual(response, new Buffer(`QWERTY${key}`, 'utf-8'));
				return response;
			}).then(() => {
				client.end();
				done();
			}).catch((err) => {
				if(client) {
					client.end();
				}
				done(err);
			});
		});

		it('set on existing key without override', (done) => {
			let client = null;
			const key = uuid();
			server.then(() => {
				const node = slotFor(key).master;
				return rc.connect(node.host, node.port);
			}).then((c) => {
				client = c;
			}).then(() => {
				return rc.sendRequestAndReadResponse(client, ["SET", key, `AZERTY${key}`]);
			}).then((response) => {
				assert.strictEqual(response, "OK");
				return response;
			}).then(() => {
				return rc.sendRequestAndReadResponse(client, ["SET", key, `QWERTY${key}`, "NX"]);
			}).then((response) => {
				assert.strictEqual(response, null);
				return response;
			}).then(() => {
				return rc.sendRequestAndReadResponse(client, ["GET", key]);
			}).then((response) => {
				assert.deepEqual(response, new Buffer(`AZERTY${key}`, 'utf-8'));
				return response;
			}).then(() => {
				client.end();
				done();
			}).catch((err) => {
				if(client) {
					client.end();
				}
				done(err);
			});
		});

		it('set on existing key without create', (done) => {
			let client = null;
			const key = uuid();
			server.then(() => {
				const node = slotFor(key).master;
				return rc.connect(node.host, node.port);
			}).then((c) => {
				client = c;
			}).then(() => {
				return rc.sendRequestAndReadResponse(client, ["SET", key, `AZERTY${key}`]);
			}).then((response) => {
				assert.strictEqual(response, "OK");
				return response;
			}).then(() => {
				return rc.sendRequestAndReadResponse(client, ["SET", key, `QWERTY${key}`, "XX"]);
			}).then((response) => {
				assert.strictEqual(response, "OK");
				return response;
			}).then(() => {
				return rc.sendRequestAndReadResponse(client, ["GET", key]);
			}).then((response) => {
				assert.deepEqual(response, new Buffer(`QWERTY${key}`, 'utf-8'));
				return response;
			}).then(() => {
				client.end();
				done();
			}).catch((err) => {
				if(client) {
					client.end();
				}
				done(err);
			});
		});

		it('set with expiration in ms', (done) => {
			let client = null;
			let clock = sinon.useFakeTimers();
			const key = uuid();
			server.then(() => {
				const node = slotFor(key).master;
				return rc.connect(node.host, node.port);
			}).then((c) => {
				client = c;
			}).then(() => {
				return rc.sendRequestAndReadResponse(client, ["SET", key, `AZERTY${key}`, "PX", "1000"]);
			}).then((response) => {
				assert.strictEqual(response, "OK");
				return response;
			}).then(() => {
				return rc.sendRequestAndReadResponse(client, ["GET", key]);
			}).then((response) => {
				assert.deepEqual(response, new Buffer(`AZERTY${key}`, 'utf-8'));
				return response;
			}).then(() => {
				clock.tick(1001);
			}).then(() => {
				return rc.sendRequestAndReadResponse(client, ["GET", key]);
			}).then((response) => {
				assert.strictEqual(response, null);
				return response;
			}).then(() => {
				clock.restore();
				client.end();
				done();
			}).catch((err) => {
				clock.restore();
				if(client) {
					client.end();
				}
				done(err);
			});
		});

		it('set with expiration in sec', (done) => {
			let client = null;
			let clock = sinon.useFakeTimers();
			const key = uuid();
			server.then(() => {
				const node = slotFor(key).master;
				return rc.connect(node.host, node.port);
			}).then((c) => {
				client = c;
			}).then(() => {
				return rc.sendRequestAndReadResponse(client, ["SET", key, `AZERTY${key}`, "EX", "1"]);
			}).then((response) => {
				assert.strictEqual(response, "OK");
				return response;
			}).then(() => {
				return rc.sendRequestAndReadResponse(client, ["GET", key]);
			}).then((response) => {
				assert.deepEqual(response, new Buffer(`AZERTY${key}`, 'utf-8'));
				return response;
			}).then(() => {
				clock.tick(1001);
			}).then(() => {
				return rc.sendRequestAndReadResponse(client, ["GET", key]);
			}).then((response) => {
				assert.strictEqual(response, null);
				return response;
			}).then(() => {
				clock.restore();
				client.end();
				done();
			}).catch((err) => {
				clock.restore();
				if(client) {
					client.end();
				}
				done(err);
			});
		});

		it('set with one key in master of invalid slot', (done) => {
			let client = null;
			const key = uuid();
			server.then(() => {
				const node = nodeWithoutSlotFor(key);
				return rc.connect(node.host, node.port);
			}).then((c) => {
				client = c;
			}).then(() => {
				return rc.sendRequestAndReadResponse(client, ["SET", key, `AZERTY${key}`]);
			}).then((response) => {
				assert(response instanceof Error);
				assert.strictEqual(response.message, `ERR This node does not host a valid slot for key ${key}`);
				return response;
			}).then(() => {
				client.end();
				done();
			}).catch((err) => {
				if(client) {
					client.end();
				}
				done(err);
			});
		});

		it('get with one key in master of invalid slot', (done) => {
			let client = null;
			const key = uuid();
			server.then(() => {
				const node = nodeWithoutSlotFor(key);
				return rc.connect(node.host, node.port);
			}).then((c) => {
				client = c;
			}).then(() => {
				return rc.sendRequestAndReadResponse(client, ["GET", key]);
			}).then((response) => {
				assert(response instanceof Error);
				assert.strictEqual(response.message, `ERR This node does not host a valid slot for key ${key}`);
				return response;
			}).then(() => {
				client.end();
				done();
			}).catch((err) => {
				if(client) {
					client.end();
				}
				done(err);
			});
		});

		it('set with one key in slave of valid slot', (done) => {
			let client = null;
			const key = uuid();
			server.then(() => {
				const node = slotFor(key).slaves[0];
				return rc.connect(node.host, node.port);
			}).then((c) => {
				client = c;
			}).then(() => {
				return rc.sendRequestAndReadResponse(client, ["SET", key, `AZERTY${key}`]);
			}).then((response) => {
				assert(response instanceof Error);
				assert.strictEqual(response.message, `ERR This node is not a master for key ${key}`);
				return response;
			}).then(() => {
				client.end();
				done();
			}).catch((err) => {
				if(client) {
					client.end();
				}
				done(err);
			});
		});

		it('invalid set', (done) => {
			let client = null;
			const key = uuid();
			server.then(() => {
				const node = slotFor(key).slaves[0];
				return rc.connect(node.host, node.port);
			}).then((c) => {
				client = c;
			}).then(() => {
				return rc.sendRequestAndReadResponse(client, ["SET"]);
			}).then((response) => {
				assert(response instanceof Error);
				assert.strictEqual(response.message, "ERR Redis 'SET' command expects a key name and a value");
				return response;
			}).then(() => {
				client.write(new Buffer("*3\r\n$3\r\nSET\r\n+myKey\r\n$2\r\nOK\r\n", 'utf-8'));
				return rc.sendRequestAndReadResponse(client, null);
			}).then((response) => {
				assert(response instanceof Error);
				assert.strictEqual(response.message, "ERR Redis 'SET' command expects only bulk string parameters");
				return response;
			}).then(() => {
				return rc.sendRequestAndReadResponse(client, ["SET", key, `AZERTY${key}`, "TX"]);
			}).then((response) => {
				assert(response instanceof Error);
				assert.strictEqual(response.message, "ERR Redis 'SET' does not accept option 'TX'");
				return response;
			}).then(() => {
				client.end();
				done();
			}).catch((err) => {
				if(client) {
					client.end();
				}
				done(err);
			});
		});

		it('invalid get', (done) => {
			let client = null;
			const key = uuid();
			server.then(() => {
				const node = slotFor(key).slaves[0];
				return rc.connect(node.host, node.port);
			}).then((c) => {
				client = c;
			}).then(() => {
				return rc.sendRequestAndReadResponse(client, ["GET"]);
			}).then((response) => {
				assert(response instanceof Error);
				assert.strictEqual(response.message, "ERR Redis 'GET' command expects a key name as a bulk string parameter");
				return response;
			}).then(() => {
				client.write(new Buffer("*2\r\n$3\r\nGET\r\n+myKey\r\n", 'utf-8'));
				return rc.sendRequestAndReadResponse(client, null);
			}).then((response) => {
				assert(response instanceof Error);
				assert.strictEqual(response.message, "ERR Redis 'GET' command expects a key name as a bulk string parameter");
				return response;
			}).then(() => {
				return rc.sendRequestAndReadResponse(client, ["GET", key, `AZERTY${key}`]);
			}).then((response) => {
				assert(response instanceof Error);
				assert.strictEqual(response.message, "ERR Redis 'GET' command expects a key name as a bulk string parameter");
				return response;
			}).then(() => {
				client.end();
				done();
			}).catch((err) => {
				if(client) {
					client.end();
				}
				done(err);
			});
		});

		it('set invalid expirations', (done) => {
			let client = null;
			const key = uuid();
			server.then(() => {
				const node = slotFor(key).slaves[0];
				return rc.connect(node.host, node.port);
			}).then((c) => {
				client = c;
			}).then(() => {
				return rc.sendRequestAndReadResponse(client, ["SET", key, `AZERTY${key}`, "EX"]);
			}).then((response) => {
				assert(response instanceof Error);
				assert.strictEqual(response.message, "ERR Redis 'SET' option 'EX' expects a non-negative integer as a parameter");
				return response;
			}).then(() => {
				return rc.sendRequestAndReadResponse(client, ["SET", key, `AZERTY${key}`, "EX", "A"]);
			}).then((response) => {
				assert(response instanceof Error);
				assert.strictEqual(response.message, "ERR Redis 'SET' option 'EX' expects a non-negative integer as a parameter");
				return response;
			}).then(() => {
				client.end();
				done();
			}).catch((err) => {
				if(client) {
					client.end();
				}
				done(err);
			});
		});
	});

	// Well known slots (for 8 slots)
	//  Value: 0120, Hash: 3032, Slot:1 (node 1)
	//  Value: 0121, Hash: 7161, Slot:3 (node 3)
	//  Value: 0122, Hash: 11162, Slot:5 (node 1)
	//  Value: 0123, Hash: 15291, Slot:7 (node 3)
	//  Value: 0124, Hash: 2908, Slot:1 (node 1)
	//  Value: 0125, Hash: 7037, Slot:3 (node 3)
	//  Value: 0126, Hash: 11038, Slot:5 (node 1)
	//  Value: 0127, Hash: 15167, Slot:7 (node 3)
	describe('MSET', () => {
		it('mset one key', (done) => {
			let client = null;
			const key = uuid();
			server.then(() => {
				const node = slotFor(key).master;
				return rc.connect(node.host, node.port);
			}).then((c) => {
				client = c;
			}).then(() => {
				return rc.sendRequestAndReadResponse(client, ["MSET", key, `AZERTY${key}`]);
			}).then((response) => {
				assert.strictEqual(response, "OK");
				return response;
			}).then(() => {
				return rc.sendRequestAndReadResponse(client, ["GET", key]);
			}).then((response) => {
				assert.deepEqual(response, new Buffer(`AZERTY${key}`, 'utf-8'));
				return response;
			}).then(() => {
				client.end();
				done();
			}).catch((err) => {
				if(client) {
					client.end();
				}
				done(err);
			});
		});

		it('mset one key hash tag', (done) => {
			let client = null;
			const key = uuid();
			server.then(() => {
				const node = slotFor("0120").master;
				return rc.connect(node.host, node.port);
			}).then((c) => {
				client = c;
			}).then(() => {
				return rc.sendRequestAndReadResponse(client, ["MSET", `{0120}${key}`, `AZERTY${key}`]);
			}).then((response) => {
				assert.strictEqual(response, "OK");
				return response;
			}).then(() => {
				return rc.sendRequestAndReadResponse(client, ["GET", `{0120}${key}`]);
			}).then((response) => {
				assert.deepEqual(response, new Buffer(`AZERTY${key}`, 'utf-8'));
				return response;
			}).then(() => {
				client.end();
				done();
			}).catch((err) => {
				if(client) {
					client.end();
				}
				done(err);
			});
		});

		it('mset one key wrong slot', (done) => {
			let client = null;
			const key = uuid();
			server.then(() => {
				const node = nodeWithoutSlotFor(key);
				return rc.connect(node.host, node.port);
			}).then((c) => {
				client = c;
			}).then(() => {
				return rc.sendRequestAndReadResponse(client, ["MSET", key, `AZERTY${key}`]);
			}).then((response) => {
				assert(response instanceof Error);
				assert.strictEqual(response.message, `ERR This node does not host a valid slot for key ${key}`);
				return response;
			}).then(() => {
				client.end();
				done();
			}).catch((err) => {
				if(client) {
					client.end();
				}
				done(err);
			});
		});

		it('mset one key on slave', (done) => {
			let client = null;
			const key = uuid();
			server.then(() => {
				const node = slotFor(key).slaves[0];
				return rc.connect(node.host, node.port);
			}).then((c) => {
				client = c;
			}).then(() => {
				return rc.sendRequestAndReadResponse(client, ["MSET", key, `AZERTY${key}`]);
			}).then((response) => {
				assert(response instanceof Error);
				assert.strictEqual(response.message, `ERR This node is not a master for key ${key}`);
				return response;
			}).then(() => {
				client.end();
				done();
			}).catch((err) => {
				if(client) {
					client.end();
				}
				done(err);
			});
		});

		it('mset two keys in same slot', (done) => {
			let client = null;
			const key = uuid();
			server.then(() => {
				const node = slotFor("0120").master;
				return rc.connect(node.host, node.port);
			}).then((c) => {
				client = c;
			}).then(() => {
				return rc.sendRequestAndReadResponse(client, ["MSET", `{0120}${key}-1`, `AZERTY${key}`, `{0120}${key}-2`, `QWERTY${key}`]);
			}).then((response) => {
				assert.strictEqual(response, "OK");
				return response;
			}).then(() => {
				return rc.sendRequestAndReadResponse(client, ["GET", `{0120}${key}-1`]);
			}).then((response) => {
				assert.deepEqual(response, new Buffer(`AZERTY${key}`, 'utf-8'));
				return response;
			}).then(() => {
				return rc.sendRequestAndReadResponse(client, ["GET", `{0120}${key}-2`]);
			}).then((response) => {
				assert.deepEqual(response, new Buffer(`QWERTY${key}`, 'utf-8'));
				return response;
			}).then(() => {
				client.end();
				done();
			}).catch((err) => {
				if(client) {
					client.end();
				}
				done(err);
			});
		});

		it('mset two keys in different slots on same node', (done) => {
			let client = null;
			const key = uuid();
			server.then(() => {
				const node = slotFor("0120").master;
				return rc.connect(node.host, node.port);
			}).then((c) => {
				client = c;
			}).then(() => {
				return rc.sendRequestAndReadResponse(client, ["MSET", `{0120}${key}-1`, `AZERTY${key}`, `{0122}${key}-2`, `QWERTY${key}`]);
			}).then((response) => {
				assert.strictEqual(response, "OK");
				return response;
			}).then(() => {
				return rc.sendRequestAndReadResponse(client, ["GET", `{0120}${key}-1`]);
			}).then((response) => {
				assert.deepEqual(response, new Buffer(`AZERTY${key}`, 'utf-8'));
				return response;
			}).then(() => {
				return rc.sendRequestAndReadResponse(client, ["GET", `{0122}${key}-2`]);
			}).then((response) => {
				assert.deepEqual(response, new Buffer(`QWERTY${key}`, 'utf-8'));
				return response;
			}).then(() => {
				client.end();
				done();
			}).catch((err) => {
				if(client) {
					client.end();
				}
				done(err);
			});
		});

		it('mset two keys in different slots on different nodes', (done) => {
			let client = null;
			const key = uuid();
			server.then(() => {
				const node = slotFor("0120").master;
				return rc.connect(node.host, node.port);
			}).then((c) => {
				client = c;
			}).then(() => {
				return rc.sendRequestAndReadResponse(client, ["MSET", `{0120}${key}-1`, `AZERTY${key}`, `{0121}${key}-2`, `QWERTY${key}`]);
			}).then((response) => {
				assert(response instanceof Error);
				assert.strictEqual(response.message, `ERR This node does not host a valid slot for key {0121}${key}-2`);
				return response;
			}).then(() => {
				client.end();
				done();
			}).catch((err) => {
				if(client) {
					client.end();
				}
				done(err);
			});
		});

		it('invalid mset', (done) => {
			let client = null;
			const key = uuid();
			server.then(() => {
				const node = slotFor(key).master;
				return rc.connect(node.host, node.port);
			}).then((c) => {
				client = c;
			}).then(() => {
				return rc.sendRequestAndReadResponse(client, ["MSET"]);
			}).then((response) => {
				assert(response instanceof Error);
				assert.strictEqual(response.message, "ERR Redis 'MSET' command expects bulk string key-value pairs");
				return response;
			}).then(() => {
				return rc.sendRequestAndReadResponse(client, ["MSET", key]);
			}).then((response) => {
				assert(response instanceof Error);
				assert.strictEqual(response.message, "ERR Redis 'MSET' command expects bulk string key-value pairs");
				return response;
			}).then(() => {
				return rc.sendRequestAndReadResponse(client, ["MSET", key, "AZERTY", `${key}-2` ]);
			}).then((response) => {
				assert(response instanceof Error);
				assert.strictEqual(response.message, "ERR Redis 'MSET' command expects bulk string key-value pairs");
				return response;
			}).then(() => {
				client.write(new Buffer("*3\r\n$4\r\nMSET\r\n+myKey\r\n$2\r\nOK\r\n", 'utf-8'));
				return rc.sendRequestAndReadResponse(client, null);
			}).then((response) => {
				assert(response instanceof Error);
				assert.strictEqual(response.message, "ERR Redis 'MSET' command expects bulk string key-value pairs");
				return response;
			}).then(() => {
				client.end();
				done();
			}).catch((err) => {
				if(client) {
					client.end();
				}
				done(err);
			});
		});
	});

	describe('config errors', () => {
		it('config errors', (done) => {
			assert.throws(() => rh({ nodes: -1 }), /The number of nodes must be a positive integer up to 255/);
			assert.throws(() => rh({ nodes: 0 }), /The number of nodes must be a positive integer up to 255/);
			assert.throws(() => rh({ nodes: 256 }), /The number of nodes must be a positive integer up to 255/);
			assert.throws(() => rh({ nodes: "a" }), /The number of nodes must be a positive integer up to 255/);
			assert.throws(() => rh({ replicas: -1 }), /The number of replicas must be a non-negative integer up to 20/);
			assert.throws(() => rh({ replicas: 21 }), /The number of replicas must be a non-negative integer up to 20/);
			assert.throws(() => rh({ replicas: "a" }), /The number of replicas must be a non-negative integer up to 20/);
			assert.throws(() => rh({ slotsPerNode: -1 }), /The number of slots per node must be a positive integer up to 255/);
			assert.throws(() => rh({ slotsPerNode: 256 }), /The number of slots per node must be a positive integer up to 255/);
			assert.throws(() => rh({ slotsPerNode: "a" }), /The number of slots per node must be a positive integer up to 255/);
			assert.throws(() => rh({ replicas: 2 }), /The number of replicas must be less than the number of nodes/);
			assert.throws(() => rh({ nodes: 164, slotsPerNode: 100 }), /The total number of slots can not be above 16384/);
			done();
		});
	});
});
