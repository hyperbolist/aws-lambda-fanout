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
 * This Node.js script tests the features from the queue.js script
 */

/* global describe:false, it:false */

// node -e 'const v="0120";const s=4;const h=require("./lib/crc16.js").xmodem(v)%16384;console.log(`Value: ${v}, Hash: ${h}, Slot:${Math.floor(h/(16384/s))}`)'
// Value: 0120, Hash: 3032, Slot:0
// Value: 0121, Hash: 7161, Slot:1
// Value: 0122, Hash: 11162, Slot:2
// Value: 0123, Hash: 15291, Slot:3
// Value: 0124, Hash: 2908, Slot:0
// Value: 0125, Hash: 7037, Slot:1
// Value: 0126, Hash: 11038, Slot:2
// Value: 0127, Hash: 15167, Slot:3

'use strict';

const rc = require('../lib/redis-common.js');
const rh = require('../test-hosts/redis-host.js');
const r = require('../lib/redis.js');
const uuid = require('uuid');
const assert = require('assert');

let config = null;
let slots = [];
const server = rh({ nodes: 2, slotsPerNode: 2, replicas: 1 }).then((c) => config = c);

const discover = server.then(() => {
	return r.servers(`localhost:${config.processPorts[0]}`);
}).then((s) => {
	slots = s;
});

describe('redis-config', () => {
	it('should provide the server list', (done) => {
		server.then(() => {
			return r.servers(`localhost:${config.processPorts[0]}`);
		}).then((response) => {
			assert.strictEqual(response.length, 4);
			assert.strictEqual(response[0].start, 0);
			assert.strictEqual(response[0].end, 4095);
			assert.strictEqual(response[0].master.host, '127.0.0.1');
			assert.strictEqual(response[0].master.port, config.processPorts[0]);
			assert.strictEqual(response[0].slaves[0].host, '127.0.0.1');
			assert.strictEqual(response[0].slaves[0].port, config.processPorts[1]);
			assert.strictEqual(response[1].start, 4096);
			assert.strictEqual(response[1].end, 8191);
			assert.strictEqual(response[1].master.host, '127.0.0.1');
			assert.strictEqual(response[1].master.port, config.processPorts[1]);
			assert.strictEqual(response[1].slaves[0].host, '127.0.0.1');
			assert.strictEqual(response[1].slaves[0].port, config.processPorts[0]);
			assert.strictEqual(response[2].start, 8192);
			assert.strictEqual(response[2].end, 12287);
			assert.strictEqual(response[2].master.host, '127.0.0.1');
			assert.strictEqual(response[2].master.port, config.processPorts[0]);
			assert.strictEqual(response[2].slaves[0].host, '127.0.0.1');
			assert.strictEqual(response[2].slaves[0].port, config.processPorts[1]);
			assert.strictEqual(response[3].start, 12288);
			assert.strictEqual(response[3].end, 16383);
			assert.strictEqual(response[3].master.host, '127.0.0.1');
			assert.strictEqual(response[3].master.port, config.processPorts[1]);
			assert.strictEqual(response[3].slaves[0].host, '127.0.0.1');
			assert.strictEqual(response[3].slaves[0].port, config.processPorts[0]);
			done();
		}).catch((err) => {
			done(err);
		});
	});

	it('second node should provide the same server list', (done) => {
		server.then(() => {
			return r.servers(`localhost:${config.processPorts[1]}`);
		}).then((response) => {
			assert.strictEqual(response.length, 4);
			assert.strictEqual(response[0].start, 0);
			assert.strictEqual(response[0].end, 4095);
			assert.strictEqual(response[0].master.host, '127.0.0.1');
			assert.strictEqual(response[0].master.port, config.processPorts[0]);
			assert.strictEqual(response[0].slaves.length, 1);
			assert.strictEqual(response[0].slaves[0].host, '127.0.0.1');
			assert.strictEqual(response[0].slaves[0].port, config.processPorts[1]);
			assert.strictEqual(response[1].start, 4096);
			assert.strictEqual(response[1].end, 8191);
			assert.strictEqual(response[1].master.host, '127.0.0.1');
			assert.strictEqual(response[1].master.port, config.processPorts[1]);
			assert.strictEqual(response[1].slaves.length, 1);
			assert.strictEqual(response[1].slaves[0].host, '127.0.0.1');
			assert.strictEqual(response[1].slaves[0].port, config.processPorts[0]);
			assert.strictEqual(response[2].start, 8192);
			assert.strictEqual(response[2].end, 12287);
			assert.strictEqual(response[2].master.host, '127.0.0.1');
			assert.strictEqual(response[2].master.port, config.processPorts[0]);
			assert.strictEqual(response[2].slaves.length, 1);
			assert.strictEqual(response[2].slaves[0].host, '127.0.0.1');
			assert.strictEqual(response[2].slaves[0].port, config.processPorts[1]);
			assert.strictEqual(response[3].start, 12288);
			assert.strictEqual(response[3].end, 16383);
			assert.strictEqual(response[3].master.host, '127.0.0.1');
			assert.strictEqual(response[3].master.port, config.processPorts[1]);
			assert.strictEqual(response[3].slaves.length, 1);
			assert.strictEqual(response[3].slaves[0].host, '127.0.0.1');
			assert.strictEqual(response[3].slaves[0].port, config.processPorts[0]);
			done();
		}).catch((err) => {
			done(err);
		});
	});

	it('should handle id when provided', (done) => {
		server.then(() => {
			config.nextResponses.push(new Buffer("*1\r\n*3\r\n:0\r\n:16383\r\n*3\r\n+10.0.0.1\r\n:12500\r\n+SERVERNAME\r\n", "utf-8"));
			return r.servers(`localhost:${config.processPorts[0]}`);
		}).then((response) => {
			assert.strictEqual(response.length, 1);
			assert.strictEqual(response[0].start, 0);
			assert.strictEqual(response[0].end, 16383);
			assert.strictEqual(response[0].master.host, '10.0.0.1');
			assert.strictEqual(response[0].master.port, 12500);
			assert.strictEqual(response[0].master.id, "SERVERNAME");
			assert.strictEqual(response[0].slaves.length, 0);
			done();
		}).catch((err) => {
			done(err);
		});
	});

	it('should generate id when provided', (done) => {
		server.then(() => {
			config.nextResponses.push(new Buffer("*1\r\n*3\r\n:0\r\n:16383\r\n*2\r\n+10.0.0.1\r\n:12500\r\n", "utf-8"));
			return r.servers(`localhost:${config.processPorts[0]}`);
		}).then((response) => {
			assert.strictEqual(response.length, 1);
			assert.strictEqual(response[0].start, 0);
			assert.strictEqual(response[0].end, 16383);
			assert.strictEqual(response[0].master.host, '10.0.0.1');
			assert.strictEqual(response[0].master.port, 12500);
			assert.strictEqual(response[0].master.id, "10.0.0.1:12500");
			assert.strictEqual(response[0].slaves.length, 0);
			done();
		}).catch((err) => {
			done(err);
		});
	});

	it('should handle server errors on server list', (done) => {
		server.then(() => {
			config.nextResponses.push(new Buffer("-ERR Invalid connection\r\n", "utf-8"));
			return r.servers(`localhost:${config.processPorts[0]}`);
		}).then(() => {
			done(new Error("An error should have been raised"));
		}).catch((err) => {
			assert(err instanceof Error);
			assert.strictEqual(err.message, "ERR Invalid connection");
			done();
		}).catch((err) => {
			done(err);
		});
	});

	it('should handle server invalid on server list (top level)', (done) => {
		server.then(() => {
			config.nextResponses.push(new Buffer("+OK\r\n", "utf-8"));
			return r.servers(`localhost:${config.processPorts[0]}`);
		}).then(() => {
			done(new Error("An error should have been raised"));
		}).catch((err) => {
			assert(err instanceof Error);
			assert.strictEqual(err.message, "Invalid response, expecting a slot list as an array");
			done();
		}).catch((err) => {
			done(err);
		});
	});

	it('should handle server invalid on server list (slot list)', (done) => {
		server.then(() => {
			config.nextResponses.push(new Buffer("*1\r\n+OK\r\n", "utf-8"));
			return r.servers(`localhost:${config.processPorts[0]}`);
		}).then(() => {
			done(new Error("An error should have been raised"));
		}).catch((err) => {
			assert(err instanceof Error);
			assert.strictEqual(err.message, "Invalid response element, should be an array containing contain an integer for the slot start, an integer for the slot end, and a list of nodes storing these slots");
			done();
		}).catch((err) => {
			done(err);
		});
	});

	it('should handle server invalid on server list (slot description)', (done) => {
		server.then(() => {
			config.nextResponses.push(new Buffer("*1\r\n*3\r\n:0\r\n:16383\r\n+OK\r\n", "utf-8"));
			return r.servers(`localhost:${config.processPorts[0]}`);
		}).then(() => {
			done(new Error("An error should have been raised"));
		}).catch((err) => {
			assert(err instanceof Error);
			assert.strictEqual(err.message, "Invalid response element, expecting an array containing a host name and a port number");
			done();
		}).catch((err) => {
			done(err);
		});
	});

	it('connect to non-existent server', (done) => {
		server.then(() => {
			return r.servers(`127.0.0.256:49272`);
		}).then(() => {
			done(new Error("An error should have been raised"));
		}).catch((err) => {
			assert(err instanceof Error);
			assert.strictEqual(err.message, "getaddrinfo ENOTFOUND 127.0.0.256 127.0.0.256:49272");
			done();
		}).catch((err) => {
			done(err);
		});
	});
});

describe('redis-client', () => {
	it('store one record', (done) => {
		const key = uuid();
		let client = null;
		discover.then(() => {
			return r.set(slots, [ { key: `{0120}${key}`, data: new Buffer(`AZERTY${key}`, "utf-8") } ]);
		}).then(() => {
			// 0120 --> Slot 0
			return rc.connect(slots[0].master.host, slots[0].master.port);
		}).then((c) => {
			client = c;
		}).then(() => {
			return rc.sendRequestAndReadResponse(client, ["GET", `{0120}${key}`]);
		}).then((response) => {
			assert.deepEqual(response, new Buffer(`AZERTY${key}`, 'utf-8'));
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

	it('store one record (as string)', (done) => {
		const key = uuid();
		let client = null;
		discover.then(() => {
			return r.set(slots, [ { key: `{0120}${key}`, data: `AZERTY${key}` } ]);
		}).then(() => {
			// 0120 --> Slot 0
			return rc.connect(slots[0].master.host, slots[0].master.port);
		}).then((c) => {
			client = c;
		}).then(() => {
			return rc.sendRequestAndReadResponse(client, ["GET", `{0120}${key}`]);
		}).then((response) => {
			assert.deepEqual(response, new Buffer(`AZERTY${key}`, 'utf-8'));
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

	it('store two records in same slot', (done) => {
		const key = uuid();
		let client = null;
		discover.then(() => {
			return r.set(slots, [ { key: `{0120}${key}-1`, data: new Buffer(`AZERTY${key}`, "utf-8") }, { key: `{0120}${key}-2`, data: new Buffer(`QWERTY${key}`, "utf-8") } ]);
		}).then(() => {
			// 0120 --> Slot 0
			return rc.connect(slots[0].master.host, slots[0].master.port);
		}).then((c) => {
			client = c;
		}).then(() => {
			return rc.sendRequestAndReadResponse(client, ["GET", `{0120}${key}-1`]);
		}).then((response) => {
			assert.deepEqual(response, new Buffer(`AZERTY${key}`, 'utf-8'));
		}).then(() => {
			return rc.sendRequestAndReadResponse(client, ["GET", `{0120}${key}-2`]);
		}).then((response) => {
			assert.deepEqual(response, new Buffer(`QWERTY${key}`, 'utf-8'));
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

	it('store two records in different slots on same node', (done) => {
		const key = uuid();
		let client = null;
		discover.then(() => {
			return r.set(slots, [ { key: `{0120}${key}-1`, data: new Buffer(`AZERTY${key}`, "utf-8") }, { key: `{0124}${key}-2`, data: new Buffer(`QWERTY${key}`, "utf-8") } ]);
		}).then(() => {
			// 0120 --> Slot 0
			// 0124 --> Slot 0
			return rc.connect(slots[0].master.host, slots[0].master.port);
		}).then((c) => {
			client = c;
		}).then(() => {
			return rc.sendRequestAndReadResponse(client, ["GET", `{0120}${key}-1`]);
		}).then((response) => {
			assert.deepEqual(response, new Buffer(`AZERTY${key}`, 'utf-8'));
		}).then(() => {
			return rc.sendRequestAndReadResponse(client, ["GET", `{0124}${key}-2`]);
		}).then((response) => {
			assert.deepEqual(response, new Buffer(`QWERTY${key}`, 'utf-8'));
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

	it('store two records in different nodes', (done) => {
		const key = uuid();
		let client = null;
		discover.then(() => {
			return r.set(slots, [ { key: `{0120}${key}-1`, data: new Buffer(`AZERTY${key}`, "utf-8") }, { key: `{0121}${key}-2`, data: new Buffer(`QWERTY${key}`, "utf-8") } ]);
		}).then(() => {
			// 0120 --> Slot 0
			return rc.connect(slots[0].master.host, slots[0].master.port);
		}).then((c) => {
			client = c;
		}).then(() => {
			return rc.sendRequestAndReadResponse(client, ["GET", `{0120}${key}-1`]);
		}).then((response) => {
			assert.deepEqual(response, new Buffer(`AZERTY${key}`, 'utf-8'));
		}).then(() => {
			client.end();
			// 0121 --> Slot 1
			return rc.connect(slots[1].master.host, slots[1].master.port);
		}).then((c) => {
			client = c;
		}).then(() => {
			return rc.sendRequestAndReadResponse(client, ["GET", `{0121}${key}-2`]);
		}).then((response) => {
			assert.deepEqual(response, new Buffer(`QWERTY${key}`, 'utf-8'));
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

	it('store two records in different nodes (in parallel)', (done) => {
		const key = uuid();
		let client = null;
		discover.then(() => {
			return r.set(slots, [ { key: `{0120}${key}-1`, data: new Buffer(`AZERTY${key}`, "utf-8") }, { key: `{0121}${key}-2`, data: new Buffer(`QWERTY${key}`, "utf-8") } ], { parallel: 2 });
		}).then(() => {
			// 0120 --> Slot 0
			return rc.connect(slots[0].master.host, slots[0].master.port);
		}).then((c) => {
			client = c;
		}).then(() => {
			return rc.sendRequestAndReadResponse(client, ["GET", `{0120}${key}-1`]);
		}).then((response) => {
			assert.deepEqual(response, new Buffer(`AZERTY${key}`, 'utf-8'));
		}).then(() => {
			client.end();
			// 0121 --> Slot 1
			return rc.connect(slots[1].master.host, slots[1].master.port);
		}).then((c) => {
			client = c;
		}).then(() => {
			return rc.sendRequestAndReadResponse(client, ["GET", `{0121}${key}-2`]);
		}).then((response) => {
			assert.deepEqual(response, new Buffer(`QWERTY${key}`, 'utf-8'));
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

	it('fails non existent server', (done) => {
		const key = uuid();
		server.then(() => {
			return r.set([ { start: 0, end: 16383, master: { id: 'none', host: "10.0.0.260", port: 1000 }, slaves: []} ], [ { key: key, data: new Buffer(`AZERTY${key}`, "utf-8") } ]);
		}).then(() => {
			done(new Error("Expecting error"));
		}).catch((err) => {
			assert(err instanceof Error);
			assert.strictEqual(err.message,`getaddrinfo ENOTFOUND 10.0.0.260 10.0.0.260:1000`);
			done();
		}).catch((err) => {
			done(err);
		});
	});

	it('fails no slot available', (done) => {
		const key = uuid();
		server.then(() => {
			return r.set([ { start: 0, end: 0, master: null, slaves: []} ], [ { key: key, data: new Buffer(`AZERTY${key}`, "utf-8") } ]);
		}).then(() => {
			done(new Error("Expecting error"));
		}).catch((err) => {
			assert(err instanceof Error);
			assert.strictEqual(err.message,`Unable to find suitable slot for current record with id: ${key}`);
			done();
		}).catch((err) => {
			done(err);
		});
	});

	it('fails invalid server response', (done) => {
		const key = uuid();
		server.then(() => {
			config.nextResponses.push(new Buffer("-ERR Invalid response\r\n", 'utf-8'));
			return r.set(slots, [ { key: key, data: new Buffer(`AZERTY${key}`, "utf-8") } ]);
		}).then(() => {
			done(new Error("Expecting error"));
		}).catch((err) => {
			assert(err instanceof Error);
			assert.strictEqual(err.message,"Error occured while sending item to Redis: Error: ERR Invalid response");
			done();
		}).catch((err) => {
			done(err);
		});
	});

	it('fails if no slot provided', (done) => {
		const key = uuid();
		server.then(() => {
			return r.set([], [ { key: key, data: new Buffer(`AZERTY${key}`, "utf-8") } ]);
		}).then(() => {
			done(new Error("Expecting error"));
		}).catch((err) => {
			assert(err instanceof Error);
			assert.strictEqual(err.message,"You must specify a non empty list of Redis slots");
			done();
		}).catch((err) => {
			done(err);
		});
	});

	it('fails if no record provided', (done) => {
		discover.then(() => {
			return r.set(slots, []);
		}).then(() => {
			done(new Error("Expecting error"));
		}).catch((err) => {
			assert(err instanceof Error);
			assert.strictEqual(err.message,"You must specify a non empty list of records to store in Redis");
			done();
		}).catch((err) => {
			done(err);
		});
	});

	it('fails if record does not have a key', (done) => {
		const key = uuid();
		discover.then(() => {
			return r.set(slots, [ { data: new Buffer(`AZERTY${key}`, "utf-8") } ]);
		}).then(() => {
			done(new Error("Expecting error"));
		}).catch((err) => {
			assert(err instanceof Error);
			assert.strictEqual(err.message,"All records must have a 'key' property as a string");
			done();
		}).catch((err) => {
			done(err);
		});
	});

	it('fails if record has a non string key', (done) => {
		const key = uuid();
		discover.then(() => {
			return r.set(slots, [ { key: 10, data: new Buffer(`AZERTY${key}`, "utf-8") } ]);
		}).then(() => {
			done(new Error("Expecting error"));
		}).catch((err) => {
			assert(err instanceof Error);
			assert.strictEqual(err.message,"All records must have a 'key' property as a string");
			done();
		}).catch((err) => {
			done(err);
		});
	});

	it('fails if record does not have data', (done) => {
		const key = uuid();
		discover.then(() => {
			return r.set(slots, [ { key: key } ]);
		}).then(() => {
			done(new Error("Expecting error"));
		}).catch((err) => {
			assert(err instanceof Error);
			assert.strictEqual(err.message,"All records must have a 'data' property as a string or buffer");
			done();
		}).catch((err) => {
			done(err);
		});
	});

	it('fails if record has a non buffer/string data', (done) => {
		const key = uuid();
		discover.then(() => {
			return r.set(slots, [ { key: key, data: 10 } ]);
		}).then(() => {
			done(new Error("Expecting error"));
		}).catch((err) => {
			assert(err instanceof Error);
			assert.strictEqual(err.message,"All records must have a 'data' property as a string or buffer");
			done();
		}).catch((err) => {
			done(err);
		});
	});
});
