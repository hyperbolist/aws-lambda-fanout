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

'use strict';

const mch = require('../test-hosts/memcached-host.js');
const mcc = require('../lib/memcached.js');

const assert = require('assert');
let config = null;
const server = mch({ shards: 2 }).then((c) => config = c);

describe('memcached-client-config', () => {
	it('should provide the server list', (done) => {
		server.then(() => {
			return mcc.servers(`localhost:${config.configPort}`);
		}).then((response) => {
			assert.strictEqual(response.length, config.processPorts.length);
			response.forEach((entry, index) => {
				assert.strictEqual(entry.id, `127.0.0.1:${config.processPorts[index]}`);
			});
		}).then(() => {
			done();
		}).catch((err) => {
			done(err);
		});
	});

	it('can not connect to non-existent hosts', (done) => {
		server.then(() => {
			return mcc.servers(`127.0.0.256:${config.configPort}`);
		}).then((response) => {
			assert.strictEqual(response.length, config.processPorts.length);
			response.forEach((entry, index) => {
				assert.strictEqual(entry, `127.0.0.1:${config.processPorts[index]}`);
			});
		}).then(() => {
			done(new Error("An error should have been thrown"));
		}).catch((err) => {
			assert(err instanceof Error);
			assert.strictEqual(err.message, `getaddrinfo ENOTFOUND 127.0.0.256 127.0.0.256:${config.configPort}`);
			done();
		}).catch((err) => {
			done(err);
		});
	});

	it('empty configuration response (length)', (done) => {
		server.then(() => {
			config.nextResponses.push("END\r\n");
			return mcc.servers(`localhost:${config.configPort}`);
		}).then(() => {
			done(new Error("An error should have been thrown"));
		}).catch((err) => {
			assert(err instanceof Error);
			assert.strictEqual(err.message, "Invalid response, expecting a CONFIG entry");
			done();
		}).catch((err) => {
			done(err);
		});
	});

	it('invalid configuration response (type)', (done) => {
		server.then(() => {
			config.nextResponses.push("VALUE abcdef 0 6\r\nAZERTY\r\nEND\r\n");
			return mcc.servers(`localhost:${config.configPort}`);
		}).then(() => {
			done(new Error("An error should have been thrown"));
		}).catch((err) => {
			assert(err instanceof Error);
			assert.strictEqual(err.message, "Invalid response element VALUE, expecting a CONFIG entry");
			done();
		}).catch((err) => {
			done(err);
		});
	});

	it('invalid configuration response (data)', (done) => {
		server.then(() => {
			config.nextResponses.push("CONFIG cluster 0 6\r\nAZERTY\r\nEND\r\n");
			return mcc.servers(`localhost:${config.configPort}`);
		}).then(() => {
			done(new Error("An error should have been thrown"));
		}).catch((err) => {
			assert(err instanceof Error);
			assert.strictEqual(err.message,'Invalid configuration data ["AZERTY"]');
			done();
		}).catch((err) => {
			done(err);
		});
	});

	it('invalid configuration response (server)', (done) => {
		server.then(() => {
			config.nextResponses.push("CONFIG cluster 0 20\r\nAZERTY\nQWERTY\nDVORAK\r\nEND\r\n");
			return mcc.servers(`localhost:${config.configPort}`);
		}).then(() => {
			done(new Error("An error should have been thrown"));
		}).catch((err) => {
			assert(err instanceof Error);
			assert.strictEqual(err.message,'Invalid server entry "QWERTY"');
			done();
		}).catch((err) => {
			done(err);
		});
	});
});

describe('memcached-client-data', () => {
	// md5 hash
	//  0120 ==> 408b7c8ad06e4d9954fa2d948a01f508 (first slot)
	//  0121 ==> fee67cadcc3a0bec8e00641884903c45 (second slot)
	//  0122 ==> f0f4b6598f2cee45644673998b4f44be (second slot)
	//  0123 ==> eb62f6b9306db575c2d596b1279627a4 (second slot)
	//  0124 ==> a4bab3ce420ea5342f99b468206738eb (second slot)
	//  0125 ==> e36e7d9b1f0df7460ad3ac1958527273 (second slot)
	//  0126 ==> f6c0c55d8d0a5cd1f69ceaf3d5b4e108 (second slot)
	//  0127 ==> 4721dc6a803c213861d569b31f3c121f (first slot)
	//  abcdef ==> e80b5017098950fc58aad83c8c14978e (second slot)

	// Redis crc
	//  0120 ==> 3032  (first slot)
	//  0121 ==> 7161  (first slot)
	//  0122 ==> 11162 (second slot)
	//  0123 ==> 15291 (second slot)
	//  0124 ==> 2908  (first slot)
	//  0125 ==> 7037  (first slot)
	//  0126 ==> 11038 (second slot)
	//  0127 ==> 15167 (second slot)
	//  abcdef ==> 15101 (second slot)
	//  {abcdef}-dodo ==> 3742  (first slot)
	//  {abcdef-dodo ==>  8476  (second slot)
	//  ab{}cdef-dodo ==> 10605 (second slot)
	//  abc def ==>       8311  (second slot)

	it('can set an entry with default settings', (done) => {
		let servers = null;
		server.then(() => {
			return mcc.servers(`localhost:${config.configPort}`);
		}).then((serverList) => {
			servers = serverList;
		}).then(() => {
			return mcc.set(servers, [ { key: "abcdef", data: new Buffer("AZERTY", "utf-8") } ]);
		}).then((response) => {
			assert.strictEqual(response.length, 1);
			assert.strictEqual(response[0].processed, true);
			assert.strictEqual(response[0].error, null);
			assert.strictEqual(response[0].server.id, `127.0.0.1:${config.processPorts[1]}`);
			done();
		}).catch((err) => {
			done(err);
		});
	});

	it('can set a string entry', (done) => {
		let servers = null;
		server.then(() => {
			return mcc.servers(`localhost:${config.configPort}`);
		}).then((serverList) => {
			servers = serverList;
		}).then(() => {
			return mcc.set(servers, [ { key: "abcdef", data: "AZERTY" } ]);
		}).then((response) => {
			assert.strictEqual(response.length, 1);
			assert.strictEqual(response[0].processed, true);
			assert.strictEqual(response[0].error, null);
			assert.strictEqual(response[0].server.id, `127.0.0.1:${config.processPorts[1]}`);
			done();
		}).catch((err) => {
			done(err);
		});
	});

	it('can set an entry with an algorithm (redis)', (done) => {
		let servers = null;
		server.then(() => {
			return mcc.servers(`localhost:${config.configPort}`);
		}).then((serverList) => {
			servers = serverList;
		}).then(() => {
			return mcc.set(servers, [ { key: "abcdef", data: new Buffer("AZERTY", "utf-8") } ], { hash: 'redis' });
		}).then((response) => {
			assert.strictEqual(response.length, 1);
			assert.strictEqual(response[0].processed, true);
			assert.strictEqual(response[0].error, null);
			assert.strictEqual(response[0].server.id, `127.0.0.1:${config.processPorts[1]}`);
			done();
		}).catch((err) => {
			done(err);
		});
	});

	it('can use a hash tag by default', (done) => {
		let servers = null;
		server.then(() => {
			return mcc.servers(`localhost:${config.configPort}`);
		}).then((serverList) => {
			servers = serverList;
		}).then(() => {
			return mcc.set(servers, [ { key: "{abcdef}-dodo", data: new Buffer("AZERTY", "utf-8") } ], { hashTags: true });
		}).then((response) => {
			assert.strictEqual(response.length, 1);
			assert.strictEqual(response[0].processed, true);
			assert.strictEqual(response[0].error, null);
			assert.strictEqual(response[0].server.id, `127.0.0.1:${config.processPorts[1]}`);
			done();
		}).catch((err) => {
			done(err);
		});
	});

	it('skips unclosed hash tags', (done) => {
		let servers = null;
		server.then(() => {
			return mcc.servers(`localhost:${config.configPort}`);
		}).then((serverList) => {
			servers = serverList;
		}).then(() => {
			return mcc.set(servers, [ { key: "{abcdef-dodo", data: new Buffer("AZERTY", "utf-8") } ], { hashTags: true });
		}).then((response) => {
			assert.strictEqual(response.length, 1);
			assert.strictEqual(response[0].processed, true);
			assert.strictEqual(response[0].error, null);
			assert.strictEqual(response[0].server.id, `127.0.0.1:${config.processPorts[1]}`);
			done();
		}).catch((err) => {
			done(err);
		});
	});

	it('skips empty hash tags', (done) => {
		let servers = null;
		server.then(() => {
			return mcc.servers(`localhost:${config.configPort}`);
		}).then((serverList) => {
			servers = serverList;
		}).then(() => {
			return mcc.set(servers, [ { key: "ab{}cdef-dodo", data: new Buffer("AZERTY", "utf-8") } ], { hashTags: true });
		}).then((response) => {
			assert.strictEqual(response.length, 1);
			assert.strictEqual(response[0].processed, true);
			assert.strictEqual(response[0].error, null);
			assert.strictEqual(response[0].server.id, `127.0.0.1:${config.processPorts[1]}`);
			done();
		}).catch((err) => {
			done(err);
		});
	});

	it('can force hash tags on', (done) => {
		let servers = null;
		server.then(() => {
			return mcc.servers(`localhost:${config.configPort}`);
		}).then((serverList) => {
			servers = serverList;
		}).then(() => {
			return mcc.set(servers, [ { key: "{abcdef}-dodo", data: new Buffer("AZERTY", "utf-8") } ], { hashTags: true });
		}).then((response) => {
			assert.strictEqual(response.length, 1);
			assert.strictEqual(response[0].processed, true);
			assert.strictEqual(response[0].error, null);
			assert.strictEqual(response[0].server.id, `127.0.0.1:${config.processPorts[1]}`);
			done();
		}).catch((err) => {
			done(err);
		});
	});

	it('can force hash tags off', (done) => {
		let servers = null;
		server.then(() => {
			return mcc.servers(`localhost:${config.configPort}`);
		}).then((serverList) => {
			servers = serverList;
		}).then(() => {
			return mcc.set(servers, [ { key: "{abcdef}-dodo", data: new Buffer("AZERTY", "utf-8") } ], { hashTags: false });
		}).then((response) => {
			assert.strictEqual(response.length, 1);
			assert.strictEqual(response[0].processed, true);
			assert.strictEqual(response[0].error, null);
			assert.strictEqual(response[0].server.id, `127.0.0.1:${config.processPorts[0]}`);
			done();
		}).catch((err) => {
			done(err);
		});
	});

	it('can set an entry with an algorithm (md5)', (done) => {
		let servers = null;
		server.then(() => {
			return mcc.servers(`localhost:${config.configPort}`);
		}).then((serverList) => {
			servers = serverList;
		}).then(() => {
			return mcc.set(servers, [ { key: "abcdef", data: new Buffer("AZERTY", "utf-8") } ], { hash: 'md5' });
		}).then((response) => {
			assert.strictEqual(response.length, 1);
			assert.strictEqual(response[0].processed, true);
			assert.strictEqual(response[0].error, null);
			assert.strictEqual(response[0].server.id, `127.0.0.1:${config.processPorts[1]}`);
			done();
		}).catch((err) => {
			done(err);
		});
	});

	it('can set an entry with an expiration', (done) => {
		let servers = null;
		server.then(() => {
			return mcc.servers(`localhost:${config.configPort}`);
		}).then((serverList) => {
			servers = serverList;
		}).then(() => {
			return mcc.set(servers, [ { key: "abcdef", data: new Buffer("AZERTY", "utf-8") } ], { expiration: 100 });
		}).then((response) => {
			assert.strictEqual(response.length, 1);
			assert.strictEqual(response[0].processed, true);
			assert.strictEqual(response[0].error, null);
			assert.strictEqual(response[0].server.id, `127.0.0.1:${config.processPorts[1]}`);
			done();
		}).catch((err) => {
			done(err);
		});
	});

	it('can set two entries in the same shard (redis)', (done) => {
		let servers = null;
		server.then(() => {
			return mcc.servers(`localhost:${config.configPort}`);
		}).then((serverList) => {
			servers = serverList;
		}).then(() => {
			return mcc.set(servers, [ { key: "0120", data: new Buffer("AZERTY", "utf-8") }, { key: "0121", data: new Buffer("QWERTY", "utf-8") } ], { hash: 'redis' });
		}).then((response) => {
			assert.strictEqual(response.length, 2);
			assert.strictEqual(response[0].processed, true);
			assert.strictEqual(response[0].error, null);
			assert.strictEqual(response[0].server.id, `127.0.0.1:${config.processPorts[0]}`);
			assert.strictEqual(response[1].processed, true);
			assert.strictEqual(response[1].error, null);
			assert.strictEqual(response[1].server.id, `127.0.0.1:${config.processPorts[0]}`);
			done();
		}).catch((err) => {
			done(err);
		});
	});

	it('can set two entries in different shards (redis)', (done) => {
		let servers = null;
		server.then(() => {
			return mcc.servers(`localhost:${config.configPort}`);
		}).then((serverList) => {
			servers = serverList;
		}).then(() => {
			return mcc.set(servers, [ { key: "0120", data: new Buffer("AZERTY", "utf-8") }, { key: "0122", data: new Buffer("QWERTY", "utf-8") } ], { hash: 'redis' });
		}).then((response) => {
			assert.strictEqual(response.length, 2);
			assert.strictEqual(response[0].processed, true);
			assert.strictEqual(response[0].error, null);
			assert.strictEqual(response[0].server.id, `127.0.0.1:${config.processPorts[0]}`);
			assert.strictEqual(response[1].processed, true);
			assert.strictEqual(response[1].error, null);
			assert.strictEqual(response[1].server.id, `127.0.0.1:${config.processPorts[1]}`);
			done();
		}).catch((err) => {
			done(err);
		});
	});

	it('can set two entries in different shards (redis) in parallel', (done) => {
		let servers = null;
		server.then(() => {
			return mcc.servers(`localhost:${config.configPort}`);
		}).then((serverList) => {
			servers = serverList;
		}).then(() => {
			return mcc.set(servers, [ { key: "0120", data: new Buffer("AZERTY", "utf-8") }, { key: "0122", data: new Buffer("QWERTY", "utf-8") } ], { hash: 'redis', parallel: 2 });
		}).then((response) => {
			assert.strictEqual(response.length, 2);
			assert.strictEqual(response[0].processed, true);
			assert.strictEqual(response[0].error, null);
			assert.strictEqual(response[0].server.id, `127.0.0.1:${config.processPorts[0]}`);
			assert.strictEqual(response[1].processed, true);
			assert.strictEqual(response[1].error, null);
			assert.strictEqual(response[1].server.id, `127.0.0.1:${config.processPorts[1]}`);
			done();
		}).catch((err) => {
			done(err);
		});
	});

	it('can set two entries in the same shard (md5)', (done) => {
		let servers = null;
		server.then(() => {
			return mcc.servers(`localhost:${config.configPort}`);
		}).then((serverList) => {
			servers = serverList;
		}).then(() => {
			return mcc.set(servers, [ { key: "0120", data: new Buffer("AZERTY", "utf-8") }, { key: "0127", data: new Buffer("QWERTY", "utf-8") } ], { hash: 'md5' });
		}).then((response) => {
			assert.strictEqual(response.length, 2);
			assert.strictEqual(response[0].processed, true);
			assert.strictEqual(response[0].error, null);
			assert.strictEqual(response[0].server.id, `127.0.0.1:${config.processPorts[0]}`);
			assert.strictEqual(response[1].processed, true);
			assert.strictEqual(response[1].error, null);
			assert.strictEqual(response[1].server.id, `127.0.0.1:${config.processPorts[0]}`);
			done();
		}).catch((err) => {
			done(err);
		});
	});

	it('can set two entries in different shards (md5)', (done) => {
		let servers = null;
		server.then(() => {
			return mcc.servers(`localhost:${config.configPort}`);
		}).then((serverList) => {
			servers = serverList;
		}).then(() => {
			return mcc.set(servers, [ { key: "0120", data: new Buffer("AZERTY", "utf-8") }, { key: "0122", data: new Buffer("QWERTY", "utf-8") } ], { hash: 'md5' });
		}).then((response) => {
			assert.strictEqual(response.length, 2);
			assert.strictEqual(response[0].processed, true);
			assert.strictEqual(response[0].error, null);
			assert.strictEqual(response[0].server.id, `127.0.0.1:${config.processPorts[0]}`);
			assert.strictEqual(response[1].processed, true);
			assert.strictEqual(response[1].error, null);
			assert.strictEqual(response[1].server.id, `127.0.0.1:${config.processPorts[1]}`);
			done();
		}).catch((err) => {
			done(err);
		});
	});

	it('fails for invalid keys', (done) => {
		let servers = null;
		server.then(() => {
			return mcc.servers(`localhost:${config.configPort}`);
		}).then((serverList) => {
			servers = serverList;
		}).then(() => {
			return mcc.set(servers, [ { key: "abc def", data: new Buffer("AZERTY", "utf-8") } ]);
		}).then(() => {
			done(new Error("Expecting error"));
		}).catch((err) => {
			assert(err instanceof Error);
			assert.strictEqual(err.message,"Errors occured when processing elements");
			assert.strictEqual(err.responses[0].error.message, `Unexpected response when publishing record abc def to server 127.0.0.1:${config.processPorts[1]}: [{"code":"CLIENT_ERROR","message":"received invalid command arguments"}]`);
			done();
		}).catch((err) => {
			done(err);
		});
	});

	it('fails for invalid hash algorithm', (done) => {
		let servers = null;
		server.then(() => {
			return mcc.servers(`localhost:${config.configPort}`);
		}).then((serverList) => {
			servers = serverList;
		}).then(() => {
			return mcc.set(servers, [ { key: "abcdef", data: new Buffer("AZERTY", "utf-8") } ], { hash: 'invalid' });
		}).then(() => {
			done(new Error("Expecting error"));
		}).catch((err) => {
			assert(err instanceof Error);
			assert.strictEqual(err.message,"Unable to identify requested hashing algorithm: invalid");
			done();
		}).catch((err) => {
			done(err);
		});
	});

	it('fails if no server provided', (done) => {
		server.then(() => {
			return mcc.set([], [ { key: "abcdef", data: new Buffer("AZERTY", "utf-8") } ]);
		}).then(() => {
			done(new Error("Expecting error"));
		}).catch((err) => {
			assert(err instanceof Error);
			assert.strictEqual(err.message,"You must specify a non empty list of memcached servers");
			done();
		}).catch((err) => {
			done(err);
		});
	});

	it('fails if no record provided', (done) => {
		let servers = null;
		server.then(() => {
			return mcc.servers(`localhost:${config.configPort}`);
		}).then((serverList) => {
			servers = serverList;
		}).then(() => {
			return mcc.set(servers, []);
		}).then(() => {
			done(new Error("Expecting error"));
		}).catch((err) => {
			assert(err instanceof Error);
			assert.strictEqual(err.message,"You must specify a non empty list of records to store in memcached");
			done();
		}).catch((err) => {
			done(err);
		});
	});

	it('fails if record does not have a key', (done) => {
		let servers = null;
		server.then(() => {
			return mcc.servers(`localhost:${config.configPort}`);
		}).then((serverList) => {
			servers = serverList;
		}).then(() => {
			return mcc.set(servers, [ { data: new Buffer("AZERTY", "utf-8") } ]);
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
		let servers = null;
		server.then(() => {
			return mcc.servers(`localhost:${config.configPort}`);
		}).then((serverList) => {
			servers = serverList;
		}).then(() => {
			return mcc.set(servers, [ { key: 10, data: new Buffer("AZERTY", "utf-8") } ]);
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
		let servers = null;
		server.then(() => {
			return mcc.servers(`localhost:${config.configPort}`);
		}).then((serverList) => {
			servers = serverList;
		}).then(() => {
			return mcc.set(servers, [ { key: "abcdef" } ]);
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
		let servers = null;
		server.then(() => {
			return mcc.servers(`localhost:${config.configPort}`);
		}).then((serverList) => {
			servers = serverList;
		}).then(() => {
			return mcc.set(servers, [ { key: "abcdef", data: 10 } ]);
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
