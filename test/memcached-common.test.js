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
const mc = require('../lib/memcached-common.js');

const assert = require('assert');
let config = null;
const server = mch({ shards: 2 }).then((c) => config = c);

describe('memcached-common', () => {
	it('client throws on invalid response code', (done) => {
		let client = null;
		server.then(() => {
			return mc.connect('localhost', config.processPorts[0]);
		}).then((c) => {
			client = c;
		}).then(() => {
			return mc.sendRequestAndReadResponse(client, `echo 6\r\nPONG\r\n\r\n`);
		}).then(() => {
			client.end();
			done(new Error("Should have raised an error"));
		}).catch((err) => {
			client.end();
			assert(err instanceof Error);
			assert.strictEqual(err.message, "Unknown response 'PONG'");
			done();
		}).catch((err) => {
			done(err);
		});
	});

	it('client throws on invalid response format (too short)', (done) => {
		let client = null;
		server.then(() => {
			return mc.connect('localhost', config.processPorts[0]);
		}).then((c) => {
			client = c;
		}).then(() => {
			return mc.sendRequestAndReadResponse(client, `echo 11\r\nVALUE 2 3\r\n\r\n`);
		}).then(() => {
			client.end();
			done(new Error("Should have raised an error"));
		}).catch((err) => {
			client.end();
			assert(err instanceof Error);
			assert.strictEqual(err.message, "Invalid response format received from server for code VALUE");
			done();
		}).catch((err) => {
			done(err);
		});
	});

	it('client throws on invalid response format (too long)', (done) => {
		let client = null;
		server.then(() => {
			return mc.connect('localhost', config.processPorts[0]);
		}).then((c) => {
			client = c;
		}).then(() => {
			return mc.sendRequestAndReadResponse(client, `echo 17\r\nVALUE 2 3 4 5 6\r\n\r\n`);
		}).then(() => {
			client.end();
			done(new Error("Should have raised an error"));
		}).catch((err) => {
			client.end();
			assert(err instanceof Error);
			assert.strictEqual(err.message, "Invalid response format received from server for code VALUE");
			done();
		}).catch((err) => {
			done(err);
		});
	});

	it('client throws on invalid response data (via ping)', (done) => {
		let client = null;
		server.then(() => {
			return mc.connect('localhost', config.processPorts[0]);
		}).then((c) => {
			client = c;
		}).then(() => {
			return mc.sendRequestAndReadResponse(client, `echo 32\r\nVALUE abcdef 0 10\r\n0123456789A\r\n\r\n`);
		}).then(() => {
			client.end();
			done(new Error("Should have raised an error"));
		}).catch((err) => {
			client.end();
			assert(err instanceof Error);
			assert.strictEqual(err.message, "Invalid response data received from server for code VALUE");
			done();
		}).catch((err) => {
			done(err);
		});
	});

	it('client accepts all end codes codes', (done) => {
		let client = null;
		server.then(() => {
			return mc.connect('localhost', config.processPorts[0]);
		}).then((c) => {
			client = c;
		}).then(() => {
			return mc.sendRequestAndReadResponse(client, `echo 7\r\nERROR\r\n\r\n`);
		}).then((response) => {
			assert.strictEqual(response.length, 1);
			assert.strictEqual(response[0].code, "ERROR");
			return response;
		}).then(() => {
			return mc.sendRequestAndReadResponse(client, `echo 8\r\nSTORED\r\n\r\n`);
		}).then((response) => {
			assert.strictEqual(response.length, 1);
			assert.strictEqual(response[0].code, "STORED");
			return response;
		}).then(() => {
			return mc.sendRequestAndReadResponse(client, `echo 12\r\nNOT_STORED\r\n\r\n`);
		}).then((response) => {
			assert.strictEqual(response.length, 1);
			assert.strictEqual(response[0].code, "NOT_STORED");
			return response;
		}).then(() => {
			return mc.sendRequestAndReadResponse(client, `echo 8\r\nEXISTS\r\n\r\n`);
		}).then((response) => {
			assert.strictEqual(response.length, 1);
			assert.strictEqual(response[0].code, "EXISTS");
			return response;
		}).then(() => {
			return mc.sendRequestAndReadResponse(client, `echo 11\r\nNOT_FOUND\r\n\r\n`);
		}).then((response) => {
			assert.strictEqual(response.length, 1);
			assert.strictEqual(response[0].code, "NOT_FOUND");
			return response;
		}).then(() => {
			return mc.sendRequestAndReadResponse(client, `echo 5\r\nEND\r\n\r\n`);
		}).then((response) => {
			assert.strictEqual(response.length, 1);
			assert.strictEqual(response[0].code, "END");
			return response;
		}).then(() => {
			return mc.sendRequestAndReadResponse(client, `echo 21\r\nCLIENT_ERROR abcdef\r\n\r\n`);
		}).then((response) => {
			assert.strictEqual(response.length, 1);
			assert.strictEqual(response[0].code, "CLIENT_ERROR");
			return response;
		}).then(() => {
			return mc.sendRequestAndReadResponse(client, `echo 21\r\nSERVER_ERROR abcdef\r\n\r\n`);
		}).then((response) => {
			assert.strictEqual(response.length, 1);
			assert.strictEqual(response[0].code, "SERVER_ERROR");
			return response;
		}).then(() => {
			client.end();
			done();
		}).catch((err) => {
			client.end();
			done(err);
		});
	});

	it('client accepts chunked results', (done) => {
		let client = null;
		server.then(() => {
			return mc.connect('localhost', config.processPorts[0]);
		}).then((c) => {
			client = c;
		}).then(() => {
			setTimeout(() => { client.write("echo 11\r\n6789\r\nEND\r\n\r\n"); }, 5);
			return mc.sendRequestAndReadResponse(client, `echo 25\r\nVALUE abcdef 0 10\r\n012345\r\n`);
		}).then((response) => {
			assert.strictEqual(response.length, 2);
			assert.strictEqual(response[0].code, "VALUE");
			assert.strictEqual(response[0].key, "abcdef");
			assert.strictEqual(response[0].value.toString(), '0123456789');
			assert.strictEqual(response[1].code, "END");
			return response;
		}).then(() => {
			client.end();
			done();
		}).catch((err) => {
			client.end();
			done(err);
		});
	});

	it('client does not accept encoding change', (done) => {
		let client = null;
		server.then(() => {
			return mc.connect('localhost', config.processPorts[0]);
		}).then((c) => {
			client = c;
			client.setEncoding('utf-8');
		}).then(() => {
			client.end();
			done(Error("An error should have been raised"));
		}).catch((err) => {
			client.end();
			assert(err instanceof Error);
			assert.strictEqual(err.message, "You can not change the encoding for a memcached client");
			done();
		}).catch((err) => {
			done(err);
		});
	});
});
