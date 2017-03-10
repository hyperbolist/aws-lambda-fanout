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

const mch    = require('../test-hosts/memcached-host.js');
const mc     = require('../lib/memcached-common.js');
const uuid   = require('uuid');
const assert = require('assert');
const sinon  = require('sinon');

let config = null;
const server = mch({ shards: 2 }).then((c) => config = c);

describe('memcached-host', () => {
	it('should provide the server list', (done) => {
		let client = null;
		server.then(() => {
			return mc.connect('localhost', config.configPort);
		}).then((c) => {
			client = c;
		}).then(() => {
			return mc.sendRequestAndReadResponse(client, "config get cluster\r\n");
		}).then((response) => {
			assert.strictEqual(response.length, 2);
			assert.strictEqual(response[0].code, "CONFIG");
			const configData = response[0].value.toString('utf-8').split("\n");
			assert.strictEqual(configData.length, 3);
			const servers = configData[1].split(' ');
			assert.strictEqual(servers.length, 2);
			servers.forEach((server) => {
				const serverParts = server.split('|');
				assert.strictEqual(serverParts.length, 3);
			});
		}).then(() => {
			client.end();
			done();
		}).catch((err) => {
			client.end();
			done(err);
		});
	});

	it('should provide the server list', (done) => {
		let client = null;
		server.then(() => {
			return mc.connect('localhost', config.configPort);
		}).then((c) => {
			client = c;
		}).then(() => {
			return mc.sendRequestAndReadResponse(client, "config get cluster\r\nconfig get cluster");
		}).then(() => {
			return mc.sendRequestAndReadResponse(client, "\r\n");
		}).then((response) => {
			assert.strictEqual(response.length, 2);
			assert.strictEqual(response[0].code, "CONFIG");
			const configData = response[0].value.toString('utf-8').split("\n");
			assert.strictEqual(configData.length, 3);
			const servers = configData[1].split(' ');
			assert.strictEqual(servers.length, 2);
			servers.forEach((server) => {
				const serverParts = server.split('|');
				assert.strictEqual(serverParts.length, 3);
			});
		}).then(() => {
			client.end();
			done();
		}).catch((err) => {
			client.end();
			done(err);
		});
	});

	it('set on non existing key', (done) => {
		let client = null;
		const key = uuid();
		server.then(() => {
			return mc.connect('localhost', config.processPorts[0]);
		}).then((c) => {
			client = c;
		}).then(() => {
			return mc.sendRequestAndReadResponse(client, `set ${key} 10 10 6\r\nAZERTY\r\n`);
		}).then((response) => {
			assert.strictEqual(response.length, 1);
			assert.strictEqual(response[0].code, "STORED");
		}).then(() => {
			client.end();
			done();
		}).catch((err) => {
			client.end();
			done(err);
		});
	});

	it('set on existing key', (done) => {
		let client = null;
		const key = uuid();
		server.then(() => {
			return mc.connect('localhost', config.processPorts[0]);
		}).then((c) => {
			client = c;
		}).then(() => {
			return mc.sendRequestAndReadResponse(client, `set ${key} 10 10 6\r\nAZERTY\r\n`);
		}).then(() => {
			return mc.sendRequestAndReadResponse(client, `set ${key} 10 10 6\r\nQWERTY\r\n`);
		}).then((response) => {
			assert.strictEqual(response.length, 1);
			assert.strictEqual(response[0].code, "STORED");
		}).then(() => {
			client.end();
			done();
		}).catch((err) => {
			client.end();
			done(err);
		});
	});

	it('set on large value', (done) => {
		let client = null;
		const key = uuid();
		server.then(() => {
			return mc.connect('localhost', config.processPorts[0]);
		}).then((c) => {
			client = c;
		}).then(() => {
			return mc.sendRequestAndReadResponse(client, `get ${key}\r\nset ${key} 10 10 512\r\n0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF`);
		}).then(() => {
			return mc.sendRequestAndReadResponse(client, `0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF\r\n`);
		}).then((response) => {
			assert.strictEqual(response.length, 1);
			assert.strictEqual(response[0].code, "STORED");
		}).then(() => {
			client.end();
			done();
		}).catch((err) => {
			client.end();
			done(err);
		});
	});

	it('add on non existing key', (done) => {
		let client = null;
		const key = uuid();
		server.then(() => {
			return mc.connect('localhost', config.processPorts[0]);
		}).then((c) => {
			client = c;
		}).then(() => {
			return mc.sendRequestAndReadResponse(client, `add ${key} 10 10 6\r\nAZERTY\r\n`);
		}).then((response) => {
			assert.strictEqual(response.length, 1);
			assert.strictEqual(response[0].code, "STORED");
		}).then(() => {
			client.end();
			done();
		}).catch((err) => {
			client.end();
			done(err);
		});
	});

	it('add on existing key', (done) => {
		let client = null;
		const key = uuid();
		server.then(() => {
			return mc.connect('localhost', config.processPorts[0]);
		}).then((c) => {
			client = c;
		}).then(() => {
			return mc.sendRequestAndReadResponse(client, `set ${key} 10 10 6\r\nAZERTY\r\n`);
		}).then(() => {
			return mc.sendRequestAndReadResponse(client, `add ${key} 10 10 6\r\nQWERTY\r\n`);
		}).then((response) => {
			assert.strictEqual(response.length, 1);
			assert.strictEqual(response[0].code, "NOT_STORED");
		}).then(() => {
			client.end();
			done();
		}).catch((err) => {
			client.end();
			done(err);
		});
	});

	it('replace on non existing key', (done) => {
		let client = null;
		const key = uuid();
		server.then(() => {
			return mc.connect('localhost', config.processPorts[0]);
		}).then((c) => {
			client = c;
		}).then(() => {
			return mc.sendRequestAndReadResponse(client, `replace ${key} 10 10 6\r\nAZERTY\r\n`);
		}).then((response) => {
			assert.strictEqual(response.length, 1);
			assert.strictEqual(response[0].code, "NOT_STORED");
		}).then(() => {
			client.end();
			done();
		}).catch((err) => {
			client.end();
			done(err);
		});
	});

	it('replace on existing key', (done) => {
		let client = null;
		const key = uuid();
		server.then(() => {
			return mc.connect('localhost', config.processPorts[0]);
		}).then((c) => {
			client = c;
		}).then(() => {
			return mc.sendRequestAndReadResponse(client, `set ${key} 10 10 6\r\nAZERTY\r\n`);
		}).then(() => {
			return mc.sendRequestAndReadResponse(client, `replace ${key} 10 10 6\r\nQWERTY\r\n`);
		}).then((response) => {
			assert.strictEqual(response.length, 1);
			assert.strictEqual(response[0].code, "STORED");
		}).then(() => {
			client.end();
			done();
		}).catch((err) => {
			client.end();
			done(err);
		});
	});

	it('get on non existing key', (done) => {
		let client = null;
		const key = uuid();
		server.then(() => {
			return mc.connect('localhost', config.processPorts[0]);
		}).then((c) => {
			client = c;
		}).then(() => {
			return mc.sendRequestAndReadResponse(client, `get ${key}\r\n`);
		}).then((response) => {
			assert.strictEqual(response.length, 1);
			assert.strictEqual(response[0].code, "END");
		}).then(() => {
			client.end();
			done();
		}).catch((err) => {
			client.end();
			done(err);
		});
	});

	it('get on non-expired key', (done) => {
		let client = null;
		const key = uuid();
		server.then(() => {
			return mc.connect('localhost', config.processPorts[0]);
		}).then((c) => {
			client = c;
		}).then(() => {
			return mc.sendRequestAndReadResponse(client, `set ${key} 10 100 6\r\nAZERTY\r\n`);
		}).then(() => {
			return mc.sendRequestAndReadResponse(client, `get ${key}\r\n`);
		}).then((response) => {
			assert.strictEqual(response.length, 2);
			assert.strictEqual(response[0].code, "VALUE");
			assert.strictEqual(response[0].value.toString(), "AZERTY");
			assert.strictEqual(response[0].flags, 10);
			assert(response[0].cas > 0);
			assert.strictEqual(response[1].code, "END");
		}).then(() => {
			client.end();
			done();
		}).catch((err) => {
			client.end();
			done(err);
		});
	});

	it('get on non-expired key (max delta)', (done) => {
		let client = null;
		const key = uuid();
		server.then(() => {
			return mc.connect('localhost', config.processPorts[0]);
		}).then((c) => {
			client = c;
		}).then(() => {
			return mc.sendRequestAndReadResponse(client, `set ${key} 10 2592000 6\r\nAZERTY\r\n`);
		}).then(() => {
			return mc.sendRequestAndReadResponse(client, `get ${key}\r\n`);
		}).then((response) => {
			assert.strictEqual(response.length, 2);
			assert.strictEqual(response[0].code, "VALUE");
			assert.strictEqual(response[0].value.toString(), "AZERTY");
			assert.strictEqual(response[0].flags, 10);
			assert(response[0].cas > 0);
			assert.strictEqual(response[1].code, "END");
		}).then(() => {
			client.end();
			done();
		}).catch((err) => {
			client.end();
			done(err);
		});
	});

	it('get on non-expired key (no expiration)', (done) => {
		let client = null;
		const key = uuid();
		server.then(() => {
			return mc.connect('localhost', config.processPorts[0]);
		}).then((c) => {
			client = c;
		}).then(() => {
			return mc.sendRequestAndReadResponse(client, `set ${key} 10 0 6\r\nAZERTY\r\n`);
		}).then(() => {
			return mc.sendRequestAndReadResponse(client, `get ${key}\r\n`);
		}).then((response) => {
			assert.strictEqual(response.length, 2);
			assert.strictEqual(response[0].code, "VALUE");
			assert.strictEqual(response[0].value.toString(), "AZERTY");
			assert.strictEqual(response[0].flags, 10);
			assert(response[0].cas > 0);
			assert.strictEqual(response[1].code, "END");
		}).then(() => {
			client.end();
			done();
		}).catch((err) => {
			client.end();
			done(err);
		});
	});

	it('get on expired key (negative)', (done) => {
		let client = null;
		const key = uuid();
		server.then(() => {
			return mc.connect('localhost', config.processPorts[0]);
		}).then((c) => {
			client = c;
		}).then(() => {
			return mc.sendRequestAndReadResponse(client, `set ${key} 10 -2 6\r\nAZERTY\r\n`);
		}).then(() => {
			return mc.sendRequestAndReadResponse(client, `get ${key}\r\n`);
		}).then((response) => {
			assert.strictEqual(response.length, 1);
			assert.strictEqual(response[0].code, "END");
		}).then(() => {
			client.end();
			done();
		}).catch((err) => {
			client.end();
			done(err);
		});
	});

	it('get on expired key (30 days + 1 second)', (done) => {
		let client = null;
		const key = uuid();
		server.then(() => {
			return mc.connect('localhost', config.processPorts[0]);
		}).then((c) => {
			client = c;
		}).then(() => {
			return mc.sendRequestAndReadResponse(client, `set ${key} 10 2592001 6\r\nAZERTY\r\n`);
		}).then(() => {
			return mc.sendRequestAndReadResponse(client, `get ${key}\r\n`);
		}).then((response) => {
			assert.strictEqual(response.length, 1);
			assert.strictEqual(response[0].code, "END");
		}).then(() => {
			client.end();
			done();
		}).catch((err) => {
			client.end();
			done(err);
		});
	});

	it('get on expired key (now - 1 day)', (done) => {
		let when = Math.floor(Date.now() / 1000) - (24*60*60);
		let client = null;
		const key = uuid();
		server.then(() => {
			return mc.connect('localhost', config.processPorts[0]);
		}).then((c) => {
			client = c;
		}).then(() => {
			return mc.sendRequestAndReadResponse(client, `set ${key} 10 ${when} 6\r\nAZERTY\r\n`);
		}).then(() => {
			return mc.sendRequestAndReadResponse(client, `get ${key}\r\n`);
		}).then((response) => {
			assert.strictEqual(response.length, 1);
			assert.strictEqual(response[0].code, "END");
		}).then(() => {
			client.end();
			done();
		}).catch((err) => {
			client.end();
			done(err);
		});
	});

	it('get on expired key (1 sec)', (done) => {
		let client = null;
		let clock = sinon.useFakeTimers();
		const key = uuid();
		server.then(() => {
			return mc.connect('localhost', config.processPorts[0]);
		}).then((c) => {
			client = c;
		}).then(() => {
			return mc.sendRequestAndReadResponse(client, `set ${key} 10 1 6\r\nAZERTY\r\n`);
		}).then(() => {
			clock.tick(1010);
		}).then(() => {
			return mc.sendRequestAndReadResponse(client, `get ${key}\r\n`);
		}).then((response) => {
			assert.strictEqual(response.length, 1);
			assert.strictEqual(response[0].code, "END");
		}).then(() => {
			clock.restore();
			client.end();
			done();
		}).catch((err) => {
			clock.restore();
			client.end();
			done(err);
		});
	});

	it('get on expired key (now + 1 sec)', (done) => {
		let clock = sinon.useFakeTimers();
		let when = Math.floor(Date.now() / 1000) + 1;
		let client = null;
		const key = uuid();
		server.then(() => {
			return mc.connect('localhost', config.processPorts[0]);
		}).then((c) => {
			client = c;
		}).then(() => {
			return mc.sendRequestAndReadResponse(client, `set ${key} 10 ${when} 6\r\nAZERTY\r\n`);
		}).then(() => {
			clock.tick(1010);
		}).then(() => {
			return mc.sendRequestAndReadResponse(client, `get ${key}\r\n`);
		}).then((response) => {
			assert.strictEqual(response.length, 1);
			assert.strictEqual(response[0].code, "END");
		}).then(() => {
			clock.restore();
			client.end();
			done();
		}).catch((err) => {
			clock.restore();
			client.end();
			done(err);
		});
	});

	it('cas immutable on multiple get', (done) => {
		let client = null;
		let cas = null;
		const key = uuid();
		server.then(() => {
			return mc.connect('localhost', config.processPorts[0]);
		}).then((c) => {
			client = c;
		}).then(() => {
			return mc.sendRequestAndReadResponse(client, `set ${key} 10 0 6\r\nAZERTY\r\n`);
		}).then(() => {
			return mc.sendRequestAndReadResponse(client, `get ${key}\r\n`);
		}).then((response) => {
			cas = response[0].cas;
		}).then(() => {
			return mc.sendRequestAndReadResponse(client, `get ${key}\r\n`);
		}).then((response) => {
			assert.strictEqual(cas, response[0].cas);
		}).then(() => {
			client.end();
			done();
		}).catch((err) => {
			client.end();
			done(err);
		});
	});

	it('cas increases on multiple set on the same key', (done) => {
		let client = null;
		let cas = null;
		const key = uuid();
		server.then(() => {
			return mc.connect('localhost', config.processPorts[0]);
		}).then((c) => {
			client = c;
		}).then(() => {
			return mc.sendRequestAndReadResponse(client, `set ${key} 10 0 6\r\nAZERTY\r\n`);
		}).then(() => {
			return mc.sendRequestAndReadResponse(client, `get ${key}\r\n`);
		}).then((response) => {
			cas = response[0].cas;
		}).then(() => {
			return mc.sendRequestAndReadResponse(client, `set ${key} 10 0 6\r\nQWERTY\r\n`);
		}).then(() => {
			return mc.sendRequestAndReadResponse(client, `get ${key}\r\n`);
		}).then((response) => {
			assert.ok(cas < response[0].cas);
		}).then(() => {
			client.end();
			done();
		}).catch((err) => {
			client.end();
			done(err);
		});
	});

	it('cas increases on multiple set on different keys', (done) => {
		let client = null;
		let cas = null;
		const key1 = uuid();
		const key2 = uuid();
		server.then(() => {
			return mc.connect('localhost', config.processPorts[0]);
		}).then((c) => {
			client = c;
		}).then(() => {
			return mc.sendRequestAndReadResponse(client, `set ${key1} 10 0 6\r\nAZERTY\r\n`);
		}).then(() => {
			return mc.sendRequestAndReadResponse(client, `get ${key1}\r\n`);
		}).then((response) => {
			cas = response[0].cas;
		}).then(() => {
			return mc.sendRequestAndReadResponse(client, `set ${key2} 10 0 6\r\nQWERTY\r\n`);
		}).then(() => {
			return mc.sendRequestAndReadResponse(client, `get ${key2}\r\n`);
		}).then((response) => {
			assert.ok(cas < response[0].cas);
		}).then(() => {
			client.end();
			done();
		}).catch((err) => {
			client.end();
			done(err);
		});
	});

	it('cas increases on multiple set on different keys on different shards', (done) => {
		let client = null;
		let cas = null;
		const key1 = uuid();
		const key2 = uuid();
		server.then(() => {
			return mc.connect('localhost', config.processPorts[0]);
		}).then((c) => {
			client = c;
		}).then(() => {
			return mc.sendRequestAndReadResponse(client, `set ${key1} 10 10 6\r\nAZERTY\r\n`);
		}).then(() => {
			return mc.sendRequestAndReadResponse(client, `get ${key1}\r\n`);
		}).then((response) => {
			cas = response[0].cas;
		}).then(() => {
			client.end();
		}).then(() => {
			return mc.connect('localhost', config.processPorts[1]);
		}).then((c) => {
			client = c;
		}).then(() => {
			return mc.sendRequestAndReadResponse(client, `set ${key2} 10 10 6\r\nQWERTY\r\n`);
		}).then(() => {
			return mc.sendRequestAndReadResponse(client, `get ${key2}\r\n`);
		}).then((response) => {
			assert.ok(cas < response[0].cas);
		}).then(() => {
			client.end();
			done();
		}).catch((err) => {
			client.end();
			done(err);
		});
	});

	it('cas increases on multiple set on same keys on different shards', (done) => {
		let client = null;
		let cas = null;
		const key = uuid();
		server.then(() => {
			return mc.connect('localhost', config.processPorts[0]);
		}).then((c) => {
			client = c;
		}).then(() => {
			return mc.sendRequestAndReadResponse(client, `set ${key} 10 10 6\r\nAZERTY\r\n`);
		}).then(() => {
			return mc.sendRequestAndReadResponse(client, `get ${key}\r\n`);
		}).then((response) => {
			cas = response[0].cas;
		}).then(() => {
			client.end();
		}).then(() => {
			return mc.connect('localhost', config.processPorts[1]);
		}).then((c) => {
			client = c;
		}).then(() => {
			return mc.sendRequestAndReadResponse(client, `set ${key} 10 10 6\r\nQWERTY\r\n`);
		}).then(() => {
			return mc.sendRequestAndReadResponse(client, `get ${key}\r\n`);
		}).then((response) => {
			assert.ok(cas < response[0].cas);
		}).then(() => {
			client.end();
			done();
		}).catch((err) => {
			client.end();
			done(err);
		});
	});

	it('data on one shard should not be available on the other shard', (done) => {
		let client = null;
		const key = uuid();
		server.then(() => {
			return mc.connect('localhost', config.processPorts[0]);
		}).then((c) => {
			client = c;
		}).then(() => {
			return mc.sendRequestAndReadResponse(client, `set ${key} 10 10 6\r\nAZERTY\r\n`);
		}).then(() => {
			client.end();
		}).then(() => {
			return mc.connect('localhost', config.processPorts[1]);
		}).then((c) => {
			client = c;
		}).then(() => {
			return mc.sendRequestAndReadResponse(client, `get ${key}\r\n`);
		}).then((response) => {
			assert.strictEqual(response.length, 1);
			assert.strictEqual(response[0].code, "END");
		}).then(() => {
			client.end();
			done();
		}).catch((err) => {
			client.end();
			done(err);
		});
	});

	it('same key can be stored with different values in different shards', (done) => {
		let client = null;
		const key = uuid();
		server.then(() => {
			return mc.connect('localhost', config.processPorts[0]);
		}).then((c) => {
			client = c;
		}).then(() => {
			return mc.sendRequestAndReadResponse(client, `set ${key} 10 10 6\r\nAZERTY\r\n`);
		}).then(() => {
			client.end();
		}).then(() => {
			return mc.connect('localhost', config.processPorts[1]);
		}).then((c) => {
			client = c;
		}).then(() => {
			return mc.sendRequestAndReadResponse(client, `set ${key} 10 10 6\r\nQWERTY\r\n`);
		}).then(() => {
			return mc.connect('localhost', config.processPorts[0]);
		}).then((c) => {
			client = c;
		}).then(() => {
			return mc.sendRequestAndReadResponse(client, `get ${key}\r\n`);
		}).then((response) => {
			assert.strictEqual(response.length, 2);
			assert.strictEqual(response[0].code, "VALUE");
			assert.strictEqual(response[0].value.toString('utf-8'), "AZERTY");
		}).then(() => {
			client.end();
		}).then(() => {
			return mc.connect('localhost', config.processPorts[1]);
		}).then((c) => {
			client = c;
		}).then(() => {
			return mc.sendRequestAndReadResponse(client, `get ${key}\r\n`);
		}).then((response) => {
			assert.strictEqual(response.length, 2);
			assert.strictEqual(response[0].code, "VALUE");
			assert.strictEqual(response[0].value.toString('utf-8'), "QWERTY");
		}).then(() => {
			client.end();
		}).then(() => {
			client.end();
			done();
		}).catch((err) => {
			client.end();
			done(err);
		});
	});

	it('should work without options', (done) => {
		let client = null;
		mch().then((c) => {
			client = c;
			assert.strictEqual(client.processPorts.length, 1);
		}).then(() => {
			client.stop();
			done();
		}).catch((err) => {
			client.stop();
			done(err);
		});
	});

	it('should process configuration port parameter', (done) => {
		let ss = null;
		mch({ configPort: 8989 }).then((s) => {
			ss = s;
			assert.strictEqual(ss.configPort, 8989);
		}).then(() => {
			ss.stop();
			done();
		}).catch((err) => {
			ss.stop();
			done(err);
		});
	});

	it('should process shards parameter', (done) => {
		let ss = null;
		mch({ shards: 5 }).then((s) => {
			ss = s;
			assert.strictEqual(ss.processPorts.length, 5);
		}).then(() => {
			ss.stop();
			done();
		}).catch((err) => {
			ss.stop();
			done(err);
		});
	});

	it('should handle exit', (done) => {
		let client = null;
		let ss = null;
		mch({ shards: 5 }).then((s) => {
			ss = s;
			ss.on('stopped', () => {
				done();
			});
		}).then(() => {
			return mc.connect('localhost', ss.configPort);
		}).then((c) => {
			client = c;
		}).then(() => {
			return mc.sendRequestAndReadResponse(client, `exit\r\n`);
		}).catch((err) => {
			done(err);
			ss.stop();
		});
	});

	it('should handle quit', (done) => {
		let client = null;
		let ss = null;
		mch({ shards: 5 }).then((s) => {
			ss = s;
			ss.on('stopped', () => {
				done();
			});
		}).then(() => {
			return mc.connect('localhost', ss.configPort);
		}).then((c) => {
			client = c;
		}).then(() => {
			return mc.sendRequestAndReadResponse(client, `quit\r\n`);
		}).catch((err) => {
			done(err);
			ss.stop();
		});
	});

	it('invalid get', (done) => {
		const key = uuid();
		let client = null;
		server.then(() => {
			return mc.connect('localhost', config.processPorts[0]);
		}).then((c) => {
			client = c;
		}).then(() => {
			return mc.sendRequestAndReadResponse(client, `get\r\n`);
		}).then((response) => {
			assert.strictEqual(response.length, 1);
			assert.strictEqual(response[0].code, "CLIENT_ERROR");
			assert.strictEqual(response[0].message, "received badly formatted command");
		}).then(() => {
			return mc.sendRequestAndReadResponse(client, `get \r\n`);
		}).then((response) => {
			assert.strictEqual(response.length, 1);
			assert.strictEqual(response[0].code, "CLIENT_ERROR");
			assert.strictEqual(response[0].message, "received invalid command arguments");
		}).then(() => {
			return mc.sendRequestAndReadResponse(client, `get 0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789X\r\n`);
		}).then((response) => {
			assert.strictEqual(response.length, 1);
			assert.strictEqual(response[0].code, "CLIENT_ERROR");
			assert.strictEqual(response[0].message, "received invalid command arguments");
		}).then(() => {
			return mc.sendRequestAndReadResponse(client, `get ${key} \r\n`);
		}).then((response) => {
			assert.strictEqual(response.length, 1);
			assert.strictEqual(response[0].code, "CLIENT_ERROR");
			assert.strictEqual(response[0].message, "received invalid command arguments");
		}).then(() => {
			return mc.sendRequestAndReadResponse(client, `set ${key} 10 10 6\r\nAZERTY\r\n`);
		}).then(() => {
			return mc.sendRequestAndReadResponse(client, `get ${key} \r\n`);
		}).then((response) => {
			assert.strictEqual(response.length, 1);
			assert.strictEqual(response[0].code, "CLIENT_ERROR");
			assert.strictEqual(response[0].message, "received invalid command arguments");
		}).then(() => {
			client.end();
			done();
		}).catch((err) => {
			client.end();
			done(err);
		});
	});

	it('process does not support exit', (done) => {
		let client = null;
		server.then(() => {
			return mc.connect('localhost', config.processPorts[0]);
		}).then((c) => {
			client = c;
		}).then(() => {
			return mc.sendRequestAndReadResponse(client, `exit\r\n`);
		}).then((response) => {
			assert.strictEqual(response.length, 1);
			assert.strictEqual(response[0].code, "CLIENT_ERROR");
			assert.strictEqual(response[0].message, "received unsupported command");
		}).then(() => {
			client.end();
			done();
		}).catch((err) => {
			client.end();
			done(err);
		});
	});

	it('config does not support get', (done) => {
		const key = uuid();
		let client = null;
		server.then(() => {
			return mc.connect('localhost', config.configPort);
		}).then((c) => {
			client = c;
		}).then(() => {
			return mc.sendRequestAndReadResponse(client, `get ${key}\r\n`);
		}).then((response) => {
			assert.strictEqual(response.length, 1);
			assert.strictEqual(response[0].code, "CLIENT_ERROR");
			assert.strictEqual(response[0].message, "received unsupported command");
		}).then(() => {
			client.end();
			done();
		}).catch((err) => {
			client.end();
			done(err);
		});
	});

	it('invalid config command', (done) => {
		let client = null;
		server.then(() => {
			return mc.connect('localhost', config.configPort);
		}).then((c) => {
			client = c;
		}).then(() => {
			return mc.sendRequestAndReadResponse(client, `config\r\n`);
		}).then((response) => {
			assert.strictEqual(response.length, 1);
			assert.strictEqual(response[0].code, "CLIENT_ERROR");
			assert.strictEqual(response[0].message, "received badly formatted command");
		}).then(() => {
			return mc.sendRequestAndReadResponse(client, `config set test\r\n`);
		}).then((response) => {
			assert.strictEqual(response.length, 1);
			assert.strictEqual(response[0].code, "CLIENT_ERROR");
			assert.strictEqual(response[0].message, "received unsupported command");
		}).then(() => {
			client.end();
			done();
		}).catch((err) => {
			client.end();
			done(err);
		});
	});

	it('invalid set', (done) => {
		let client = null;
		const key = uuid();
		server.then(() => {
			return mc.connect('localhost', config.processPorts[0]);
		}).then((c) => {
			client = c;
		}).then(() => {
			return mc.sendRequestAndReadResponse(client, `set\r\n`);
		}).then((response) => {
			assert.strictEqual(response.length, 1);
			assert.strictEqual(response[0].code, "CLIENT_ERROR");
			assert.strictEqual(response[0].message, "received badly formatted command");
		}).then(() => {
			return mc.sendRequestAndReadResponse(client, `set ${key} 10 10 0\r\nA\r\n`);
		}).then((response) => {
			assert.strictEqual(response.length, 1);
			assert.strictEqual(response[0].code, "CLIENT_ERROR");
			assert.strictEqual(response[0].message, "received invalid command data");
		}).then(() => {
			return mc.sendRequestAndReadResponse(client, null);
		}).then((response) => {
			assert.strictEqual(response.length, 1);
			assert.strictEqual(response[0].code, "CLIENT_ERROR");
			assert.strictEqual(response[0].message, "received unsupported command");
		}).then(() => {
			return mc.sendRequestAndReadResponse(client, `set ${key} 10 AZ 6\r\n`);
		}).then((response) => {
			assert.strictEqual(response.length, 1);
			assert.strictEqual(response[0].code, "CLIENT_ERROR");
			assert.strictEqual(response[0].message, "received invalid command arguments");
		}).then(() => {
			return mc.sendRequestAndReadResponse(client, `set ${key} 10 10 AZ\r\n`);
		}).then((response) => {
			assert.strictEqual(response.length, 1);
			assert.strictEqual(response[0].code, "CLIENT_ERROR");
			assert.strictEqual(response[0].message, "received invalid command arguments");
		}).then(() => {
			return mc.sendRequestAndReadResponse(client, `set  10 10 1\r\nA\r\n`);
		}).then((response) => {
			assert.strictEqual(response.length, 1);
			assert.strictEqual(response[0].code, "CLIENT_ERROR");
			assert.strictEqual(response[0].message, "received invalid command arguments");
		}).then(() => {
			return mc.sendRequestAndReadResponse(client, null);
		}).then((response) => {
			assert.strictEqual(response.length, 1);
			assert.strictEqual(response[0].code, "CLIENT_ERROR");
			assert.strictEqual(response[0].message, "received unsupported command");
		}).then(() => {
			return mc.sendRequestAndReadResponse(client, `set 0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789X 10 10 1\r\nA\r\n`);
		}).then((response) => {
			assert.strictEqual(response.length, 1);
			assert.strictEqual(response[0].code, "CLIENT_ERROR");
			assert.strictEqual(response[0].message, "received invalid command arguments");
		}).then(() => {
			client.end();
			done();
		}).catch((err) => {
			client.end();
			done(err);
		});
	});

	it('invalid command', (done) => {
		let client = null;
		server.then(() => {
			return mc.connect('localhost', config.processPorts[0]);
		}).then((c) => {
			client = c;
		}).then(() => {
			return mc.sendRequestAndReadResponse(client, `unknown\r\n`);
		}).then((response) => {
			assert.strictEqual(response.length, 1);
			assert.strictEqual(response[0].code, "CLIENT_ERROR");
			assert.strictEqual(response[0].message, "received unsupported command");
		}).then(() => {
			client.end();
			done();
		}).catch((err) => {
			client.end();
			done(err);
		});
	});

	it('invalid config command', (done) => {
		let client = null;
		server.then(() => {
			return mc.connect('localhost', config.configPort);
		}).then((c) => {
			client = c;
		}).then(() => {
			return mc.sendRequestAndReadResponse(client, `unknown\r\n`);
		}).then((response) => {
			assert.strictEqual(response.length, 1);
			assert.strictEqual(response[0].code, "CLIENT_ERROR");
			assert.strictEqual(response[0].message, "received unsupported command");
		}).then(() => {
			client.end();
			done();
		}).catch((err) => {
			client.end();
			done(err);
		});
	});

	it('invalid echo command', (done) => {
		let client = null;
		server.then(() => {
			return mc.connect('localhost', config.processPorts[0]);
		}).then((c) => {
			client = c;
		}).then(() => {
			return mc.sendRequestAndReadResponse(client, `echo\r\n`);
		}).then((response) => {
			assert.strictEqual(response.length, 1);
			assert.strictEqual(response[0].code, "CLIENT_ERROR");
			assert.strictEqual(response[0].message, "received badly formatted command");
		}).then(() => {
			return mc.sendRequestAndReadResponse(client, `echo -1\r\n`);
		}).then((response) => {
			assert.strictEqual(response.length, 1);
			assert.strictEqual(response[0].code, "CLIENT_ERROR");
			assert.strictEqual(response[0].message, "received invalid command arguments");
		}).then(() => {
			return mc.sendRequestAndReadResponse(client, `echo 5\r\n123456\r\n`);
		}).then((response) => {
			assert.strictEqual(response.length, 1);
			assert.strictEqual(response[0].code, "CLIENT_ERROR");
			assert.strictEqual(response[0].message, "received invalid command data");
		}).then(() => {
			client.end();
			done();
		}).catch((err) => {
			client.end();
			done(err);
		});
	});

	it('commands with an invalid key', (done) => {
		let client = null;
		const key = "abc\tdef";
		server.then(() => {
			return mc.connect('localhost', config.processPorts[0]);
		}).then((c) => {
			client = c;
		}).then(() => {
			return mc.sendRequestAndReadResponse(client, `get ${key}\r\n`);
		}).then((response) => {
			assert.strictEqual(response.length, 1);
			assert.strictEqual(response[0].code, "CLIENT_ERROR");
			assert.strictEqual(response[0].message, "received invalid command arguments");
		}).then(() => {
			return mc.sendRequestAndReadResponse(client, `set ${key} 10 0 6\r\nAZERTY\r\n`);
		}).then((response) => {
			assert.strictEqual(response.length, 1);
			assert.strictEqual(response[0].code, "CLIENT_ERROR");
			assert.strictEqual(response[0].message, "received invalid command arguments");
		}).then(() => {
			client.end();
			done();
		}).catch((err) => {
			client.end();
			done(err);
		});
	});

	it('two echo with pending data', (done) => {
		let client = null;
		server.then(() => {
			return mc.connect('localhost', config.processPorts[0]);
		}).then((c) => {
			client = c;
		}).then(() => {
			return mc.sendRequestAndReadResponse(client, `echo 5\r\nEND\r\n\r\necho 5\r\nEND`);
		}).then((response) => {
			assert.strictEqual(response.length, 1);
			assert.strictEqual(response[0].code, "END");
		}).then(() => {
			return mc.sendRequestAndReadResponse(client, `\r\n\r\n`);
		}).then((response) => {
			assert.strictEqual(response.length, 1);
			assert.strictEqual(response[0].code, "END");
		}).then(() => {
			client.end();
			done();
		}).catch((err) => {
			client.end();
			done(err);
		});
	});

	it('two set with pending data', (done) => {
		let client = null;
		const key = uuid();
		server.then(() => {
			return mc.connect('localhost', config.processPorts[0]);
		}).then((c) => {
			client = c;
		}).then(() => {
			return mc.sendRequestAndReadResponse(client, `set ${key} 10 0 6\r\nAZERTY\r\nset ${key} 10 0 6\r\nAZE`);
		}).then((response) => {
			assert.strictEqual(response.length, 1);
			assert.strictEqual(response[0].code, "STORED");
		}).then(() => {
			return mc.sendRequestAndReadResponse(client, `RTY\r\n`);
		}).then((response) => {
			assert.strictEqual(response.length, 1);
			assert.strictEqual(response[0].code, "STORED");
		}).then(() => {
			client.end();
			done();
		}).catch((err) => {
			client.end();
			done(err);
		});
	});

	it('two get with pending data', (done) => {
		let client = null;
		const key = uuid();
		server.then(() => {
			return mc.connect('localhost', config.processPorts[0]);
		}).then((c) => {
			client = c;
		}).then(() => {
			return mc.sendRequestAndReadResponse(client, `get ${key}\r\nget`);
		}).then((response) => {
			assert.strictEqual(response.length, 1);
			assert.strictEqual(response[0].code, "END");
		}).then(() => {
			return mc.sendRequestAndReadResponse(client, ` ${key}\r\n`);
		}).then((response) => {
			assert.strictEqual(response.length, 1);
			assert.strictEqual(response[0].code, "END");
		}).then(() => {
			client.end();
			done();
		}).catch((err) => {
			client.end();
			done(err);
		});
	});

	it('set, add and replace with noreply does not return', (done) => {
		let client = null;
		const key = uuid();
		server.then(() => {
			return mc.connect('localhost', config.processPorts[0]);
		}).then((c) => {
			client = c;
		}).then(() => {
			client.write(`set ${key} 10 0 6 noreply\r\nAZERTY\r\n`);
		}).then(() => {
			return mc.sendRequestAndReadResponse(client, `get ${key}\r\n`);
		}).then((response) => {
			assert.strictEqual(response.length, 2);
			assert.strictEqual(response[0].code, "VALUE");
			assert.strictEqual(response[0].value.toString('utf-8'), "AZERTY");
			assert.strictEqual(response[1].code, "END");
		}).then(() => {
			client.write(`add ${key} 10 0 6 noreply\r\nAZERTY\r\n`);
		}).then(() => {
			return mc.sendRequestAndReadResponse(client, `get ${key}\r\n`);
		}).then((response) => {
			assert.strictEqual(response.length, 2);
			assert.strictEqual(response[0].code, "VALUE");
			assert.strictEqual(response[0].value.toString('utf-8'), "AZERTY");
			assert.strictEqual(response[1].code, "END");
		}).then(() => {
			client.write(`replace ${key} 10 0 6 noreply\r\nAZERTY\r\n`);
		}).then(() => {
			return mc.sendRequestAndReadResponse(client, `get ${key}\r\n`);
		}).then((response) => {
			assert.strictEqual(response.length, 2);
			assert.strictEqual(response[0].code, "VALUE");
			assert.strictEqual(response[0].value.toString('utf-8'), "AZERTY");
			assert.strictEqual(response[1].code, "END");
		}).then(() => {
			client.end();
			done();
		}).catch((err) => {
			client.end();
			done(err);
		});
	});

	it('override responses in config server', (done) => {
		let client = null;
		server.then(() => {
			return mc.connect('localhost', config.configPort);
		}).then((c) => {
			client = c;
		}).then(() => {
			config.nextResponses.push("VALUE abcdef 0 6\r\nAZERTY\r\nEND\r\n");
		}).then(() => {
			return mc.sendRequestAndReadResponse(client, `config get cluster\r\n`);
		}).then((response) => {
			assert.strictEqual(response.length, 2);
			assert.strictEqual(response[0].code, "VALUE");
			assert.strictEqual(response[0].key, "abcdef");
			assert.strictEqual(response[0].value.toString(), "AZERTY");
			assert.strictEqual(response[1].code, "END");
		}).then(() => {
			client.end();
			done();
		}).catch((err) => {
			client.end();
			done(err);
		});
	});

	it('override responses in data server', (done) => {
		let client = null;
		const key = uuid();
		server.then(() => {
			return mc.connect('localhost', config.processPorts[0]);
		}).then((c) => {
			client = c;
		}).then(() => {
			config.nextResponses.push("VALUE abcdef 0 6\r\nAZERTY\r\nEND\r\n");
			config.nextResponses.push("VALUE ghijkl 0 6\r\nQWERTY\r\nEND\r\n");
			config.nextResponses.push("VALUE mnopqr 0 6\r\nDVORAK\r\nEND\r\n");
		}).then(() => {
			return mc.sendRequestAndReadResponse(client, `set ${key} 10 0 6 noreply\r\nAZERTY\r\n`);
		}).then((response) => {
			assert.strictEqual(response.length, 2);
			assert.strictEqual(response[0].code, "VALUE");
			assert.strictEqual(response[0].key, "abcdef");
			assert.strictEqual(response[0].value.toString(), "AZERTY");
			assert.strictEqual(response[1].code, "END");
		}).then(() => {
			return mc.sendRequestAndReadResponse(client, `get ${key}\r\n`);
		}).then((response) => {
			assert.strictEqual(response.length, 2);
			assert.strictEqual(response[0].code, "VALUE");
			assert.strictEqual(response[0].key, "ghijkl");
			assert.strictEqual(response[0].value.toString(), "QWERTY");
			assert.strictEqual(response[1].code, "END");
		}).then(() => {
			return mc.sendRequestAndReadResponse(client, `echo 17\r\nTHIS IS THE END\r\n\r\n`);
		}).then((response) => {
			assert.strictEqual(response.length, 2);
			assert.strictEqual(response[0].code, "VALUE");
			assert.strictEqual(response[0].key, "mnopqr");
			assert.strictEqual(response[0].value.toString(), "DVORAK");
			assert.strictEqual(response[1].code, "END");
		}).then(() => {
			client.end();
			done();
		}).catch((err) => {
			client.end();
			done(err);
		});
	});

	it('pending data in server', (done) => {
		let client = null;
		const key = uuid();
		server.then(() => {
			return mc.connect('localhost', config.processPorts[0]);
		}).then((c) => {
			client = c;
		}).then(() => {
			config.nextResponses.push("VALUE abcdef 0 6\r\nAZERTY\r\nEND\r\nVALUE ghijkl 0 6");
			config.nextResponses.push("\r\nQWERTY\r\nEND\r\n");
		}).then(() => {
			return mc.sendRequestAndReadResponse(client, `set ${key} 10 0 6 noreply\r\nAZERTY\r\n`);
		}).then((response) => {
			assert.strictEqual(response.length, 2);
			assert.strictEqual(response[0].code, "VALUE");
			assert.strictEqual(response[0].key, "abcdef");
			assert.strictEqual(response[0].value.toString(), "AZERTY");
			assert.strictEqual(response[1].code, "END");
		}).then(() => {
			return mc.sendRequestAndReadResponse(client, `get ${key}\r\n`);
		}).then((response) => {
			assert.strictEqual(response.length, 2);
			assert.strictEqual(response[0].code, "VALUE");
			assert.strictEqual(response[0].key, "ghijkl");
			assert.strictEqual(response[0].value.toString(), "QWERTY");
			assert.strictEqual(response[1].code, "END");
		}).then(() => {
			client.end();
			done();
		}).catch((err) => {
			client.end();
			done(err);
		});
	});

	it('should fail with invalid parameters', (done) => {
		assert.throws(() => mch({ shards: "10" }), /The number of shards must be a positive integer up to 20/);
		assert.throws(() => mch({ shards: {} }), /The number of shards must be a positive integer up to 20/);
		assert.throws(() => mch({ shards: -10 }), /The number of shards must be a positive integer up to 20/);
		assert.throws(() => mch({ shards: 0 }), /The number of shards must be a positive integer up to 20/);
		assert.throws(() => mch({ shards: 21 }), /The number of shards must be a positive integer up to 20/);
		assert.throws(() => mch({ shards: 1/0 }), /The number of shards must be a positive integer up to 20/);
		assert.throws(() => mch({ configPort: "10" }), /The configuration port number must be a valid TCP port number \[1-65535\] or 0/);
		assert.throws(() => mch({ configPort: {} }), /The configuration port number must be a valid TCP port number \[1-65535\] or 0/);
		assert.throws(() => mch({ configPort: -10 }), /The configuration port number must be a valid TCP port number \[1-65535\] or 0/);
		assert.throws(() => mch({ configPort: 100000 }), /The configuration port number must be a valid TCP port number \[1-65535\] or 0/);
		done();
	});
});
