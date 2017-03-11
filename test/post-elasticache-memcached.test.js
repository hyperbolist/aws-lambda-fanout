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
 * This Node.js script tests the features from the post-elasticache-memcached.js
 */

/* global describe:false, it:false, before:false, after:false */

'use strict';

const uuid      = require('uuid');
const assert    = require('assert');
const post      = require('../lib/post-elasticache-memcached.js');
const sinon     = require('sinon');
const memcached = require('../lib/memcached.js');

describe('post-memcached-redis', () => {
	let processRequest = null;
	before(() => {
		sinon.stub(memcached, 'servers', () => {
			return Promise.resolve({});
		});
		sinon.stub(memcached, 'set', (slots, records, options) => {
			try {
				return Promise.resolve(processRequest(slots, records, options));
			} catch(e) {
				return Promise.reject(e);
			}
		});
	});
	after(() => {
		memcached.set.restore();
		memcached.servers.restore();
	});

	it('setAWS', (done) => {
		post.setAWS(null);
		done();
	});

	it('destinationRegex', (done) => {
		assert(! post.destinationRegex.test(""));
		assert(! post.destinationRegex.test("nothing"));
		assert(post.destinationRegex.test("test-memcached.abcdef.cfg.euw1.cache.amazonaws.com:11211"));
		done();
	});

	it('configure', (done) => {
		let conf = post.configure();
		assert.strictEqual(conf.service, 'memcached');
		conf = post.configure(null);
		assert.strictEqual(conf.service, 'memcached');
		conf = post.configure({});
		assert.strictEqual(conf.service, 'memcached');
		conf = post.configure({ service: "OtherService" });
		assert.strictEqual(conf.service, 'OtherService');
		done();
	});

	it('target', (done) => {
		let target = { };
		post.targetSettings(target);
		assert.deepEqual(target, { collapse: "multiple" });
		target = { collapse: "JSON" };
		post.targetSettings(target);
		assert.deepEqual(target, { collapse: "multiple" });
		target = { role: "roleName" };
		post.targetSettings(target);
		assert.deepEqual(target, { collapse: "multiple", role: null });
		target = { region: "myRegion" };
		post.targetSettings(target);
		assert.deepEqual(target, { collapse: "multiple", region: null });
		done();
	});

	it('store item', () => {
		const key     = uuid();
		const target  = { destination: `localhost:11211` };
		const service = post.create(target);

		processRequest = (slots, records) => {
			assert.deepEqual(records, [ { key: key, data: new Buffer(`AZERTY${key}`, `utf-8`) } ]);
		};

		return post.send(service, target, [ { key: key, data: new Buffer(`AZERTY${key}`, `utf-8`) } ]);
	});

	it('store 2 items in 2 calls', () => {
		const key     = uuid();
		const target  = { destination: `localhost:11211` };
		const service = post.create(target);

		processRequest = (slots, records) => {
			assert.deepEqual(records, [ { key: `${key}-1`, data: new Buffer(`AZERTY${key}`, `utf-8`) } ]);
		};

		return post.send(service, target, [ { key: `${key}-1`, data: new Buffer(`AZERTY${key}`, `utf-8`) } ])
			.then(() => {
				processRequest = (slots, records) => {
					assert.deepEqual(records, [ { key: `${key}-2`, data: new Buffer(`QWERTY${key}`, `utf-8`) } ]);
				};

				return post.send(service, target, [ { key: `${key}-2`, data: new Buffer(`QWERTY${key}`, `utf-8`) } ]);
			});
	});

	it('store 2 items in 1 call', () => {
		const key     = uuid();
		const target  = { destination: `localhost:11211` };
		const service = post.create(target);

		processRequest = (slots, records) => {
			assert.deepEqual(records, [ { key: `${key}-1`, data: new Buffer(`AZERTY${key}`, `utf-8`) }, { key: `${key}-2`, data: new Buffer(`QWERTY${key}`, `utf-8`) } ]);
		};

		return post.send(service, target, [ { key: `${key}-1`, data: new Buffer(`AZERTY${key}`, `utf-8`) }, { key: `${key}-2`, data: new Buffer(`QWERTY${key}`, `utf-8`) } ]);
	});

	it('store item with options', () => {
		const key     = uuid();
		const target  = { destination: `localhost:11211` };
		const service = post.create(target, { test: true });

		processRequest = (slots, records, options) => {
			assert.strictEqual(options.test, true);
			assert.deepEqual(records, [ { key: key, data: new Buffer(`AZERTY${key}`, `utf-8`) } ]);
		};

		return post.send(service, target, [ { key: key, data: new Buffer(`AZERTY${key}`, `utf-8`) } ]);
	});
});
