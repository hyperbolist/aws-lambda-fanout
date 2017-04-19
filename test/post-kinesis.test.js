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
 * This Node.js script tests the features from the post-elasticache-redis.js
 */

/* global describe:false, it:false, before:false, after:false */

'use strict';

// Environment settings for AWS SDK
process.env.AWS_REGION            = 'xx-test-1'; 
process.env.AWS_ACCESS_KEY_ID     = "AKIAENVEXAMPLE";
process.env.AWS_SECRET_ACCESS_KEY = "SecretKeyExample";
process.env.AWS_SESSION_TOKEN     = "SessionTokenExample";

const AWS          = require('./mock-aws.js');
const uuid         = require('uuid');
const assert       = require('assert');
const post         = require('../lib/post-kinesis.js');

describe('post-kinesis', () => {
	let processRequest = null;
	before(() => {
		AWS.mock('Kinesis','putRecords', (params) => {
			processRequest(params);
		});
	});
	after(() => {
		AWS.restore('Kinesis','putRecords');
	});

	it('setAWS', (done) => {
		post.setAWS(AWS);
		done();
	});

	it('destinationRegex', (done) => {
		assert(! post.destinationRegex.test(""));
		assert(! post.destinationRegex.test("#Invalid"));
		assert(post.destinationRegex.test("streamName"));
		done();
	});

	it('configure', (done) => {
		let conf = post.configure();
		assert.strictEqual(conf.service, 'Kinesis');
		conf = post.configure(null);
		assert.strictEqual(conf.service, 'Kinesis');
		conf = post.configure({});
		assert.strictEqual(conf.service, 'Kinesis');
		conf = post.configure({ service: "OtherService" });
		assert.strictEqual(conf.service, 'OtherService');
		done();
	});

	it('target', (done) => {
		let target = { };
		post.targetSettings(target);
		assert.deepEqual(target, { collapse: "API" });
		target = { collapse: "JSON" };
		post.targetSettings(target);
		assert.deepEqual(target, { collapse: "API" });
		target = { collapse: "none" };
		post.targetSettings(target);
		assert.deepEqual(target, { collapse: "API" });
		target = { role: "roleName" };
		post.targetSettings(target);
		assert.deepEqual(target, { collapse: "API", role: "roleName" });
		done();
	});

	it('store item (with params)', () => {
		const key     = uuid();
		const target  = { destination: `streamName` };
		const service = post.create(target, { debug: false });

		processRequest = (params) => {
			assert.deepEqual(params.Records, [ { PartitionKey: key, Data: new Buffer(`AZERTY${key}`, `utf-8`) } ]);
		};
		return post.send(service, target, [ { key: key, data: new Buffer(`AZERTY${key}`, `utf-8`) } ]);
	});

	it('store item (default)', () => {
		const key     = uuid();
		const target  = { destination: `streamName` };
		const service = post.create(target);

		processRequest = (params) => {
			assert.deepEqual(params.Records, [ { PartitionKey: key, Data: new Buffer(`AZERTY${key}`, `utf-8`) } ]);
		};

		return post.send(service, target, [ { key: key, data: new Buffer(`AZERTY${key}`, `utf-8`) } ]);
	});

	it('store items', () => {
		const key     = uuid();
		const target  = { destination: `streamName` };
		const service = post.create(target);

		processRequest = (params) => {
			assert.deepEqual(params.Records, [ { PartitionKey: `${key}#1`, Data: new Buffer(`AZERTY${key}`, `utf-8`) }, { PartitionKey: `${key}#2`, Data: new Buffer(`QWERTY${key}`, `utf-8`) } ]);
		};

		return post.send(service, target, [ { key: `${key}#1`, data: new Buffer(`AZERTY${key}`, `utf-8`) }, { key: `${key}#2`, data: new Buffer(`QWERTY${key}`, `utf-8`) } ]);
	});

	it('error', () => {
		const key     = uuid();
		const target  = { destination: `streamName` };
		const service = post.create(target);

		processRequest = () => {
			throw new Error("Unable to comply");
		};

		return post.send(service, target, [ { key: key, data: new Buffer(`AZERTY${key}`, `utf-8`) } ]).then(() => {
			throw new Error("An error should have been raised");
		}).catch((err) => {
			assert(err instanceof Error);
			assert.strictEqual(err.message, `Unable to comply`);
		});
	});
});
