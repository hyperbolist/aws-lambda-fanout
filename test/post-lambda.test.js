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

const AWS    = require('./mock-aws.js');
const uuid   = require('uuid');
const assert = require('assert');
const post   = require('../lib/post-lambda.js');

describe('post-lambda', () => {
	let processRequest = null;
	before(() => {
		AWS.mock('Lambda','invoke', (params) => {
			processRequest(params);
		});
	});
	after(() => {
		AWS.restore('Lambda','invoke');
	});

	it('setAWS', (done) => {
		post.setAWS(AWS);
		done();
	});

	it('destinationRegex', (done) => {
		assert(! post.destinationRegex.test(""));
		assert(! post.destinationRegex.test("#Invalid"));
		assert(post.destinationRegex.test("functionName"));
		assert(post.destinationRegex.test("functionName:$LATEST"));
		assert(post.destinationRegex.test("functionName:ALIAS"));
		assert(post.destinationRegex.test("functionName:10"));
		done();
	});

	it('configure', (done) => {
		let conf = post.configure();
		assert.strictEqual(conf.service, 'Lambda');
		conf = post.configure(null);
		assert.strictEqual(conf.service, 'Lambda');
		conf = post.configure({});
		assert.strictEqual(conf.service, 'Lambda');
		conf = post.configure({ service: "OtherService" });
		assert.strictEqual(conf.service, 'OtherService');
		done();
	});

	it('target', (done) => {
		let target = { };
		post.targetSettings(target);
		assert.deepEqual(target, { collapse: "API" });
		target = { collapse: "none" };
		post.targetSettings(target);
		assert.deepEqual(target, { collapse: "API" });
		done();
	});

	it('intercept', () => {
		const key     = uuid();
		const target  = { destination: `functionName` };
		const service = post.create(target, { debug: false });

		processRequest = (params) => {
			assert.strictEqual(params.FunctionName, target.destination);
			assert.strictEqual(params.Qualifier, '$LATEST');
			assert.strictEqual(params.InvocationType, 'Event');
			assert.deepEqual(JSON.parse(params.Payload), { Records: [ { key: key, data: `AZERTY${key}` } ] });
		};
		return post.intercept(service, target, { Records: [ { key: key, data: `AZERTY${key}` } ] });
	});

	it('intercept (default)', () => {
		const key     = uuid();
		const target  = { destination: `functionName` };
		const service = post.create(target);

		processRequest = (params) => {
			assert.strictEqual(params.FunctionName, target.destination);
			assert.strictEqual(params.Qualifier, '$LATEST');
			assert.strictEqual(params.InvocationType, 'Event');
			assert.deepEqual(JSON.parse(params.Payload), { Records: [ { key: key, data: `AZERTY${key}` } ] });
		};
		return post.intercept(service, target, { Records: [ { key: key, data: `AZERTY${key}` } ] });
	});

	it('intercept (with version)', () => {
		const key     = uuid();
		const target  = { destination: `functionName:10` };
		const service = post.create(target);

		processRequest = (params) => {
			assert.strictEqual(params.FunctionName, target.destination.split(':')[0]);
			assert.strictEqual(params.Qualifier, target.destination.split(':')[1]);
			assert.strictEqual(params.InvocationType, 'Event');
			assert.deepEqual(JSON.parse(params.Payload), { Records: [ { key: key, data: `AZERTY${key}` } ] });
		};
		return post.intercept(service, target, { Records: [ { key: key, data: `AZERTY${key}` } ] });
	});

	it('error', () => {
		const key     = uuid();
		const target  = { destination: `functionName` };
		const service = post.create(target);

		processRequest = () => {
			throw new Error("Unable to comply");
		};

		return post.intercept(service, target, { Records: [ { key: key, data: `AZERTY${key}` } ] }).then(() => {
			throw new Error("An error should have been raised");
		}).catch((err) => {
			assert(err instanceof Error);
			assert.strictEqual(err.message, `Unable to comply`);
		});
	});
});
