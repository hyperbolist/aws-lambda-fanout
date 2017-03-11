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
 * This Node.js script tests the features from the post-sns.js
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
const post   = require('../lib/post-sns.js');

describe('post-sns', () => {
	let processRequest = null;
	before(() => {
		AWS.mock('SNS','publish', (params) => {
			processRequest(params);
		});
	});
	after(() => {
		AWS.restore('SNS','publish');
	});

	it('setAWS', (done) => {
		post.setAWS(AWS);
		done();
	});

	it('destinationRegex', (done) => {
		assert(! post.destinationRegex.test(""));
		assert(! post.destinationRegex.test("nothing"));
		assert(post.destinationRegex.test("arn:aws:sns:xx-test-1:123456789012:myTopic"));
		done();
	});

	it('configure', (done) => {
		let conf = post.configure();
		assert.strictEqual(conf.service, 'SNS');
		conf = post.configure(null);
		assert.strictEqual(conf.service, 'SNS');
		conf = post.configure({});
		assert.strictEqual(conf.service, 'SNS');
		conf = post.configure({ service: "OtherService" });
		assert.strictEqual(conf.service, 'OtherService');
		done();
	});

	it('target', (done) => {
		let target = { };
		post.targetSettings(target);
		assert.deepEqual(target, { });
		target = { collapse: "JSON" };
		post.targetSettings(target);
		assert.deepEqual(target, { collapse: "JSON" });
		done();
	});

	it('store item (with params, no collapse)', () => {
		const key     = uuid();
		const target  = { destination: `arn:aws:sns:xx-test-1:123456789012:myTopic` };
		const service = post.create(target, { debug: false } );

		processRequest = (params) => {
			assert.deepEqual(params.Message, `AZERTY${key}`);
		};
		return post.send(service, target, [ { key: key, data: new Buffer(`AZERTY${key}`, `utf-8`) } ]);
	});

	it('store item (no collapse)', () => {
		const key     = uuid();
		const target  = { destination: `arn:aws:sns:xx-test-1:123456789012:myTopic` };
		const service = post.create(target);

		processRequest = (params) => {
			assert.deepEqual(params.Message, `AZERTY${key}`);
		};
		return post.send(service, target, [ { key: key, data: new Buffer(`AZERTY${key}`, `utf-8`) } ]);
	});

	it('store multiple items (no collapse)', () => {
		const key     = uuid();
		const target  = { destination: `arn:aws:sns:xx-test-1:123456789012:myTopic` };
		const service = post.create(target);

		return post.send(service, target, [ { key: `${key}-1`, data: new Buffer(`AZERTY${key}`, `utf-8`) }, { key: `${key}-2`, data: new Buffer(`QWERTY${key}`, `utf-8`) } ]).then(() => {
			throw new Error("An error should have been raised");
		}).catch((err) => {
			assert(err instanceof Error);
			assert.strictEqual(err.message, `Multiple records must be collapsed, SNS supports JSON|concat-b64|concat`);
		});
	});

	it('store item (JSON)', () => {
		const key     = uuid();
		const target  = { destination: `arn:aws:sns:xx-test-1:123456789012:myTopic`, collapse: 'JSON' };
		const service = post.create(target);

		processRequest = (params) => {
			assert.deepEqual(JSON.parse(params.Message), { Records: [ { id: `${key}-2`, value: `AZERTY${key}` }, { id: `${key}-2`, value: `QWERTY${key}` } ] });
		};
		return post.send(service, target, [ { key: `${key}-1`, data: new Buffer(JSON.stringify({ id: `${key}-2`, value: `AZERTY${key}` }), `utf-8`) }, { key: `${key}-2`, data: new Buffer(JSON.stringify({ id: `${key}-2`, value: `QWERTY${key}` }), `utf-8`) } ]);
	});

	it('store item (concat)', () => {
		const key     = uuid();
		const target  = { destination: `arn:aws:sns:xx-test-1:123456789012:myTopic`, collapse: 'concat' };
		const service = post.create(target);

		processRequest = (params) => {
			assert.deepEqual(params.Message, `AZERTY${key}QWERTY${key}`);
		};
		return post.send(service, target, [ { key: `${key}-1`, data: new Buffer(`AZERTY${key}`, `utf-8`) }, { key: `${key}-2`, data: new Buffer(`QWERTY${key}`, `utf-8`) } ]);
	});

	it('store item (concat with separator)', () => {
		const key     = uuid();
		const target  = { destination: `arn:aws:sns:xx-test-1:123456789012:myTopic`, collapse: 'concat', separator: '\n' };
		const service = post.create(target);

		processRequest = (params) => {
			assert.deepEqual(params.Message, `AZERTY${key}\nQWERTY${key}`);
		};
		return post.send(service, target, [ { key: `${key}-1`, data: new Buffer(`AZERTY${key}`, `utf-8`) }, { key: `${key}-2`, data: new Buffer(`QWERTY${key}`, `utf-8`) } ]);
	});

	it('store item (concat-b64)', () => {
		const key     = uuid();
		const target  = { destination: `arn:aws:sns:xx-test-1:123456789012:myTopic`, collapse: 'concat-b64' };
		const service = post.create(target);

		processRequest = (params) => {
			assert.deepEqual(params.Message, new Buffer(`AZERTY${key}QWERTY${key}`, 'utf-8').toString('base64'));
		};
		return post.send(service, target, [ { key: `${key}-1`, data: new Buffer(`AZERTY${key}`, `utf-8`) }, { key: `${key}-2`, data: new Buffer(`QWERTY${key}`, `utf-8`) } ]);
	});
});
