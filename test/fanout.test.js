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
 * This Node.js script tests the features from the transformation.js script
 */

/* global describe:false, it:false, before: false, after: false */

'use strict';

// Environment settings for AWS SDK
process.env.AWS_REGION            = 'xx-test-1'; 
process.env.AWS_ACCESS_KEY_ID     = "AKIAENVEXAMPLE";
process.env.AWS_SECRET_ACCESS_KEY = "SecretKeyExample";
process.env.AWS_SESSION_TOKEN     = "SessionTokenExample";

const configuration = require('../lib/configuration.js');
const statistics = require('../lib/statistics.js');
const fanout = require('../fanout.js');
const sinon  = require('sinon');
const assert = require('assert');
const path   = require('path');

let sendStatistics = null;

function FakeStatistics() {
	this.stats = {};
}
FakeStatistics.prototype.register = function(name, type, unit, source, destination) {
	const key = `${name}#${source || ''}#${destination || ''}`;
	this.stats[key] = 0;
};
FakeStatistics.prototype.addTick = function(name, source, destination) {
	this.addValue(name, 1, source, destination);
};
FakeStatistics.prototype.addValue = function(name, value, source, destination) {
	const key = `${name}#${source || ''}#${destination || ''}`;
	if(this.stats.hasOwnProperty(key)) {
		this.stats[key] += value;
	} else {
		throw new Error(`Statistic not registered '${key}'`);
	}
	return this.stats[key];
};
FakeStatistics.prototype.publish = function() {
	return sendStatistics(this.stats);
};

describe('fanout', () => {
	let processConfigurationRequest = null;

	const createStatistics = () => {
		return new FakeStatistics();
	};

	before(() => {
		sinon.stub(configuration, 'get', () => {
			return Promise.resolve(processConfigurationRequest());
		});
		sinon.stub(statistics, 'create', () => {
			return createStatistics();
		});
	});
	after(() => {
		statistics.create.restore();
		configuration.get.restore();
	});

	it('should handle no records', (done) => {
		let stats = {};
		sendStatistics = (s) => {
			stats = s;
		};
		processConfigurationRequest = () => {
			return [];
		};
		const event = { "Records": [] };
		fanout.handler(event, null, (err) => {
			if(err) {
				done(err);
			} else {
				try {
					assert.strictEqual(stats['Invocations##'], 1);
					assert.strictEqual(stats['InputRecords##'], 0);
					done();
				} catch(e) {
					done(e);
				}
			}
		});
	});

	it('should handle no targets', (done) => {
		let stats = {};
		sendStatistics = (s) => {
			stats = s;
		};
		processConfigurationRequest = () => {
			return [];
		};
		const event = { "Records": [ { "eventID": "shardId-000000000000:49545115243490985018280067714973144582180062593244200961", "eventVersion": "1.0", "kinesis": { "approximateArrivalTimestamp": 1428537600, "partitionKey": "partitionKey-3", "data": "SGVsbG8sIHRoaXMgaXMgYSB0ZXN0IDEyMy4=", "kinesisSchemaVersion": "1.0", "sequenceNumber": "49545115243490985018280067714973144582180062593244200961" }, "invokeIdentityArn": "arn:aws:iam::EXAMPLE", "eventName": "aws:kinesis:record", "eventSourceARN": "arn:aws:kinesis:EXAMPLE", "eventSource": "aws:kinesis", "awsRegion": "xx-test-1" } ] };
		fanout.handler(event, null, (err) => {
			if(err) {
				done(err);
			} else {
				try {
					assert.strictEqual(stats['Invocations##'], 1);
					assert.strictEqual(stats['InputRecords##'], 1);

					assert.strictEqual(stats['Invocations#arn:aws:kinesis:EXAMPLE#'], 1);
					assert.strictEqual(stats['InputRecords#arn:aws:kinesis:EXAMPLE#'], 1);

					assert.strictEqual(stats['Records#arn:aws:kinesis:EXAMPLE#'], 0);
					assert.strictEqual(stats['Targets#arn:aws:kinesis:EXAMPLE#'], 0);
					done();
				} catch(e) {
					done(e);
				}
			}
		});
	});

	it('should handle configuration', () => {
		process.env.CONFIGURATION = '{"test":true}';
		const fanoutPath = path.resolve(__dirname, '../fanout.js');
		assert(require.cache.hasOwnProperty(fanoutPath));
		delete require.cache[fanoutPath];
		const fanout2 = require('../fanout.js');
		const config = fanout2.getConfig();
		assert.strictEqual(config.test, true);
		require.cache[fanoutPath] = fanout;
	});

	it('should handle invalid configuration', () => {
		process.env.CONFIGURATION = "#";
		const fanoutPath = path.resolve(__dirname, '../fanout.js');
		assert(require.cache.hasOwnProperty(fanoutPath));
		delete require.cache[fanoutPath];
		require('../fanout.js');
		require.cache[fanoutPath] = fanout;
	});
});
