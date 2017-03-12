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
 * This Node.js script tests the features from the crc16.js script against well known CRC16
 */

/* global describe:false, it:false, before:false, after:false */

'use strict';

// Environment settings for AWS SDK
process.env.AWS_REGION               = 'xx-test-1'; 
process.env.AWS_ACCESS_KEY_ID        = "AKIAENVEXAMPLE";
process.env.AWS_SECRET_ACCESS_KEY    = "SecretKeyExample";
process.env.AWS_SESSION_TOKEN        = "SessionTokenExample";

const AWS        = require('./mock-aws.js');
const statistics = require('../lib/statistics.js');
const uuid       = require('uuid');
const assert     = require('assert');

describe('statistics', () => {
	let processRequest = null;
	before(() => {
		AWS.mock('CloudWatch','putMetricData', (params) => {
			return processRequest(params);
		});
	});

	after(() => {
		AWS.restore('CloudWatch','putMetricData');
	});

	it('setAWS', (done) => {
		statistics.setAWS(AWS);
		done();
	});

	it('create', (done) => {
		assert(statistics.create());
		assert(statistics.create({ functionName: 'test' }));
		assert(statistics.create({ cloudWatchNamespace: 'test' }));
		assert.throws(() => statistics.create({ cloudWatchNamespace: 'AWS/Test' }));
		assert.throws(() => statistics.create({ cloudWatchNamespace: ':' }));
		assert.throws(() => statistics.create({ cloudWatchNamespace: '' }));
		done();
	});

	it('register', () => {
		const requestKey = uuid();
		const stats = statistics.create();
		assert.throws(() => stats.register(10, 'stats', 'Bytes'), /Invalid name '10', must be a string between 1 and 255 characters/);
		assert.throws(() => stats.register('0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF', 'stats', 'Bytes'), /Invalid name '0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF', must be a string between 1 and 255 characters/);
		assert.throws(() => stats.register(`WithSrc${requestKey}`, 'counter', 'Count', '0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF'), /Invalid source '0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF', must be a string with at most 255 characters/);
		assert.throws(() => stats.register(`WithDest${requestKey}`, 'counter', 'Count', null, '0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF'), /Invalid destination '0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF', must be a string with at most 255 characters/);
		stats.register(`TopLevel${requestKey}`, 'counter', 'Count');
		stats.register(`WithSrc${requestKey}`, 'counter', 'Count', `Src${requestKey}`);
		stats.register(`WithDest${requestKey}`, 'counter', 'Count', null, `Dest${requestKey}`);
		stats.register(`WithSrdAndDest${requestKey}`, 'counter', 'Count', `Src${requestKey}`, `Dest${requestKey}`);
		assert.throws(() => stats.register(`TopLevel${requestKey}`, 'stats', 'Bytes'), /Metric 'TopLevel.*' is already registered for source '' and destination ''/);
		assert.throws(() => stats.register(`WithSrc${requestKey}`, 'stats', 'Bytes', `Src${requestKey}`), /Metric 'WithSrc.*' is already registered for source 'Src.*' and destination ''/);
		assert.throws(() => stats.register(`WithDest${requestKey}`, 'stats', 'Bytes', null, `Dest${requestKey}`), /Metric 'WithDest.*' is already registered for source '' and destination 'Dest.*'/);
		assert.throws(() => stats.register(`WithSrdAndDest${requestKey}`, 'stats', 'Bytes', `Src${requestKey}`, `Dest${requestKey}`), /Metric 'WithSrdAndDest.*' is already registered for source 'Src.*' and destination 'Dest.*'/);
		assert.throws(() => stats.register(`InvalidUnit1${requestKey}`, 'stats', 10), /Invalid unit specified '10', allowed values are \(Seconds \| Microseconds \| Milliseconds \| Bytes \| Kilobytes \| Megabytes \| Gigabytes \| Terabytes \| Bits \| Kilobits \| Megabits \| Gigabits \| Terabits \| Percent \| Count \| Bytes\/Second \| Kilobytes\/Second \| Megabytes\/Second \| Gigabytes\/Second \| Terabytes\/Second \| Bits\/Second \| Kilobits\/Second \| Megabits\/Second \| Gigabits\/Second \| Terabits\/Second \| Count\/Second \| None\)/);
		assert.throws(() => stats.register(`InvalidUnit2${requestKey}`, 'stats', 'Lightyears'), /Invalid unit specified 'Lightyears', allowed values are \(Seconds \| Microseconds \| Milliseconds \| Bytes \| Kilobytes \| Megabytes \| Gigabytes \| Terabytes \| Bits \| Kilobits \| Megabits \| Gigabits \| Terabits \| Percent \| Count \| Bytes\/Second \| Kilobytes\/Second \| Megabytes\/Second \| Gigabytes\/Second \| Terabytes\/Second \| Bits\/Second \| Kilobits\/Second \| Megabits\/Second \| Gigabits\/Second \| Terabits\/Second \| Count\/Second \| None\)/);
		assert.throws(() => stats.register(`InvalidType${requestKey}`, 'atoms', 'Count'), /Invalid metric type 'atoms', allowed values are 'counter' and 'stats'/);
	});

	it('publish counter errors', () => {
		const requestKey = uuid();
		const stats = statistics.create();

		assert.throws(() => stats.addTick(`TopLevel${requestKey}`), /Metric 'TopLevel.*' is not registered for source '' and destination ''/);
		assert.throws(() => stats.addTick(`WithSrc${requestKey}`, `Src${requestKey}`), /Metric 'WithSrc.*' is not registered for source 'Src.*' and destination ''/);
		assert.throws(() => stats.addTick(`WithDest${requestKey}`, null, `Dest${requestKey}`), /Metric 'WithDest.*' is not registered for source '' and destination 'Dest.*'/);
		assert.throws(() => stats.addTick(`WithSrdAndDest${requestKey}`, `Src${requestKey}`, `Dest${requestKey}`), /Metric 'WithSrdAndDest.*' is not registered for source 'Src.*' and destination 'Dest.*'/);

		stats.register(`TopLevel${requestKey}`, 'counter', 'Count');
		stats.register(`WithSrc${requestKey}`, 'counter', 'Count', `Src${requestKey}`);
		stats.register(`WithDest${requestKey}`, 'counter', 'Count', null, `Dest${requestKey}`);
		stats.register(`WithSrdAndDest${requestKey}`, 'counter', 'Count', `Src${requestKey}`, `Dest${requestKey}`);

		assert.throws(() => stats.addTick(`TopLevel${requestKey}`, `Src${requestKey}`), /Metric 'TopLevel.*' is not registered for source 'Src.*' and destination ''/);
		assert.throws(() => stats.addTick(`TopLevel${requestKey}`, null, `Dest${requestKey}`), /Metric 'TopLevel.*' is not registered for source '' and destination 'Dest.*'/);
		assert.throws(() => stats.addTick(`TopLevel${requestKey}`, `Src${requestKey}`, `Dest${requestKey}`), /Metric 'TopLevel.*' is not registered for source 'Src.*' and destination 'Dest.*'/);
		assert.throws(() => stats.addTick(`WithSrc${requestKey}`), /Metric 'WithSrc.*' is not registered for source '' and destination ''/);
		assert.throws(() => stats.addTick(`WithSrc${requestKey}`, null, `Dest${requestKey}`), /Metric 'WithSrc.*' is not registered for source '' and destination 'Dest.*'/);
		assert.throws(() => stats.addTick(`WithSrc${requestKey}`, null, `Src${requestKey}`), /Metric 'WithSrc.*' is not registered for source '' and destination 'Src.*'/);
		assert.throws(() => stats.addTick(`WithSrc${requestKey}`, `Src${requestKey}`, `Dest${requestKey}`), /Metric 'WithSrc.*' is not registered for source 'Src.*' and destination 'Dest.*'/);
		assert.throws(() => stats.addTick(`WithDest${requestKey}`), /Metric 'WithDest.*' is not registered for source '' and destination ''/);
		assert.throws(() => stats.addTick(`WithDest${requestKey}`, `Src${requestKey}`), /Metric 'WithDest.*' is not registered for source 'Src.*' and destination ''/);
		assert.throws(() => stats.addTick(`WithDest${requestKey}`, `Dest${requestKey}`), /Metric 'WithDest.*' is not registered for source 'Dest.*' and destination ''/);
		assert.throws(() => stats.addTick(`WithDest${requestKey}`, `Src${requestKey}`, `Dest${requestKey}`), /Metric 'WithDest.*' is not registered for source 'Src.*' and destination 'Dest.*'/);
		assert.throws(() => stats.addTick(`WithSrdAndDest${requestKey}`), /Metric 'WithSrdAndDest.*' is not registered for source '' and destination ''/);
		assert.throws(() => stats.addTick(`WithSrdAndDest${requestKey}`, null, `Dest${requestKey}`), /Metric 'WithSrdAndDest.*' is not registered for source '' and destination 'Dest.*'/);
		assert.throws(() => stats.addTick(`WithSrdAndDest${requestKey}`, `Src${requestKey}`), /Metric 'WithSrdAndDest.*' is not registered for source 'Src.*' and destination ''/);
		assert.throws(() => stats.addTick(`WithSrdAndDest${requestKey}`, `Dest${requestKey}`, `Src${requestKey}`), /Metric 'WithSrdAndDest.*' is not registered for source 'Dest.*' and destination 'Src.*'/);

		stats.register(`Stats${requestKey}`, 'stats', 'Count');
		stats.register(`StatsWithSrc${requestKey}`, 'stats', 'Count', `Src${requestKey}`);
		stats.register(`StatsWithDest${requestKey}`, 'stats', 'Count', null, `Dest${requestKey}`);
		stats.register(`StatsWithSrdAndDest${requestKey}`, 'stats', 'Count', `Src${requestKey}`, `Dest${requestKey}`);
		assert.throws(() => stats.addTick(`Stats${requestKey}`), /Wrong metrics type for metrics 'Stats.*' with source '' and destination '', expecting 'counter' and found 'stats'/);
		assert.throws(() => stats.addTick(`StatsWithSrc${requestKey}`, `Src${requestKey}`), /Wrong metrics type for metrics 'Stats.*' with source 'Src.*' and destination '', expecting 'counter' and found 'stats'/);
		assert.throws(() => stats.addTick(`StatsWithDest${requestKey}`, null, `Dest${requestKey}`), /Wrong metrics type for metrics 'Stats.*' with source '' and destination 'Dest.*', expecting 'counter' and found 'stats'/);
		assert.throws(() => stats.addTick(`StatsWithSrdAndDest${requestKey}`, `Src${requestKey}`, `Dest${requestKey}`), /Wrong metrics type for metrics 'Stats.*' with source 'Src.*' and destination 'Dest.*', expecting 'counter' and found 'stats'/);
	});

	it('publish stats errors', () => {
		const requestKey = uuid();
		const stats = statistics.create();

		assert.throws(() => stats.addValue(`TopLevel${requestKey}`, 5), /Metric 'TopLevel.*' is not registered for source '' and destination ''/);
		assert.throws(() => stats.addValue(`WithSrc${requestKey}`, 5, `Src${requestKey}`), /Metric 'WithSrc.*' is not registered for source 'Src.*' and destination ''/);
		assert.throws(() => stats.addValue(`WithDest${requestKey}`, 5, null, `Dest${requestKey}`), /Metric 'WithDest.*' is not registered for source '' and destination 'Dest.*'/);
		assert.throws(() => stats.addValue(`WithSrdAndDest${requestKey}`, 5, `Src${requestKey}`, `Dest${requestKey}`), /Metric 'WithSrdAndDest.*' is not registered for source 'Src.*' and destination 'Dest.*'/);

		stats.register(`TopLevel${requestKey}`, 'stats', 'Count');
		stats.register(`WithSrc${requestKey}`, 'stats', 'Count', `Src${requestKey}`);
		stats.register(`WithDest${requestKey}`, 'stats', 'Count', null, `Dest${requestKey}`);
		stats.register(`WithSrdAndDest${requestKey}`, 'stats', 'Count', `Src${requestKey}`, `Dest${requestKey}`);

		assert.throws(() => stats.addValue(`TopLevel${requestKey}`, 5, `Src${requestKey}`), /Metric 'TopLevel.*' is not registered for source 'Src.*' and destination ''/);
		assert.throws(() => stats.addValue(`TopLevel${requestKey}`, 5, null, `Dest${requestKey}`), /Metric 'TopLevel.*' is not registered for source '' and destination 'Dest.*'/);
		assert.throws(() => stats.addValue(`TopLevel${requestKey}`, 5, `Src${requestKey}`, `Dest${requestKey}`), /Metric 'TopLevel.*' is not registered for source 'Src.*' and destination 'Dest.*'/);
		assert.throws(() => stats.addValue(`WithSrc${requestKey}`, 5), /Metric 'WithSrc.*' is not registered for source '' and destination ''/);
		assert.throws(() => stats.addValue(`WithSrc${requestKey}`, 5, null, `Dest${requestKey}`), /Metric 'WithSrc.*' is not registered for source '' and destination 'Dest.*'/);
		assert.throws(() => stats.addValue(`WithSrc${requestKey}`, 5, null, `Src${requestKey}`), /Metric 'WithSrc.*' is not registered for source '' and destination 'Src.*'/);
		assert.throws(() => stats.addValue(`WithSrc${requestKey}`, 5, `Src${requestKey}`, `Dest${requestKey}`), /Metric 'WithSrc.*' is not registered for source 'Src.*' and destination 'Dest.*'/);
		assert.throws(() => stats.addValue(`WithDest${requestKey}`, 5), /Metric 'WithDest.*' is not registered for source '' and destination ''/);
		assert.throws(() => stats.addValue(`WithDest${requestKey}`, 5, `Src${requestKey}`), /Metric 'WithDest.*' is not registered for source 'Src.*' and destination ''/);
		assert.throws(() => stats.addValue(`WithDest${requestKey}`, 5, `Dest${requestKey}`), /Metric 'WithDest.*' is not registered for source 'Dest.*' and destination ''/);
		assert.throws(() => stats.addValue(`WithDest${requestKey}`, 5, `Src${requestKey}`, `Dest${requestKey}`), /Metric 'WithDest.*' is not registered for source 'Src.*' and destination 'Dest.*'/);
		assert.throws(() => stats.addValue(`WithSrdAndDest${requestKey}`, 5), /Metric 'WithSrdAndDest.*' is not registered for source '' and destination ''/);
		assert.throws(() => stats.addValue(`WithSrdAndDest${requestKey}`, 5, null, `Dest${requestKey}`), /Metric 'WithSrdAndDest.*' is not registered for source '' and destination 'Dest.*'/);
		assert.throws(() => stats.addValue(`WithSrdAndDest${requestKey}`, 5, `Src${requestKey}`), /Metric 'WithSrdAndDest.*' is not registered for source 'Src.*' and destination ''/);
		assert.throws(() => stats.addValue(`WithSrdAndDest${requestKey}`, 5, `Dest${requestKey}`, `Src${requestKey}`), /Metric 'WithSrdAndDest.*' is not registered for source 'Dest.*' and destination 'Src.*'/);

		stats.register(`Counter${requestKey}`, 'counter', 'Count');
		stats.register(`CounterWithSrc${requestKey}`, 'counter', 'Count', `Src${requestKey}`);
		stats.register(`CounterWithDest${requestKey}`, 'counter', 'Count', null, `Dest${requestKey}`);
		stats.register(`CounterWithSrdAndDest${requestKey}`, 'counter', 'Count', `Src${requestKey}`, `Dest${requestKey}`);
		assert.throws(() => stats.addValue(`Counter${requestKey}`, 5), /Wrong metrics type for metrics 'Counter.*' with source '' and destination '', expecting 'stats' and found 'counter'/);
		assert.throws(() => stats.addValue(`CounterWithSrc${requestKey}`, 5, `Src${requestKey}`), /Wrong metrics type for metrics 'Counter.*' with source 'Src.*' and destination '', expecting 'stats' and found 'counter'/);
		assert.throws(() => stats.addValue(`CounterWithDest${requestKey}`, 5, null, `Dest${requestKey}`), /Wrong metrics type for metrics 'Counter.*' with source '' and destination 'Dest.*', expecting 'stats' and found 'counter'/);
		assert.throws(() => stats.addValue(`CounterWithSrdAndDest${requestKey}`, 5, `Src${requestKey}`, `Dest${requestKey}`), /Wrong metrics type for metrics 'Counter.*' with source 'Src.*' and destination 'Dest.*', expecting 'stats' and found 'counter'/);

		assert.throws(() => stats.addValue(`TopLevel${requestKey}`, NaN), /Metric 'TopLevel.*' only accepts valid numbers as parameters/);
		assert.throws(() => stats.addValue(`TopLevel${requestKey}`, +Infinity), /Metric 'TopLevel.*' only accepts valid numbers as parameters/);
		assert.throws(() => stats.addValue(`TopLevel${requestKey}`, -Infinity), /Metric 'TopLevel.*' only accepts valid numbers as parameters/);
		assert.throws(() => stats.addValue(`TopLevel${requestKey}`, "10"), /Metric 'TopLevel.*' only accepts valid numbers as parameters/);
	});

	it('publish counter', () => {
		const requestKey = uuid();
		const stats = statistics.create();

		processRequest = (params) => {
			assert.strictEqual(params.Namespace, 'Custom/FanOut');
			assert.strictEqual(params.MetricData.length, 1);
			assert.strictEqual(params.MetricData[0].MetricName, `TopLevel${requestKey}`);
			assert.deepEqual(params.MetricData[0].Dimensions, [ { Name: 'Function', Value: 'fanOut' }]);
			assert.strictEqual(params.MetricData[0].Unit, 'Count');
			assert.strictEqual(params.MetricData[0].Value, 3);
		};

		stats.register(`TopLevel${requestKey}`, 'counter', 'Count');
		stats.addTick(`TopLevel${requestKey}`);
		stats.addTick(`TopLevel${requestKey}`);
		stats.addTick(`TopLevel${requestKey}`);

		return stats.publish();
	});

	it('publish counter in custom namespace', () => {
		const requestKey = uuid();
		const stats = statistics.create({ cloudWatchNamespace: 'test' });

		processRequest = (params) => {
			assert.strictEqual(params.Namespace, 'test');
			assert.strictEqual(params.MetricData.length, 1);
			assert.strictEqual(params.MetricData[0].MetricName, `TopLevel${requestKey}`);
			assert.deepEqual(params.MetricData[0].Dimensions, [ { Name: 'Function', Value: 'fanOut' }]);
			assert.strictEqual(params.MetricData[0].Unit, 'Count');
			assert.strictEqual(params.MetricData[0].Value, 5);
		};

		stats.register(`TopLevel${requestKey}`, 'counter', 'Count');
		stats.addTick(`TopLevel${requestKey}`);
		stats.addTick(`TopLevel${requestKey}`);
		stats.addTick(`TopLevel${requestKey}`);
		stats.addTick(`TopLevel${requestKey}`);
		stats.addTick(`TopLevel${requestKey}`);

		return stats.publish();
	});

	it('publish counter with source', () => {
		const requestKey = uuid();
		const stats = statistics.create();

		processRequest = (params) => {
			assert.strictEqual(params.Namespace, 'Custom/FanOut');
			assert.strictEqual(params.MetricData.length, 1);
			assert.strictEqual(params.MetricData[0].MetricName, `TopLevel${requestKey}`);
			assert.deepEqual(params.MetricData[0].Dimensions, [ { Name: 'Source', Value: `Src${requestKey}` }, { Name: 'Function', Value: 'fanOut' } ]);
			assert.strictEqual(params.MetricData[0].Unit, 'Count');
			assert.strictEqual(params.MetricData[0].Value, 4);
		};

		stats.register(`TopLevel${requestKey}`, 'counter', 'Count', `Src${requestKey}`);
		stats.addTick(`TopLevel${requestKey}`, `Src${requestKey}`);
		stats.addTick(`TopLevel${requestKey}`, `Src${requestKey}`);
		stats.addTick(`TopLevel${requestKey}`, `Src${requestKey}`);
		stats.addTick(`TopLevel${requestKey}`, `Src${requestKey}`);

		return stats.publish();
	});

	it('publish counter with destination', () => {
		const requestKey = uuid();
		const stats = statistics.create();

		processRequest = (params) => {
			assert.strictEqual(params.Namespace, 'Custom/FanOut');
			assert.strictEqual(params.MetricData.length, 1);
			assert.strictEqual(params.MetricData[0].MetricName, `TopLevel${requestKey}`);
			assert.deepEqual(params.MetricData[0].Dimensions, [ { Name: 'Destination', Value: `Dest${requestKey}` }, { Name: 'Function', Value: 'fanOut' } ]);
			assert.strictEqual(params.MetricData[0].Unit, 'Count');
			assert.strictEqual(params.MetricData[0].Value, 2);
		};

		stats.register(`TopLevel${requestKey}`, 'counter', 'Count', null, `Dest${requestKey}`);
		stats.addTick(`TopLevel${requestKey}`, null, `Dest${requestKey}`);
		stats.addTick(`TopLevel${requestKey}`, null, `Dest${requestKey}`);

		return stats.publish();
	});

	it('publish counter with source and destination', () => {
		const requestKey = uuid();
		const stats = statistics.create();

		processRequest = (params) => {
			assert.strictEqual(params.Namespace, 'Custom/FanOut');
			assert.strictEqual(params.MetricData.length, 1);
			assert.strictEqual(params.MetricData[0].MetricName, `TopLevel${requestKey}`);
			assert.deepEqual(params.MetricData[0].Dimensions, [ { Name: 'Source', Value: `Src${requestKey}` }, { Name: 'Destination', Value: `Dest${requestKey}` }, { Name: 'Function', Value: 'fanOut' } ]);
			assert.strictEqual(params.MetricData[0].Unit, 'Count');
			assert.strictEqual(params.MetricData[0].Value, 1);
		};

		stats.register(`TopLevel${requestKey}`, 'counter', 'Count', `Src${requestKey}`, `Dest${requestKey}`);
		stats.addTick(`TopLevel${requestKey}`, `Src${requestKey}`, `Dest${requestKey}`);

		return stats.publish();
	});

	it('publish stats', () => {
		const requestKey = uuid();
		const stats = statistics.create();

		processRequest = (params) => {
			assert.strictEqual(params.Namespace, 'Custom/FanOut');
			assert.strictEqual(params.MetricData.length, 1);
			assert.strictEqual(params.MetricData[0].MetricName, `TopLevel${requestKey}`);
			assert.deepEqual(params.MetricData[0].Dimensions, [ { Name: 'Function', Value: 'fanOut' }]);
			assert.strictEqual(params.MetricData[0].Unit, 'Count');
			assert.strictEqual(params.MetricData[0].StatisticValues.SampleCount, 3);
			assert.strictEqual(params.MetricData[0].StatisticValues.Min, 1);
			assert.strictEqual(params.MetricData[0].StatisticValues.Max, 3);
			assert.strictEqual(params.MetricData[0].StatisticValues.Sum, 6);
		};

		stats.register(`TopLevel${requestKey}`, 'stats', 'Count');
		stats.addValue(`TopLevel${requestKey}`, 1);
		stats.addValue(`TopLevel${requestKey}`, 2);
		stats.addValue(`TopLevel${requestKey}`, 3);

		return stats.publish();
	});

	it('publish stats in custom namespace', () => {
		const requestKey = uuid();
		const stats = statistics.create({ cloudWatchNamespace: 'test' });

		processRequest = (params) => {
			assert.strictEqual(params.Namespace, 'test');
			assert.strictEqual(params.MetricData.length, 1);
			assert.strictEqual(params.MetricData[0].MetricName, `TopLevel${requestKey}`);
			assert.deepEqual(params.MetricData[0].Dimensions, [ { Name: 'Function', Value: 'fanOut' }]);
			assert.strictEqual(params.MetricData[0].Unit, 'Count');
			assert.strictEqual(params.MetricData[0].StatisticValues.SampleCount, 5);
			assert.strictEqual(params.MetricData[0].StatisticValues.Min, -100);
			assert.strictEqual(params.MetricData[0].StatisticValues.Max, 1000);
			assert.strictEqual(params.MetricData[0].StatisticValues.Sum, 914);
		};

		stats.register(`TopLevel${requestKey}`, 'stats', 'Count');
		stats.addValue(`TopLevel${requestKey}`, 1);
		stats.addValue(`TopLevel${requestKey}`, -100);
		stats.addValue(`TopLevel${requestKey}`, 1000);
		stats.addValue(`TopLevel${requestKey}`, 10);
		stats.addValue(`TopLevel${requestKey}`, 3);

		return stats.publish();
	});

	it('publish stats with source', () => {
		const requestKey = uuid();
		const stats = statistics.create();

		processRequest = (params) => {
			assert.strictEqual(params.Namespace, 'Custom/FanOut');
			assert.strictEqual(params.MetricData.length, 1);
			assert.strictEqual(params.MetricData[0].MetricName, `TopLevel${requestKey}`);
			assert.deepEqual(params.MetricData[0].Dimensions, [ { Name: 'Source', Value: `Src${requestKey}` }, { Name: 'Function', Value: 'fanOut' } ]);
			assert.strictEqual(params.MetricData[0].Unit, 'Count');
			assert.strictEqual(params.MetricData[0].StatisticValues.SampleCount, 4);
			assert.strictEqual(params.MetricData[0].StatisticValues.Min, -10);
			assert.strictEqual(params.MetricData[0].StatisticValues.Max, 50);
			assert.strictEqual(params.MetricData[0].StatisticValues.Sum, 65);
		};

		stats.register(`TopLevel${requestKey}`, 'stats', 'Count', `Src${requestKey}`);
		stats.addValue(`TopLevel${requestKey}`, -10, `Src${requestKey}`);
		stats.addValue(`TopLevel${requestKey}`, 5, `Src${requestKey}`);
		stats.addValue(`TopLevel${requestKey}`, 20, `Src${requestKey}`);
		stats.addValue(`TopLevel${requestKey}`, 50, `Src${requestKey}`);

		return stats.publish();
	});

	it('publish stats with destination', () => {
		const requestKey = uuid();
		const stats = statistics.create();

		processRequest = (params) => {
			assert.strictEqual(params.Namespace, 'Custom/FanOut');
			assert.strictEqual(params.MetricData.length, 1);
			assert.strictEqual(params.MetricData[0].MetricName, `TopLevel${requestKey}`);
			assert.deepEqual(params.MetricData[0].Dimensions, [ { Name: 'Destination', Value: `Dest${requestKey}` }, { Name: 'Function', Value: 'fanOut' } ]);
			assert.strictEqual(params.MetricData[0].Unit, 'Count');
			assert.strictEqual(params.MetricData[0].StatisticValues.SampleCount, 2);
			assert.strictEqual(params.MetricData[0].StatisticValues.Min, 4);
			assert.strictEqual(params.MetricData[0].StatisticValues.Max, 6);
			assert.strictEqual(params.MetricData[0].StatisticValues.Sum, 10);
		};

		stats.register(`TopLevel${requestKey}`, 'stats', 'Count', null, `Dest${requestKey}`);
		stats.addValue(`TopLevel${requestKey}`, 4, null, `Dest${requestKey}`);
		stats.addValue(`TopLevel${requestKey}`, 6, null, `Dest${requestKey}`);

		return stats.publish();
	});

	it('publish stats with source and destination', () => {
		const requestKey = uuid();
		const stats = statistics.create();

		processRequest = (params) => {
			assert.strictEqual(params.Namespace, 'Custom/FanOut');
			assert.strictEqual(params.MetricData.length, 1);
			assert.strictEqual(params.MetricData[0].MetricName, `TopLevel${requestKey}`);
			assert.deepEqual(params.MetricData[0].Dimensions, [ { Name: 'Source', Value: `Src${requestKey}` }, { Name: 'Destination', Value: `Dest${requestKey}` }, { Name: 'Function', Value: 'fanOut' } ]);
			assert.strictEqual(params.MetricData[0].Unit, 'Count');
			assert.strictEqual(params.MetricData[0].StatisticValues.SampleCount, 1);
			assert.strictEqual(params.MetricData[0].StatisticValues.Min, 5);
			assert.strictEqual(params.MetricData[0].StatisticValues.Max, 5);
			assert.strictEqual(params.MetricData[0].StatisticValues.Sum, 5);
		};

		stats.register(`TopLevel${requestKey}`, 'stats', 'Count', `Src${requestKey}`, `Dest${requestKey}`);
		stats.addValue(`TopLevel${requestKey}`, 5, `Src${requestKey}`, `Dest${requestKey}`);

		return stats.publish();
	});

	it('publish over 20 metrics (multiple calls)', () => {
		const requestKey = uuid();
		const stats = statistics.create();
		let   count = 0;
		const total = 55;
		const metrics = {};

		processRequest = (params) => {
			assert.strictEqual(params.Namespace, 'Custom/FanOut');
			const length = (total - count) > 20 ? 20 : (total - count);
			assert.strictEqual(params.MetricData.length, length);
			for(let i = 0; i < length; ++i) {
				metrics[params.MetricData[i].MetricName] = params.MetricData[i];
			}
			count += length;

			if(count == total) {
				for(let i = 1; i <= total; ++i) {
					const metric = metrics[`Metric.${i}.${requestKey}`];
					assert.strictEqual(metric.MetricName, `Metric.${i}.${requestKey}`);
					assert.deepEqual(metric.Dimensions, [ { Name: 'Function', Value: 'fanOut' } ]);
					assert.strictEqual(metric.Unit, 'Bytes');
					assert.strictEqual(metric.StatisticValues.SampleCount, i);
					assert.strictEqual(metric.StatisticValues.Min, i);
					assert.strictEqual(metric.StatisticValues.Max, i);
					assert.strictEqual(metric.StatisticValues.Sum, i*i);
				}
			}
		};

		for(let i = 1; i <= total; ++i) {
			stats.register(`Metric.${i}.${requestKey}`, 'stats', 'Bytes');
			for(let j = 0; j < i; ++j) {
				stats.addValue(`Metric.${i}.${requestKey}`, i);
			}
		}

		return stats.publish();
	});
});
