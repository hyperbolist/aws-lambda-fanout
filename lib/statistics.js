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
 * This Node.js script manages the statistics and their publication to Amazon CloudWatch.
 */
'use strict';

// Modules
let   AWS  = require('aws-sdk');
const util = require('./util.js');

// Runtime variables
let cloudWatch = new AWS.CloudWatch();

// Defines the AWS SDK Instance
module.exports.setAWS = (aws) => {
	AWS = aws;
	cloudWatch = new AWS.CloudWatch();
};

const unitRegexp = /^None|Percent|Seconds|(Micro|Milli)seconds|(Count|Bytes|Bits|(Kilo|Mega|Giga|Tera)(bytes|bits)(\/Seconds)?)$/;

module.exports.create = (options) => {
	const config = {
		cloudWatchNamespace: 'Custom/FanOut',
		functionName: util.getEnvString('AWS_LAMBDA_FUNCTION_NAME', 'fanOut'),
		debug: false
	};
	util.applyChanges(config, options);
	if((config.cloudWatchNamespace.length == 0) || config.cloudWatchNamespace.startsWith(':') || config.cloudWatchNamespace.startsWith('AWS/')) {
		throw new Error(`Invalid namespace '${config.cloudWatchNamespace}', must be a non-empty string and must not start with 'AWS/' or ':'`);
	}

	const metrics = {};

	const register = (name, type, unit, source, destination) => {
		if ((typeof name != "string") || (name.length == 0) || (name.length > 255)) {
			throw new Error(`Invalid name '${name}', must be a string between 1 and 255 characters`);
		}
		if((source == null) || (source == undefined)) {
			source = '';
		}
		if ((typeof source != "string") || (source.length > 255)) {
			throw new Error(`Invalid source '${source}', must be a string with at most 255 characters`);
		}
		if((destination == null) || (destination == undefined)) {
			destination = '';
		}
		if ((typeof destination != "string") || (destination.length > 255)) {
			throw new Error(`Invalid destination '${destination}', must be a string with at most 255 characters`);
		}

		const entryName = `${name}#${source}#${destination}`;
		if (metrics.hasOwnProperty(entryName)) {
			throw new Error(`Metric '${name}' is already registered for source '${source}' and destination '${destination}'`);
		}
		let dimensions = null;
		if (source.length > 0) {
			if (destination.length > 0) {
				// Aggregation per source, destination and function
				dimensions = [{ Name: 'Source', Value: source }, { Name: 'Destination', Value: destination }, { Name: 'Function', Value: config.functionName }];
			} else {
				// Aggregation per source and function
				dimensions = [{ Name: 'Source', Value: source }, { Name: 'Function', Value: config.functionName }];
			}
		} else if (destination.length > 0) {
			// Aggregation per destination and function
			dimensions = [{ Name: 'Destination', Value: destination }, { Name: 'Function', Value: config.functionName }];
		} else {
			// Aggregation per function
			dimensions = [{ Name: 'Function', Value: config.functionName }];
		}

		if ((typeof unit != "string") || (! unitRegexp.test(unit))) {
			throw new Error(`Invalid unit specified '${unit}', allowed values are (Seconds | Microseconds | Milliseconds | Bytes | Kilobytes | Megabytes | Gigabytes | Terabytes | Bits | Kilobits | Megabits | Gigabits | Terabits | Percent | Count | Bytes/Second | Kilobytes/Second | Megabytes/Second | Gigabytes/Second | Terabytes/Second | Bits/Second | Kilobits/Second | Megabits/Second | Gigabits/Second | Terabits/Second | Count/Second | None)`);
		}

		switch(`${type}`) {
			case 'counter': {
				metrics[entryName] = { name: name, unit: unit, dimensions: dimensions, type: 'counter', value: 0 };
				break;
			}
			case 'stats': {
				metrics[entryName] = { name: name, unit: unit, dimensions: dimensions, type: 'stats', value: { Minimum: 0, Maximum: 0, SampleCount: 0, Sum: 0 } };
				break;
			}
			default: {
				throw new Error(`Invalid metric type '${type}', allowed values are 'counter' and 'stats'`);
			}
		}
	};

	const publish = function() {
		const time = new Date();
		let cwMetrics =  Object.keys(metrics).map((key) => {
			const metric = metrics[key];
			switch(metric.type) {
				case 'counter': {
					return { MetricName: metric.name, Dimensions: metric.dimensions, Unit: metric.unit, Timestamp: time, Value: metric.value };
				}
				case 'stats': {
					return { MetricName: metric.name, Dimensions: metric.dimensions, Unit: metric.unit, Timestamp: time, StatisticValues: metric.value };
				}
			}
		});

		const _publish = () => {
			let toSend = null;
			if(cwMetrics.length == 0) {
				/* istanbul ignore next */
				if(config.debug) {
					console.log('Metrics published to Amazon CloudWatch');
				}
				// Don't call CloudWatch is we have no metrics in this namespace
				return Promise.resolve(null);
			} else if(cwMetrics.length > 20) {
				// Cloudwatch only accepts up to 20 
				toSend = cwMetrics.slice(0, 20);
				cwMetrics = cwMetrics.slice(20);
			} else {
				toSend = cwMetrics;
				cwMetrics = [];
			}

			return cloudWatch.putMetricData({ Namespace: config.cloudWatchNamespace, MetricData: toSend }).promise().then(() => {
				return _publish();
			});
		};
		return _publish();
	};

	const addTick = (name, source, destination) => {
		const entryName = `${name}#${source || ''}#${destination || ''}`;
		if(! metrics.hasOwnProperty(entryName)) {
			throw new Error(`Metric '${name}' is not registered for source '${source || ''}' and destination '${destination || ''}'`);
		}
		if(metrics[entryName].type != 'counter') {
			throw new Error(`Wrong metrics type for metrics '${name}' with source '${source || ''}' and destination '${destination || ''}', expecting 'counter' and found '${metrics[entryName].type}'`);
		}
		metrics[entryName].value += 1;
	};

	const addValue = (name, value, source, destination) => {
		const entryName = `${name}#${source || ''}#${destination || ''}`;
		if(! metrics.hasOwnProperty(entryName)) {
			throw new Error(`Metric '${name}' is not registered for source '${source || ''}' and destination '${destination || ''}'`);
		}
		if(typeof value != 'number' || isNaN(value) || (value == Infinity) || (value == -Infinity)) {
			throw new Error(`Metric '${name}' only accepts valid numbers as parameters`);
		}
		if(metrics[entryName].type != 'stats') {
			throw new Error(`Wrong metrics type for metrics '${name}' with source '${source || ''}' and destination '${destination || ''}', expecting 'stats' and found '${metrics[entryName].type}'`);
		}

		const entry = metrics[entryName].value;
		if(entry.SampleCount === 0) {
			entry.Min = value;
			entry.Max = value;
		} else {
			if(entry.Min > value) {
				entry.Min = value;
			}
			if(entry.Max < value) {
				entry.Max = value;
			}
		}
		entry.Sum += value;
		entry.SampleCount++;
	};

	return {
		register: register,
		addTick: addTick,
		addValue: addValue,
		publish: publish
	};
};
