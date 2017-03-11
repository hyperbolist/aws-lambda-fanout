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

// Default values
const config = {
	cloudWatchNamespace: "Custom/FanOut", // Namespace for publishing metrics to CloudWatch
	debug  : false
};

module.exports.configure = (values) => {
	return util.applyChanges(config, values);
};

// Runtime variables
let cloudWatch = new AWS.CloudWatch(); // CloudWatch service for loading the configuration, in the current region

// Defines the AWS SDK Instance
module.exports.setAWS = (aws) => {
	AWS = aws;
	cloudWatch = new AWS.CloudWatch();
};

exports.create = function() {
	var metrics = {};
	var entries = {};

	var register = (name, displayName, type, unit, source, destination) => {
		if (metrics.hasOwnProperty(name)) {
			return;
		}

		var dimensions = [];

		if (source) {
			if (destination) {
				// Aggregation per source, destination and function
				dimensions = [{ Name: "Source", Value: source }, { Name: "Destination", Value: source }, { Name: "Function", Value: process.env.AWS_LAMBDA_FUNCTION_NAME }];
			} else {
				// Aggregation per source and function
				dimensions = [{ Name: "Source", Value: source }, { Name: "Function", Value: process.env.AWS_LAMBDA_FUNCTION_NAME }];
			}
		} else if (destination) {
			// Aggregation per destination and function
			dimensions = [{ Name: "Destination", Value: source }, { Name: "Function", Value: process.env.AWS_LAMBDA_FUNCTION_NAME }];
		} else {
			// Aggregation per function
			dimensions = [{ Name: "Function", Value: process.env.AWS_LAMBDA_FUNCTION_NAME }];
		}

		if(type == "counter") {
			metrics[name] = { name: displayName, unit: unit, dimensions: dimensions, type: type };
			entries[name] = 0;
		}
		else if(type == "stats") {
			metrics[name] = { name: displayName, unit: unit, dimensions: dimensions, type: type };
			entries[name] = { Minimum: 0, Maximum: 0, SampleCount: 0, Sum: 0 };
		}
		else {
			console.error("Invalid metric type '" + type + "', allowed values are 'counter' and 'stats'");
		}
	};

	var publish = function(callback) {
		var time = new Date();
		var cwMetrics = [];
		Object.keys(metrics).forEach(function(key) {
			const metric = metrics[key];
			if(metric.type == 'counter') {
				cwMetrics.push({ MetricName: metric.name, Dimensions: metric.dimensions, Unit: metric.unit, Timestamp: time, Value: entries[key] });
			} else if(metric.type == 'stats') {
				cwMetrics.push({ MetricName: metric.name, Dimensions: metric.dimensions, Unit: metric.unit, Timestamp: time, StatisticValues: entries[key] });
			}
		});

		var _publish = () => {
			var toSend = null;
			if(cwMetrics.length == 0) {
				if(config.debug) {
					console.log("Metrics published to Amazon CloudWatch");
				}
				// Don't call CloudWatch is we have no metrics in this namespace
				callback(null);
				return;
			} else if(cwMetrics.length > 20) {
				// Cloudwatch only accepts up to 20 
				toSend = cwMetrics.slice(0, 20);
				cwMetrics = cwMetrics.slice(20);
			} else {
				toSend = cwMetrics;
			}

			cloudWatch.putMetricData({ Namespace: config.cloudWatchNamespace, MetricData: toSend }, function(err) {
				if(err) {
					console.error("Error pushing metrics to AWS CloudWatch: ", err);
					callback(null);
				} else {
					_publish();
				}
			});
		};
		_publish();
	};

	var addTick = (name) => {
		if(! entries.hasOwnProperty(name)) {
			console.error("Metric not registered: '" + name + "'");
			return;
		}
		if(metrics[name].type != 'counter') {
			console.error("Wrong metrics type for metrics: '" + name + "', expexting 'counter' and found '" + metrics[name].type + "'");
			return;
		}
		entries[name] = entries[name] + 1;
	};

	var addValue = (name, value) => {
		if(! entries.hasOwnProperty(name)) {
			console.error("Metric not registered: '" + name + "'");
			return;
		}
		if(metrics[name].type != 'stats') {
			console.error("Wrong metrics type for metrics: '" + name + "', expexting 'stats' and found '" + metrics[name].type + "'");
			return;
		}
		var entry = entries[name];
		if(entry.SampleCount == 0) {
			entry.Min = value;
			entry.Max = value;
			entry.Sum = value;
			entry.SampleCount = 1;
		} else {
			if(entry.Min > value) {
				entry.Min = value;
			}
			if(entry.Max < value) {
				entry.Max = value;
			}
			entry.Sum += value;
			entry.SampleCount += 1;
		}
	};

	return {
		register: register,
		addTick: addTick,
		addValue: addValue,
		publish: publish
	};
};
