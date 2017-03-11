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
 * This Node.js library processes messages and forwards them to an AWS IoT MQTT Topic.
 */
'use strict';

// Modules
let   AWS  = require('aws-sdk');
const util = require('./util.js');

// Default values
const config = {
	service: 'IoT',
	debug  : false
};

module.exports.configure = (values) => {
	return util.applyChanges(config, values);
};

// Defines the AWS SDK Instance
module.exports.setAWS = (aws) => {
	AWS = aws;
};

// Limits for message publication
exports.limits = {
	maxRecords: Number.MAX_VALUE,  // No limit on number of records, we collapse them in a single value
	maxSize: 128*1024,             // AWS IoT Publish only accepts up to 128KiB in a single call
	maxUnitSize: 128*1024,         // AWS IoT Publish only accepts up to 128KiB per message
	includeKey: false,             // Records will not include the key
	listOverhead: 14,              // Records are put in a JSON object "{"Records":[]}"
	recordOverhead: 0,             // Records are just serialized
	interRecordOverhead: 1         // Records are comma separated
};

// This function is used to validate the destination in a target. This is used by the configuration
exports.destinationRegex = /^[a-zA-Z0-9-]+\.iot\.[a-z]+-[a-z]+-[0-9]\.amazonaws\.com#.*$/;

exports.targetSettings = () => {
};

//********
// This function creates an instance of an AWS IoT service
exports.create = function(target, options) {
	const index = target.destination.indexOf('#');
	const endpoint = target.destination.substr(0, index);
	const destination = target.destination.substr(index+1);

	const serviceOptions = {
		endpoint: endpoint
	};
	if(options) {
		util.applyChanges(serviceOptions, options.serviceOptions);
	}

	const service = {
		topic: destination,
		instance: new AWS.IotData(serviceOptions),
		debug: config.debug
	};
	util.applyChanges(service, options);
	/* istanbul ignore next */
	if(service.debug) {
		console.log("Created new AWS.IotData service instance");
	}
	return service;
};

//********
// This function sends messages to Amazon IoT
exports.send = function(service, target, records) {
	switch(target.collapse) {
		case "JSON": {
			// We have multiple messages, collapse them in a single JSON Array
			const entries = { Records: records.map(function(record) { return JSON.parse(record.data.toString('utf-8')); }) };
			return service.instance.publish({ topic: target.destination, payload: JSON.stringify(entries), qos: 0 }).promise();
		}
		case "concat-b64": {
			// We have multiple messages, collapse them in a single buffer
			const data = Buffer.concat(records.map((record) => record.data));
			return service.instance.publish({ topic: target.destination, payload: data.toString('base64'), qos: 0 }).promise();
		}
		case "concat": {
			let data = null;
			// We have multiple messages, collapse them in a single buffer
			if(target.separator && target.separator.length > 0) {
				data = new Buffer(records.map((record) => record.data.toString('utf-8')).join(target.separator));
			} else {
				data = Buffer.concat(records.map((record) => record.data));
			}
			return service.instance.publish({ topic: target.destination, payload: data, qos: 0 }).promise();
		}
		default: {
			if(records.length != 1) {
				return Promise.reject(new Error(`Multiple records must be collapsed, IoT supports JSON|concat-b64|concat`));
			}
			// We have a single message, let's send it
			return service.instance.publish({ topic: target.destination, payload: records[0].data, qos: 0 }).promise();
		}
	}
};
