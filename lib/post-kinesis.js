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
 * This Node.js library processes messages and forwards them to Amazon Kinesis Streams.
 */
'use strict';

// Modules
let   AWS  = require('aws-sdk');
const util = require('./util.js');

// Default values
const config = {
	service: 'Kinesis',
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
	maxRecords: 500,        // Amazon Kinesis PutRecords only accepts up to 500 messages in a single call
	maxSize: 5*1024*1024,   // Amazon Kinesis PutRecords only accepts up to 5MiB in a single call
	maxUnitSize: 1024*1024, // Amazon Kinesis PutRecords only accepts up to 1MiB per message
	includeKey: true,       // Records will include the key
	listOverhead: 0,        // Native Amazon Kinesis call, no specific limits
	recordOverhead: 0,      // Native Amazon Kinesis call, no specific limits
	interRecordOverhead: 0  // Native Amazon Kinesis call, no specific limits
};

// This function is used to validate the destination in a target. This is used by the configuration
exports.destinationRegex = /^([a-zA-Z0-9_-]{1,128})$/;

exports.targetSettings = (target) => {
	if(target.collapse != "none") {
		console.error(`Ignoring parameter 'collapse' for target '${target.id}' of type '${target.type}'`);
	}
	target.collapse = "API";
};

//********
// This function creates an instance of an Amazon Kinesis service
exports.create = function(target, options) {
	const serviceOptions = {};
	if(options) {
		util.applyChanges(serviceOptions, options.serviceOptions);
	}
	const service = {
		instance: new AWS.Kinesis(serviceOptions),
		debug: config.debug
	};
	util.applyChanges(service, options);
	/* istanbul ignore next */
	if(service.debug) {
		console.log("Created new AWS.Kinesis service instance");
	}
	return service;
};

//********
// This function sends messages to Amazon Kinesis
exports.send = function(service, target, records) {
	var entries = records.map((record) => { return { PartitionKey: record.key, Data: record.data }; });
	return service.instance.putRecords({ StreamName: target.destination, Records: entries }).promise();
};
