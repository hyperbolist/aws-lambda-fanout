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
 * This Node.js library processes messages and forwards them to Amazon Kinesis Firehose.
 */
'use strict';

// Modules
let AWS = require('aws-sdk');

// Default values
const config = {
	service: 'Firehose',
	debug  : false
};

module.exports.configure = (values) => {
	if((values !== null) && (values !== undefined) && (typeof values == "object")) {
		for(let key in values) {
			config[key] = values[key];
		}
	}
	return config;
};

// Defines the AWS SDK Instance
module.exports.setAWS = (aws) => {
	AWS = aws;
};

// Limits for message publication
module.exports.limits = {
	maxRecords: 500,        // Amazon Kinesis Firehose PutRecords only accepts up to 500 messages in a single call
	maxSize: 4*1024*1024,   // Amazon Kinesis Firehose PutRecords only accepts up to 4MiB in a single call
	maxUnitSize: 1024*1024, // Amazon Kinesis Firehose PutRecords only accepts up to 1MiB per message
	includeKey: false,      // Records will not include the key
	listOverhead: 0,        // Native Amazon Kinesis Firehose call, no specific limits
	recordOverhead: 0,      // Native Amazon Kinesis Firehose call, no specific limits
	interRecordOverhead: 0  // Native Amazon Kinesis Firehose call, no specific limits
};

// This function is used to validate the destination in a target. This is used by the configuration
module.exports.destinationRegex = /^([a-zA-Z0-9_-]{1,64})$/;

module.exports.targetSettings = (target) => {
	target.collapse = "API";
};

//********
// This function creates an instance of an Amazon Kinesis Firehose service
module.exports.create = function(target, options) {
	var service = new AWS.Firehose(options);
	if(config.debug) {
		console.log("Created new AWS.Firehose service instance");
	}
	return service;
};

//********
// This function sends messages to Amazon Kinesis Firehose
module.exports.send = function(service, target, records, callback) {
	var entries = records.map(function(record) { return { Data: record.data }; });
	service.putRecordBatch({ DeliveryStreamName: target.destination, Records: entries }, callback);
};