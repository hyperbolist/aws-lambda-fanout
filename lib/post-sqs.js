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
 * This Node.js library processes messages and forwards them to Amazon SQS.
 */
'use strict';

// Modules
let   AWS  = require('aws-sdk');
const util = require('./util.js');

// Default values
const config = {
	service: 'SQS',
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
	maxSize: 256*1024,             // Amazon SQS only accepts up to 256KiB per message
	maxUnitSize: 256*1024,         // Amazon SQS only accepts up to 256KiB per message
	includeKey: false,             // Records will not include the key
	listOverhead: 14,              // Records are put in a JSON object "{"Records":[]}"
	recordOverhead: 0,             // Records are just serialized
	interRecordOverhead: 1         // Records are comma separated
};

// This function is used to validate the destination in a target. This is used by the configuration
exports.destinationRegex = /^https:\/\/((queue)|(sqs\.[a-z]+-[a-z]+-[0-9]))\.amazonaws\.com\/[0-9]{12}\/[a-zA-Z0-9_-]{1,80}$/;

exports.targetSettings = () => {
};

//********
// This function creates an instance of an Amazon SQS service
exports.create = function(target, options) {
	var service = new AWS.SQS(options);
	if(config.debug) {
		console.log("Created new AWS.SQS service instance");
	}
	return service;
};

//********
// This function sends messages to Amazon SQS
exports.send = function(service, target, records, callback) {
	switch(target.collapse) {
		case "JSON": {
			// We have multiple messages, collapse them in a single JSON Array
			var entries = { Records: records.map((record) => JSON.parse(record.data.toString())) };
			service.sendMessage({ QueueUrl: target.destination, MessageBody: JSON.stringify(entries) }, callback);
			break;
		}
		case "concat-b64": {
			// We have multiple messages, collapse them in a single buffer
			var data = Buffer.concat(records.map((record) => record.data));
			service.sendMessage({ QueueUrl: target.destination, MessageBody: data.toString('base64') }, callback);
			break;
		}
		case "concat": {
			// We have multiple messages, collapse them in a single buffer
			var message = records.map((record) => record.data.toString()).join('');
			service.sendMessage({ QueueUrl: target.destination, MessageBody: message }, callback);
			break;
		}
		default: {
			// We have a single message, let's send it
			service.sendMessage({ QueueUrl: target.destination, MessageBody: records[0].data.toString() }, callback);
		}
	}
};
