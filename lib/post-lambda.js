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
 * This Node.js library processes messages and forwards them to AWS Lambda.
 */
'use strict';

// Modules
let   AWS  = require('aws-sdk');
const util = require('./util.js');

// Default values
const config = {
	service: 'Lambda',
	debug  : false
};

module.exports.configure = (values) => {
	return util.applyChanges(config, values);
};

// Defines the AWS SDK Instance
module.exports.setAWS = (aws) => {
	AWS = aws;
};

// This function is used to validate the destination in a target. This is used by the configuration
exports.destinationRegex = /^[a-zA-Z0-9_-]{1,64}(:(\$LATEST|[a-zA-Z0-9_-]+))?$/;

exports.targetSettings = (target) => {
	if(target.collapse != "none") {
		console.error(`Ignoring parameter 'collapse' for target '${target.id}' of type '${target.type}'`);
	}
	target.collapse = "API";
};

//********
// This function creates an instance of an AWS Lambda service
exports.create = function(target, options) {
	const serviceOptions = {};
	if(options) {
		util.applyChanges(serviceOptions, options.serviceOptions);
	}
	const service = {
		version: '$LATEST',
		functionName: target.destination,
		instance: new AWS.Lambda(serviceOptions),
		debug: config.debug
	};
	const versionSeparator = target.destination.indexOf(':');
	if(versionSeparator != -1) {
		service.functionName = target.destination.substr(0, versionSeparator);
		service.version = target.destination.substr(versionSeparator + 1);
	}
	util.applyChanges(service, options);
	/* istanbul ignore next */
	if(service.debug) {
		console.log("Created new AWS.Lambda service instance");
	}
	return service;
};

//********
// This function sends messages to AWS Lambda (this will be a simple passthrough)
exports.intercept = function(service, target, event) {
	return service.instance.invoke({ FunctionName: service.functionName, Qualifier: service.version, InvocationType: 'Event', Payload: JSON.stringify(event) }).promise();
};
