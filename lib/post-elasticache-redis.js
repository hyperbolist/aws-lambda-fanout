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
 * This Node.js library processes messages and forwards them to Amazon ElastiCache Redis.
 */
'use strict';

// Modules
const redis = require('./redis.js');
const util  = require('./util.js');

// Default values
const config = {
	service    : 'Redis',
	debug      : false, // Activate debug messages
	refreshRate: 6000   // Refresh server list every minute
};

module.exports.configure = (values) => {
	return util.applyChanges(config, values);
};

// Defines the AWS 
module.exports.setAWS = () => {
};

// Limits for message publication
module.exports.limits = {
	maxRecords: Number.MAX_VALUE,  // No limit on number of records, we collapse them in a single value
	maxSize: Number.MAX_VALUE,     // No limit
	maxUnitSize: Number.MAX_VALUE, // No limit
	includeKey: false,             // Records will not include the key
	listOverhead: 0,               // Records are not concatenated, they are sent one by one to the store
	recordOverhead: 0,             // Records are not quoted
	interRecordOverhead: 0         // Records are not concatenated
};

// This function is used to validate the destination in a target. This is used by the configuration
module.exports.destinationRegex = /^[a-zA-Z][a-zA-Z0-9-]{0,19}\.[a-z0-9]+\.clustercfg\.[a-z]+[0-9]\.cache\.amazonaws\.com:[0-9]+$/;

module.exports.targetSettings = (target) => {
	if(target.collapse) {
		console.error(`Ignoring parameter 'collapse' for target '${target.id}' of type '${target.type}'`);
	}
	target.collapse = "multiple";
	if(target.role) {
		console.error(`Ignoring parameter 'role' for target '${target.id}' of type '${target.type}'`);
		target.role = null;
	}
	if(target.region) {
		console.error(`Ignoring parameter 'region' for target type '${target.type}'`);
		target.region = null;
	}
};

//********
// This function creates an instance of Redis
module.exports.create = function(target, options) {
	var service = {
		endpoint: target.destination,
		refresh: 0,
		servers: [],
		debug: config.debug,
		refreshRate: config.refreshRate
	};
	if((options !== null) && (options !== undefined) && (typeof options == "object")) {
		for(let key in options) {
			service[key] = options[key];
		}
	}
	/* istanbul ignore next */
	if(service.debug) {
		console.log("Created new redis service instance");
	}
	return service;
};

//********
// This function checks if we need to reload the list of hosts (initially done every minute)
function refreshHosts(service) {
	if(service.refresh < Date.now()) {
		/* istanbul ignore next */
		if(service.debug) {
			console.log("Updating redis server list");
		}
		return redis.servers(service.endpoint, service).then((hosts) => {
			service.servers = hosts;
			service.refresh = Date.now() + service.refreshRate;
		});
	} else {
		return Promise.resolve(null);
	}
}

//********
// This function sends messages to Redis
module.exports.send = function(service, target, records) {
	return refreshHosts(service).then(() => {
		/* istanbul ignore next */
		if(service.debug) {
			console.log(`Sending ${records.length} items to redis`);
		}
		return redis.set(service.servers, records, service);
	});
};
