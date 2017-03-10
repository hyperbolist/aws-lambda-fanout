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
 * This Node.js manages the creation of roles and service references for 
 *  calling AWS services.
 */
'use strict';

// Modules
let   AWS = require('aws-sdk');
const configuration = require('./configuration.js');

// Definition of services:
var serviceDefinitions = {
	sns:       require('./post-sns.js'),
	sqs:       require('./post-sqs.js'),
	es:        require('./post-es.js'),
	kinesis:   require('./post-kinesis.js'),
	firehose:  require('./post-firehose.js'),
	iot:       require('./post-iot.js'),
	lambda:    require('./post-lambda.js'),
	memcached: require('./post-elasticache-memcached.js'),
	redis:     require('./post-elasticache-redis.js')
};
exports.definitions = serviceDefinitions;

// Default values
const config = {
	debug             : false,
	stsSessionDuration: 900,
	stsSessionName    : 60
};

module.exports.configure = (values) => {
	if((values !== null) && (values !== undefined) && (typeof values == "object")) {
		for(let key in values) {
			config[key] = values[key];
		}
	}
	for(let i in serviceDefinitions) {
		serviceDefinitions[i].configure(config);
		serviceDefinitions[i].configure(config[i]); // Allow service per service override of the settings
	}
	return config;
};

// Runtime variables
const currentRegion = configuration.getEnvString('AWS_REGION', 'us-east-1'); // Current region from Lambda environment variables (where the function runs)
let   sts           = new AWS.STS({ region: currentRegion });                // STS service for impersonation of roles in the fan-out process 

// Defines the AWS SDK Instance
module.exports.setAWS = (aws) => {
	AWS = aws;
	sts = new AWS.STS({ region: currentRegion });
};

// Roles
const roles = { none: { services: {}, credentials: null, expiration: Number.MAX_VALUE } }; // Object storing service references for assumed roles

//********
// This function tests if a service exists for the specified role, and creates one of it does none exist
// The services are created in a "pool", objects returned have three properties:
//  - definition: the library used for the service definition
//  - service: the actual reference
//  - dispose(): a method that returns the service to the pool for another usage
function ensureService(target, role) {
	const serviceRegion = (target.region && (target.region.length > 0)) ? target.region : currentRegion;
	let   serviceKey    = serviceRegion + '|' + target.type;
	if(target.endpoint) {
		serviceKey = serviceKey + '|' + target.endpoint;
	}

	let service = null;
	let definition = null;

	if(! role.services.hasOwnProperty(serviceKey)) {
		role.services[serviceKey] = [];
	}

	if(role.services[serviceKey].length == 0) {
		const options = { region: serviceRegion };
		if(role.credentials) {
			options.credentials = role.credentials;
		}
		if(serviceDefinitions.hasOwnProperty(target.type)) {
			definition = serviceDefinitions[target.type];
			service = definition.create(target, options);
		} else {
			console.error("Error creating service reference '" + target.id + "', type '" + target.type + "' is invalid", JSON.stringify(target));
			return Promise.reject(new Error("Unknown service type '" + target.type + "'"));
		}
	} else {
		if(config.debug) {
			console.log("Reusing existing service of type '", target.type, "'");
		}
		definition = serviceDefinitions[target.type];
		service = role.services[serviceKey].shift();
	}
	return Promise.resolve({ definition: definition, service: service, dispose: () => role.services[serviceKey].push(service) });
}

//********
// This function generates the service reference for accessing the remote service
exports.get = function(target) {
	var roleArn = (target.role && (target.role.length > 0)) ? target.role : "none";

	if((! roles.hasOwnProperty(roleArn)) || (roles[roleArn].expiration < Date.now())) {
		// This role has not yet been impersonated, or has expired
		var stsSessionDuration = config.stsSessionDuration;
		var stsSessionName = config.stsSessionName;

		var params = { RoleArn: roleArn, RoleSessionName: stsSessionName, DurationSeconds: stsSessionDuration };
		if(target.externalId) {
			params.ExternalId = target.externalId;
		}
		sts.assumeRole(params).promise().then((data) => {
			if(config.debug) {
				console.log("Assumed role '" + roleArn + "'");
			}
			// Clear all values, and force service refresh 5 seconds before end of impersonation
			var role = { services: {}, credentials: sts.credentialsFrom(data), expiration: Date.now() + ((stsSessionDuration - 5) * 1000) };
			roles[roleArn] = role;
			return ensureService(target, role);
		}).catch((err) => {
			// Unable to impersonate role, generate error
			console.error("Error creating service reference '" + target.id + "', an error occured while impersonating role '" + roleArn + "':", err);
			return Promise.reject(new Error("Error assuming role '" + roleArn + "'"));
		});
	} else {
		// We have a valid role, reuse or create the service
		return ensureService(target, roles[roleArn]);
	}
};
