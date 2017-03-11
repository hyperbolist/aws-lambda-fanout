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
 * This Node.js library processes messages and forwards them to Amazon Elasticsearch Service.
 */
'use strict';

// Modules
let   AWS  = require('aws-sdk');
const util = require('./util.js');

// Default values
const config = {
	service: 'Elasticsearch',
	debug  : false
};

module.exports.configure = (values) => {
	return util.applyChanges(config, values);
};

// Limits for message publication
module.exports.limits = {
	maxRecords: Number.MAX_VALUE,  // No limit on number of records, we collapse them in a single value
	maxSize: 10*1024*1024,         // Amazon ElasticSearch Publish only accepts up to 10MiB in a single call
	maxUnitSize: 10*1024*1024,     // Amazon ElasticSearch Publish only accepts up to 10MiB per message
	includeKey: true,              // Records will include the key
	listOverhead: 0,               // Records are already prepared
	recordOverhead: 0,             // Records are already prepared
	interRecordOverhead: 0         // Records are already prepared
};

// This function is used to validate the destination in a target. This is used by the configuration
module.exports.destinationRegex = /^search-[a-z][a-z0-9-]{2,27}-[a-z0-9]+\.[a-z]+-[a-z]+-[0-9]\.es\.amazonaws\.com#.*$/;

module.exports.targetSettings = (target) => {
	target.collapse = "API";
};

// Defines the AWS SDK Instance
module.exports.setAWS = (aws) => {
	AWS = aws;
};

//********
// This function creates an instance of an Amazon ElasticSearch service
module.exports.create = function(target, options) {
	const index = target.destination.indexOf('#');
	const endpoint = target.destination.substr(0, index);
	const destination = target.destination.substr(index+1);
	const service = {
		region: ((target.region && (target.region.length > 0)) ? target.region : util.getEnvString('AWS_REGION')),
		endpoint: new AWS.Endpoint(endpoint),
		path: destination,
		debug: config.debug,
		credentials: new AWS.EnvironmentCredentials('AWS')
	};
	util.applyChanges(service, options);
	/* istanbul ignore next */
	if(service.debug) {
		console.log("Created new Elasticsearch service instance");
	}
	return service;
};

//********
// This function sends messages to Amazon ElasticSearch
module.exports.send = function(service, target, records) {
	return new Promise((resolve, reject) => {
		const req = new AWS.HttpRequest(service.endpoint);
		req.method = 'POST';
		req.path = service.path + '/_bulk';
		if(req.path.charAt(0) != '/') {
			req.path = '/' + req.path;
		}
		req.region = service.region;
		req.headers['presigned-expires'] = false;
		req.headers['Host'] = service.endpoint.host;

		req.body = Buffer.concat(records.map((record) => {
			const index = JSON.stringify({ index: { _id: record.key } });
			const text = record.data.toString('utf-8');
			let   object = null;
			try {
				object = JSON.stringify(JSON.parse(text)); // Ensure we don't have formatting
			} catch (e) {
				object = JSON.stringify({ data: record.data.toString('utf-8') });
			}
			return new Buffer(`${index}\n${object}\n`, 'utf-8');
		}));

		const signer = new AWS.Signers.V4(req , 'es');
		signer.addAuthorization(service.credentials, new Date());

		const send = new AWS.NodeHttpClient();
		send.handleRequest(req, null, function(httpResp) {
			let respBody = '';
			httpResp.on('data', function (chunk) {
				respBody += chunk;
			});
			httpResp.on('end', function () {
				if(httpResp.statusCode == 200) {
					resolve(null);
				} else {
					reject(new Error("Error posting to Amazon ElasticSearch: HTTP Status Code: '" + httpResp.statusCode + "', body '" + respBody + "'"));
				}
			});
		}, function(err) {
			reject(new Error("Error posting to Amazon ElasticSearch: '" + err + "'"));
		});
	});
};
