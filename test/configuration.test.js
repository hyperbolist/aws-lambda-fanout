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
 * This Node.js script tests the features from the crc16.js script against well known CRC16
 */

/* global describe:false, it:false */

'use strict';
const AWS           = require('./mock-aws.js');
const configuration = require('../lib/configuration.js');
const ddb           = require('../lib/ddb-utils.js');
const uuid          = require('uuid');
const assert        = require('assert');

describe('environment', () => {
	it('getEnvString', (done) => {
		const value = uuid();
		process.env.MY_FAKE_STRING = `${value}`;
		assert.strictEqual(configuration.getEnvString('MY_FAKE_STRING', 'nothing'), value);
		assert.strictEqual(configuration.getEnvString('MY_FAKE_STRING2', 'nothing'), 'nothing');
		done();
	});
	it('getEnvNumber', (done) => {
		assert.strictEqual(configuration.getEnvNumber('MY_FAKE_NUMBER2', 10), 10);

		let value = 0;
		process.env.MY_FAKE_NUMBER = `${value}`;
		assert.strictEqual(configuration.getEnvNumber('MY_FAKE_NUMBER', 10), value);
		value = 20;
		process.env.MY_FAKE_NUMBER = `${value}`;
		assert.strictEqual(configuration.getEnvNumber('MY_FAKE_NUMBER', 10), value);
		value = -20;
		process.env.MY_FAKE_NUMBER = `${value}`;
		assert.strictEqual(configuration.getEnvNumber('MY_FAKE_NUMBER', 10), value);
		value = 20.5;
		process.env.MY_FAKE_NUMBER = `${value}`;
		assert.strictEqual(configuration.getEnvNumber('MY_FAKE_NUMBER', 10), value);
		value = "a";
		process.env.MY_FAKE_NUMBER = `${value}`;
		assert.strictEqual(configuration.getEnvNumber('MY_FAKE_NUMBER', 10), 10);
		done();
	});
	it('getEnvBool', (done) => {
		assert.strictEqual(configuration.getEnvBool('MY_FAKE_BOOL2', false), false);
		assert.strictEqual(configuration.getEnvBool('MY_FAKE_BOOL2', true), true);

		let value = "true";
		process.env.MY_FAKE_BOOL = `${value}`;
		assert.strictEqual(configuration.getEnvBool('MY_FAKE_BOOL', false), true);
		assert.strictEqual(configuration.getEnvBool('MY_FAKE_BOOL', true), true);
		value = "on";
		process.env.MY_FAKE_BOOL = `${value}`;
		assert.strictEqual(configuration.getEnvBool('MY_FAKE_BOOL', false), true);
		assert.strictEqual(configuration.getEnvBool('MY_FAKE_BOOL', true), true);
		value = "1";
		process.env.MY_FAKE_BOOL = `${value}`;
		assert.strictEqual(configuration.getEnvBool('MY_FAKE_BOOL', false), true);
		assert.strictEqual(configuration.getEnvBool('MY_FAKE_BOOL', true), true);
		value = "TrUe";
		process.env.MY_FAKE_BOOL = `${value}`;
		assert.strictEqual(configuration.getEnvBool('MY_FAKE_BOOL', false), true);
		assert.strictEqual(configuration.getEnvBool('MY_FAKE_BOOL', true), true);
		value = "false";
		process.env.MY_FAKE_BOOL = `${value}`;
		assert.strictEqual(configuration.getEnvBool('MY_FAKE_BOOL', false), false);
		assert.strictEqual(configuration.getEnvBool('MY_FAKE_BOOL', true), false);
		value = "off";
		process.env.MY_FAKE_BOOL = `${value}`;
		assert.strictEqual(configuration.getEnvBool('MY_FAKE_BOOL', false), false);
		assert.strictEqual(configuration.getEnvBool('MY_FAKE_BOOL', true), false);
		value = "0";
		process.env.MY_FAKE_BOOL = `${value}`;
		assert.strictEqual(configuration.getEnvBool('MY_FAKE_BOOL', false), false);
		assert.strictEqual(configuration.getEnvBool('MY_FAKE_BOOL', true), false);
		value = "FaLsE";
		process.env.MY_FAKE_BOOL = `${value}`;
		assert.strictEqual(configuration.getEnvBool('MY_FAKE_BOOL', false), false);
		assert.strictEqual(configuration.getEnvBool('MY_FAKE_BOOL', true), false);
		value = "other";
		process.env.MY_FAKE_BOOL = `${value}`;
		assert.strictEqual(configuration.getEnvBool('MY_FAKE_BOOL', false), false);
		assert.strictEqual(configuration.getEnvBool('MY_FAKE_BOOL', true), true);
		done();
	});
});

describe('common', () => {
	it('configure', (done) => {
		let config = configuration.configure();
		assert.notStrictEqual(config, null);
		assert.notStrictEqual(config, undefined);
		let initialConfig = config;
		config = configuration.configure(null);
		assert.deepEqual(config, initialConfig);
		config = configuration.configure(10);
		assert.deepEqual(config, initialConfig);
		config = configuration.configure({});
		assert.deepEqual(config, initialConfig);
		config = configuration.configure({ value: 10 });
		assert.strictEqual(config.value, 10);
		for(let key in config) {
			if(key != 'value') {
				assert.strictEqual(config[key], initialConfig[key]);
			}
		}
		initialConfig = config;
		done();
	});
});

describe('configuration', () => {
	it('setAWS', (done) => {
		configuration.setAWS(AWS);
		done();
	});

	it('valid empty items', (done) => {
		const requestKey = uuid();
		const serviceDefinitions = {};
		AWS.mock('DynamoDB','query', { Items: [] });

		configuration.get(requestKey, serviceDefinitions).then((entries) => {
			assert.deepEqual(entries, []);
			AWS.restore('DynamoDB', 'query');
			done();
		}).catch((err) => {
			AWS.restore('DynamoDB', 'query');
			done(err);
		});
	});

	it('valid no items', (done) => {
		const requestKey = uuid();
		const serviceDefinitions = {};
		AWS.mock('DynamoDB','query', {});

		configuration.get(requestKey, serviceDefinitions).then((entries) => {
			assert.deepEqual(entries, []);
			AWS.restore('DynamoDB', 'query');
			done();
		}).catch((err) => {
			AWS.restore('DynamoDB', 'query');
			done(err);
		});
	});

	it('valid kinesis', (done) => {
		const requestKey = uuid();
		const serviceDefinitions = {
			'kinesis': {
				destinationRegex: /^arn:aws:kinesis:[a-z]+-[a-z]+-[0-9]:[0-9]{12}:stream\/[a-zA-Z0-9_-]{1,128}$/,
				targetSettings: function() {}
			}
		};

		const items = [
			{ id: `${requestKey}`, type: "kinesis", collapse: "JSON", sourceArn: "arn:aws:kinesis:eu-west-1:123456789012:stream/in", destination: "arn:aws:kinesis:eu-west-1:123456789012:stream/out", active: true, parallel: true, role: "arn:aws:iam::123456789012:role/myRole", externalId: "Code", region: "eu-west-1", convertDDB: true, deaggregate: true } // !!! VALID !!!
		].map(ddb.generateDynamoDBObject);

		AWS.mock('DynamoDB','query', { Items: items });

		configuration.get(requestKey, serviceDefinitions).then((entries) => {
			assert.deepEqual(entries,[
				{ id: `${requestKey}`, type: "kinesis", collapse: "JSON", sourceArn: "arn:aws:kinesis:eu-west-1:123456789012:stream/in", destination: "arn:aws:kinesis:eu-west-1:123456789012:stream/out", active: true, parallel: true, role: "arn:aws:iam::123456789012:role/myRole", externalId: "Code", region: "eu-west-1", convertDDB: true, deaggregate: true }
			]);
			AWS.restore('DynamoDB', 'query');
			done();
		}).catch((err) => {
			AWS.restore('DynamoDB', 'query');
			done(err);
		});
	});

	it('valid kinesis with custom target settings', (done) => {
		const requestKey = uuid();
		const serviceDefinitions = {
			'kinesis': {
				destinationRegex: /^arn:aws:kinesis:[a-z]+-[a-z]+-[0-9]:[0-9]{12}:stream\/[a-zA-Z0-9_-]{1,128}$/,
				targetSettings: function(target) { target.collapse = "API"; }
			}
		};

		const items = [
			{ id: `${requestKey}`, type: "kinesis", collapse: "JSON", sourceArn: "arn:aws:kinesis:eu-west-1:123456789012:stream/in", destination: "arn:aws:kinesis:eu-west-1:123456789012:stream/out", active: true, parallel: true, role: "arn:aws:iam::123456789012:role/myRole", externalId: "Code", region: "eu-west-1", convertDDB: true, deaggregate: true } // !!! VALID !!!
		].map(ddb.generateDynamoDBObject);

		AWS.mock('DynamoDB','query', { Items: items });

		configuration.get(requestKey, serviceDefinitions).then((entries) => {
			assert.deepEqual(entries,[
				{ id: `${requestKey}`, type: "kinesis", collapse: "API", sourceArn: "arn:aws:kinesis:eu-west-1:123456789012:stream/in", destination: "arn:aws:kinesis:eu-west-1:123456789012:stream/out", active: true, parallel: true, role: "arn:aws:iam::123456789012:role/myRole", externalId: "Code", region: "eu-west-1", convertDDB: true, deaggregate: true }
			]);
			AWS.restore('DynamoDB', 'query');
			done();
		}).catch((err) => {
			AWS.restore('DynamoDB', 'query');
			done(err);
		});
	});

	it('valid kinesis with reload', (done) => {
		const requestKey = uuid();
		const serviceDefinitions = {
			'kinesis': {
				destinationRegex: /^arn:aws:kinesis:[a-z]+-[a-z]+-[0-9]:[0-9]{12}:stream\/[a-zA-Z0-9_-]{1,128}$/,
				targetSettings: function() {}
			}
		};

		const items = [
			{ id: `${requestKey}`, type: "kinesis", collapse: "JSON", sourceArn: "arn:aws:kinesis:eu-west-1:123456789012:stream/in", destination: "arn:aws:kinesis:eu-west-1:123456789012:stream/out", active: true, parallel: true, role: "arn:aws:iam::123456789012:role/myRole", externalId: "Code", region: "eu-west-1", convertDDB: true, deaggregate: true } // !!! VALID !!!
		].map(ddb.generateDynamoDBObject);

		AWS.mock('DynamoDB','query', { Items: items });

		configuration.get(requestKey, serviceDefinitions).then((entries) => {
			assert.deepEqual(entries,[
				// collapse = API for kinesis
				{ id: `${requestKey}`, type: "kinesis", collapse: "JSON", sourceArn: "arn:aws:kinesis:eu-west-1:123456789012:stream/in", destination: "arn:aws:kinesis:eu-west-1:123456789012:stream/out", active: true, parallel: true, role: "arn:aws:iam::123456789012:role/myRole", externalId: "Code", region: "eu-west-1", convertDDB: true, deaggregate: true }
			]);
			AWS.restore('DynamoDB', 'query');

			// Ensure any reload will fail
			AWS.mock('DynamoDB','query', Promise.reject(new Error(`Generated error for request ${requestKey}`)));

			configuration.get(requestKey, serviceDefinitions).then((entries) => {
				assert.deepEqual(entries,[
					// collapse = API for kinesis
					{ id: `${requestKey}`, type: "kinesis", collapse: "JSON", sourceArn: "arn:aws:kinesis:eu-west-1:123456789012:stream/in", destination: "arn:aws:kinesis:eu-west-1:123456789012:stream/out", active: true, parallel: true, role: "arn:aws:iam::123456789012:role/myRole", externalId: "Code", region: "eu-west-1", convertDDB: true, deaggregate: true }
				]);
				AWS.restore('DynamoDB', 'query');
				done();
			}).catch((err) => {
				AWS.restore('DynamoDB', 'query');
				done(err);
			});
		}).catch((err) => {
			AWS.restore('DynamoDB', 'query');
			done(err);
		});
	});

	it('valid kinesis with pagination', (done) => {
		const requestKey = uuid();
		const serviceDefinitions = {
			'kinesis': {
				destinationRegex: /^arn:aws:kinesis:[a-z]+-[a-z]+-[0-9]:[0-9]{12}:stream\/[a-zA-Z0-9_-]{1,128}$/,
				targetSettings: function() {}
			}
		};

		const responses = [
			{
				ExclusiveStartKey: undefined,
				Response: {
					Items: [
						{ id: `${requestKey}#01`, type: "kinesis", collapse: "JSON", sourceArn: "arn:aws:kinesis:eu-west-1:123456789012:stream/in", destination: "arn:aws:kinesis:eu-west-1:123456789012:stream/out", active: true, parallel: true, role: "arn:aws:iam::123456789012:role/myRole", externalId: "Code", region: "eu-west-1", convertDDB: true, deaggregate: true } // !!! VALID !!!
					].map(ddb.generateDynamoDBObject),
					ExclusiveStartKey: `${requestKey}#1`
				}
			},
			{
				ExclusiveStartKey: `${requestKey}#1`,
				Response: {
					Items: [
						{ id: `${requestKey}#02`, type: "kinesis", collapse: "JSON", sourceArn: "arn:aws:kinesis:eu-west-1:123456789012:stream/in", destination: "arn:aws:kinesis:eu-west-1:123456789012:stream/out", active: true, parallel: true, role: "arn:aws:iam::123456789012:role/myRole", externalId: "Code", region: "eu-west-1", convertDDB: true, deaggregate: true } // !!! VALID !!!
					].map(ddb.generateDynamoDBObject),
				}
			}
		];

		AWS.mock('DynamoDB','query', (params) => {
			const res = responses.shift();
			assert.strictEqual(params.ExclusiveStartKey, res.ExclusiveStartKey);
			return res.Response;
		});

		configuration.get(requestKey, serviceDefinitions).then((entries) => {
			assert.deepEqual(entries,[
				{ id: `${requestKey}#01`, type: "kinesis", collapse: "JSON", sourceArn: "arn:aws:kinesis:eu-west-1:123456789012:stream/in", destination: "arn:aws:kinesis:eu-west-1:123456789012:stream/out", active: true, parallel: true, role: "arn:aws:iam::123456789012:role/myRole", externalId: "Code", region: "eu-west-1", convertDDB: true, deaggregate: true },
				{ id: `${requestKey}#02`, type: "kinesis", collapse: "JSON", sourceArn: "arn:aws:kinesis:eu-west-1:123456789012:stream/in", destination: "arn:aws:kinesis:eu-west-1:123456789012:stream/out", active: true, parallel: true, role: "arn:aws:iam::123456789012:role/myRole", externalId: "Code", region: "eu-west-1", convertDDB: true, deaggregate: true }
			]);
			AWS.restore('DynamoDB', 'query');
			done();
		}).catch((err) => {
			AWS.restore('DynamoDB', 'query');
			done(err);
		});
	});

	it('invalid definitions', (done) => {
		const requestKey = uuid();
		const serviceDefinitions = {
			'kinesis': {
				destinationRegex: /^arn:aws:kinesis:[a-z]+-[a-z]+-[0-9]:[0-9]{12}:stream\/[a-zA-Z0-9_-]{1,128}$/,
				targetSettings: function() {}
			}
		};

		const items = [
			{ },                                                                                                                                                                                                                                                                                                                                      // No id
			{ id: "" },                                                                                                                                                                                                                                                                                                                               // Id empty string
			{ id: 2 },                                                                                                                                                                                                                                                                                                                                // Id not as string
			{ id: `${requestKey}#03`, type: true },                                                                                                                                                                                                                                                                                                                 // Target not as string
			{ id: `${requestKey}#04`, type: "unknown" },                                                                                                                                                                                                                                                                                                            // Target unknown
			{ id: `${requestKey}#05`, type: "kinesis" },                                                                                                                                                                                                                                                                                                            // No Collapse
			{ id: `${requestKey}#06`, type: "kinesis", collapse: 10 },                                                                                                                                                                                                                                                                                              // Collapse not as string
			{ id: `${requestKey}#07`, type: "kinesis", collapse: "blah" },                                                                                                                                                                                                                                                                                          // Collapse not valid
			{ id: `${requestKey}#08`, type: "kinesis", collapse: "JSON" },                                                                                                                                                                                                                                                                                          // No source ARN
			{ id: `${requestKey}#09`, type: "kinesis", collapse: "JSON", sourceArn: "arn:aws:s3:::myBucket" },                                                                                                                                                                                                                                                      // source ARN invalid
			{ id: `${requestKey}#10`, type: "kinesis", collapse: "JSON", sourceArn: "arn:aws:kinesis:eu-west-1:123456789012:stream/in" },                                                                                                                                                                                                                           // No destination ARN
			{ id: `${requestKey}#11`, type: "kinesis", collapse: "JSON", sourceArn: "arn:aws:kinesis:eu-west-1:123456789012:stream/in", destination: "arn:aws:s3:::myBucket" },                                                                                                                                                                                     // destination ARN invalid
			{ id: `${requestKey}#12`, type: "kinesis", collapse: "JSON", sourceArn: "arn:aws:kinesis:eu-west-1:123456789012:stream/in", destination: "arn:aws:kinesis:eu-west-1:123456789012:stream/out", active: 10 },                                                                                                                                             // active not bool
			{ id: `${requestKey}#13`, type: "kinesis", collapse: "JSON", sourceArn: "arn:aws:kinesis:eu-west-1:123456789012:stream/in", destination: "arn:aws:kinesis:eu-west-1:123456789012:stream/out", active: true, parallel: 10 },                                                                                                                             // parallel not bool
			{ id: `${requestKey}#14`, type: "kinesis", collapse: "JSON", sourceArn: "arn:aws:kinesis:eu-west-1:123456789012:stream/in", destination: "arn:aws:kinesis:eu-west-1:123456789012:stream/out", active: true, parallel: true, role: 10 },                                                                                                                 // role not string
			{ id: `${requestKey}#15`, type: "kinesis", collapse: "JSON", sourceArn: "arn:aws:kinesis:eu-west-1:123456789012:stream/in", destination: "arn:aws:kinesis:eu-west-1:123456789012:stream/out", active: true, parallel: true, role: "arn:aws:iam::123456789012:user/myUser" },                                                                            // role not valid
			{ id: `${requestKey}#16`, type: "kinesis", collapse: "JSON", sourceArn: "arn:aws:kinesis:eu-west-1:123456789012:stream/in", destination: "arn:aws:kinesis:eu-west-1:123456789012:stream/out", active: true, parallel: true, role: "arn:aws:iam::123456789012:role/myRole", externalId: 10 },                                                            // externalId not string
			{ id: `${requestKey}#17`, type: "kinesis", collapse: "JSON", sourceArn: "arn:aws:kinesis:eu-west-1:123456789012:stream/in", destination: "arn:aws:kinesis:eu-west-1:123456789012:stream/out", active: true, parallel: true, role: "arn:aws:iam::123456789012:role/myRole", externalId: "$Error" },                                                      // externalId not valid
			{ id: `${requestKey}#18`, type: "kinesis", collapse: "JSON", sourceArn: "arn:aws:kinesis:eu-west-1:123456789012:stream/in", destination: "arn:aws:kinesis:eu-west-1:123456789012:stream/out", active: true, parallel: true, role: "arn:aws:iam::123456789012:role/myRole", externalId: "Code", region: 10 },                                            // region not string
			{ id: `${requestKey}#19`, type: "kinesis", collapse: "JSON", sourceArn: "arn:aws:kinesis:eu-west-1:123456789012:stream/in", destination: "arn:aws:kinesis:eu-west-1:123456789012:stream/out", active: true, parallel: true, role: "arn:aws:iam::123456789012:role/myRole", externalId: "Code", region: "Invalid" },                                     // region not string
			{ id: `${requestKey}#20`, type: "kinesis", collapse: "JSON", sourceArn: "arn:aws:kinesis:eu-west-1:123456789012:stream/in", destination: "arn:aws:kinesis:eu-west-1:123456789012:stream/out", active: true, parallel: true, role: "arn:aws:iam::123456789012:role/myRole", externalId: "Code", region: "eu-west-1", convertDDB: 10 },                   // convertDDB not boolean
			{ id: `${requestKey}#21`, type: "kinesis", collapse: "JSON", sourceArn: "arn:aws:kinesis:eu-west-1:123456789012:stream/in", destination: "arn:aws:kinesis:eu-west-1:123456789012:stream/out", active: true, parallel: true, role: "arn:aws:iam::123456789012:role/myRole", externalId: "Code", region: "eu-west-1", convertDDB: true, deaggregate: 10 } // deaggregate not boolean
		].map(ddb.generateDynamoDBObject);

		AWS.mock('DynamoDB','query', { Items: items });

		configuration.get(requestKey, serviceDefinitions).then((entries) => {
			assert.deepEqual(entries,[]);
			AWS.restore('DynamoDB', 'query');
			done();
		}).catch((err) => {
			AWS.restore('DynamoDB', 'query');
			done(err);
		});
	});

	it('failures', (done) => {
		const requestKey = uuid();
		const serviceDefinitions = {};

		AWS.mock('DynamoDB','query', Promise.reject(new Error(`Generated error for request ${requestKey}`)));

		configuration.get(requestKey, serviceDefinitions).then(() => {
			AWS.restore('DynamoDB', 'query');
			done(new Error("An error should have been raised"));
		}).catch((err) => {
			AWS.restore('DynamoDB', 'query');
			assert(err instanceof Error);
			assert.strictEqual(err.message, `Errors occured while loading configuration data from Amazon DynamoDB table 'fanoutTargets':Error: Generated error for request ${requestKey}`);
			done();
		}).catch((err) => {
			done(err);
		});
	});
});
