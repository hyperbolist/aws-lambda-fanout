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
 * This Node.js script tests the features from the post-elasticache-redis.js
 */

/* global describe:false, it:false, before:false, after:false */

'use strict';

// Environment settings for AWS SDK
process.env.AWS_REGION            = 'xx-test-1'; 
process.env.AWS_ACCESS_KEY_ID     = "AKIAENVEXAMPLE";
process.env.AWS_SECRET_ACCESS_KEY = "SecretKeyExample";
process.env.AWS_SESSION_TOKEN     = "SessionTokenExample";

const AWS          = require('aws-sdk');
const sinon        = require('sinon');
const uuid         = require('uuid');
const assert       = require('assert');
const post         = require('../lib/post-es.js');
const EventEmitter = require('events');

function parseAuthorization(authorization) {
	const result = {};
	result.type = authorization.split(' ')[0];
	result.params = {};
	authorization.split(' ').slice(1).forEach((v) => {
		if(v.endsWith(',')) {
			v = v.substr(0, v.length - 1);
		}
		const parts = v.split('=', 2);
		result.params[parts[0]] = parts[1];
	});
	return result;
}

function MockHttpResponse(httpRequest, httpOptions, processor) {
	EventEmitter.call(this);
	this.httpRequest = httpRequest;
	this.httpOptions = httpOptions;
	this.processor   = processor;
	this.statusCode  = 0;
}

MockHttpResponse.prototype = Object.create(EventEmitter.prototype); 

MockHttpResponse.prototype.send = function(content) {
	this.emit('data', content);
};

MockHttpResponse.prototype.end = function() {
	this.emit('end');
};

MockHttpResponse.prototype.process = function(callback, errCallback) {
	try {
		let result = { statusCode: 200, body: "OK" };

		if(typeof this.processor == 'function') {
			result = this.processor(this.httpRequest, this.httpOptions);
		} else if(this.processor !== null && this.processor !== undefined) {
			result = this.processor;
		}

		callback(this);
		if(result.hasOwnProperty('statusCode') && result.hasOwnProperty('body')) {
			this.statusCode = result.statusCode;
			if(Array.isArray(result.body)) {
				result.body.forEach((chunk) => {
					if((typeof chunk == "string") || (! Buffer.isBuffer(chunk))) {
						this.send(chunk);
					} else {
						this.send(JSON.stringify(chunk));
					}
				});
			} else {
				this.send(result.body);
			}
		} else if((typeof result == "string") || (! Buffer.isBuffer(result))) {
			this.statusCode = 200;
			this.send(result);
		} else {
			this.statusCode = 200;
			this.send(JSON.stringify(result));
		}
		this.end();
	} catch(e) {
		errCallback(e);				
	}
};

describe('post-es', () => {
	let processRequest = null;
	before(() => {
		sinon.stub(AWS.NodeHttpClient.prototype, 'handleRequest', (httpRequest, httpOptions, callback, errCallback) => {
			new MockHttpResponse(httpRequest, httpOptions, processRequest).process(callback, errCallback);
		});
	});
	after(() => {
		AWS.NodeHttpClient.prototype.handleRequest.restore();
	});

	it('setAWS', (done) => {
		post.setAWS(AWS);
		done();
	});

	it('destinationRegex', (done) => {
		assert(! post.destinationRegex.test(""));
		assert(! post.destinationRegex.test("nothing"));
		assert(post.destinationRegex.test("search-test-abcdefghijklmnopqrstuvwxyz.us-east-1.es.amazonaws.com#type/index"));
		done();
	});

	it('configure', (done) => {
		let conf = post.configure();
		assert.strictEqual(conf.service, 'Elasticsearch');
		conf = post.configure(null);
		assert.strictEqual(conf.service, 'Elasticsearch');
		conf = post.configure({});
		assert.strictEqual(conf.service, 'Elasticsearch');
		conf = post.configure({ service: "OtherService" });
		assert.strictEqual(conf.service, 'OtherService');
		done();
	});

	it('target', (done) => {
		let target = { };
		post.targetSettings(target);
		assert.deepEqual(target, { collapse: "API" });
		target = { collapse: "JSON" };
		post.targetSettings(target);
		assert.deepEqual(target, { collapse: "API" });
		target = { role: "roleName" };
		post.targetSettings(target);
		assert.deepEqual(target, { collapse: "API", role: "roleName" });
		done();
	});

	it('store item (with params)', (done) => {
		const key     = uuid();
		let   service = null;
		const target  = { destination: `search-test-abcdefghijklmnopqrstuvwxyz.us-east-1.es.amazonaws.com#type/index` };

		Promise.resolve(null).then(() => {
			return post.create(target, { subObject: { value: 1}, credentials: new AWS.Credentials("AKIAEXAMPLE", "SecretKeyExample") } );
		}).then((s) => {
			service = s;
		}).then(() => {
			processRequest = (httpRequest) => {
				assert.strictEqual(httpRequest.headers.Host, target.destination.split('#')[0]);
				assert.strictEqual(httpRequest.region, process.env.AWS_REGION);
				assert.strictEqual(httpRequest.headers['x-amz-security-token'], undefined);
				const auth = parseAuthorization(httpRequest.headers.Authorization);
				const when = new Date().toISOString().substr(0, 10).replace(/-/g, '');
				assert.strictEqual(auth.type, 'AWS4-HMAC-SHA256');
				assert.strictEqual(auth.params.Credential, `AKIAEXAMPLE/${when}/${process.env.AWS_REGION}/es/aws4_request`);

				const bodyParts = httpRequest.body.toString('utf-8').split('\n');
				assert.strictEqual(bodyParts.length, 3);
				assert.deepEqual(JSON.parse(bodyParts[0]), { index: { _id: `${key}` } });
				assert.deepEqual(JSON.parse(bodyParts[1]), { data: `AZERTY${key}` });
				assert.strictEqual(bodyParts[2], "");
				return { statusCode: 200, body: "OK" };
			};

			return post.send(service, target, [ { key: key, data: `AZERTY${key}` } ]);
		}).then(() => {
			done();
		}).catch((err) => {
			done(err);
		});
	});

	it('store item (default)', (done) => {
		const key     = uuid();
		let   service = null;
		const target  = { destination: `search-test-abcdefghijklmnopqrstuvwxyz.us-east-1.es.amazonaws.com#type/index` };

		Promise.resolve(null).then(() => {
			return post.create(target);
		}).then((s) => {
			service = s;
		}).then(() => {
			processRequest = (httpRequest) => {
				assert.strictEqual(httpRequest.headers.Host, target.destination.split('#')[0]);
				assert.strictEqual(httpRequest.region, process.env.AWS_REGION);
				assert.strictEqual(httpRequest.headers['x-amz-security-token'], process.env.AWS_SESSION_TOKEN);
				const auth = parseAuthorization(httpRequest.headers.Authorization);
				const when = new Date().toISOString().substr(0, 10).replace(/-/g, '');
				assert.strictEqual(auth.type, 'AWS4-HMAC-SHA256');
				assert.strictEqual(auth.params.Credential, `${process.env.AWS_ACCESS_KEY_ID}/${when}/${process.env.AWS_REGION}/es/aws4_request`);

				const bodyParts = httpRequest.body.toString('utf-8').split('\n');
				assert.strictEqual(bodyParts.length, 3);
				assert.deepEqual(JSON.parse(bodyParts[0]), { index: { _id: `${key}` } });
				assert.deepEqual(JSON.parse(bodyParts[1]), { data: `AZERTY${key}` });
				assert.strictEqual(bodyParts[2], "");
				return { statusCode: 200, body: "OK" };
			};

			return post.send(service, target, [ { key: key, data: `AZERTY${key}` } ]);
		}).then(() => {
			done();
		}).catch((err) => {
			done(err);
		});
	});

	it('store item (with region)', (done) => {
		const key     = uuid();
		let   service = null;
		const target  = { destination: `search-test-abcdefghijklmnopqrstuvwxyz.us-east-1.es.amazonaws.com#type/index`, region: 'us-east-1' };

		Promise.resolve(null).then(() => {
			return post.create(target);
		}).then((s) => {
			service = s;
		}).then(() => {
			processRequest = (httpRequest) => {
				assert.strictEqual(httpRequest.headers.Host, target.destination.split('#')[0]);
				assert.strictEqual(httpRequest.region, target.region);
				assert.strictEqual(httpRequest.headers['x-amz-security-token'], process.env.AWS_SESSION_TOKEN);
				const auth = parseAuthorization(httpRequest.headers.Authorization);
				const when = new Date().toISOString().substr(0, 10).replace(/-/g, '');
				assert.strictEqual(auth.type, 'AWS4-HMAC-SHA256');
				assert.strictEqual(auth.params.Credential, `${process.env.AWS_ACCESS_KEY_ID}/${when}/${target.region}/es/aws4_request`);

				const bodyParts = httpRequest.body.toString('utf-8').split('\n');
				assert.strictEqual(bodyParts.length, 3);
				assert.deepEqual(JSON.parse(bodyParts[0]), { index: { _id: `${key}` } });
				assert.deepEqual(JSON.parse(bodyParts[1]), { data: `AZERTY${key}` });
				assert.strictEqual(bodyParts[2], "");
				return { statusCode: 200, body: "OK" };
			};

			return post.send(service, target, [ { key: key, data: `AZERTY${key}` } ]);
		}).then(() => {
			done();
		}).catch((err) => {
			done(err);
		});
	});

	it('store item (with / prefix)', (done) => {
		const key     = uuid();
		let   service = null;
		const target  = { destination: `search-test-abcdefghijklmnopqrstuvwxyz.us-east-1.es.amazonaws.com#/type/index` };

		Promise.resolve(null).then(() => {
			return post.create(target);
		}).then((s) => {
			service = s;
		}).then(() => {
			processRequest = (httpRequest) => {
				assert.strictEqual(httpRequest.headers.Host, target.destination.split('#')[0]);
				assert.strictEqual(httpRequest.region, process.env.AWS_REGION);
				assert.strictEqual(httpRequest.headers['x-amz-security-token'], process.env.AWS_SESSION_TOKEN);
				const auth = parseAuthorization(httpRequest.headers.Authorization);
				const when = new Date().toISOString().substr(0, 10).replace(/-/g, '');
				assert.strictEqual(auth.type, 'AWS4-HMAC-SHA256');
				assert.strictEqual(auth.params.Credential, `${process.env.AWS_ACCESS_KEY_ID}/${when}/${process.env.AWS_REGION}/es/aws4_request`);

				const bodyParts = httpRequest.body.toString('utf-8').split('\n');
				assert.strictEqual(bodyParts.length, 3);
				assert.deepEqual(JSON.parse(bodyParts[0]), { index: { _id: `${key}` } });
				assert.deepEqual(JSON.parse(bodyParts[1]), { data: `AZERTY${key}` });
				assert.strictEqual(bodyParts[2], "");
				return { statusCode: 200, body: "OK" };
			};

			return post.send(service, target, [ { key: key, data: `AZERTY${key}` } ]);
		}).then(() => {
			done();
		}).catch((err) => {
			done(err);
		});
	});

	it('store items', (done) => {
		const key     = uuid();
		let   service = null;
		const target  = { destination: `search-test-abcdefghijklmnopqrstuvwxyz.us-east-1.es.amazonaws.com#type/index` };

		Promise.resolve(null).then(() => {
			return post.create(target);
		}).then((s) => {
			service = s;
		}).then(() => {
			processRequest = (httpRequest) => {
				assert.strictEqual(httpRequest.headers.Host, target.destination.split('#')[0]);
				assert.strictEqual(httpRequest.region, process.env.AWS_REGION);
				assert.strictEqual(httpRequest.headers['x-amz-security-token'], process.env.AWS_SESSION_TOKEN);
				const auth = parseAuthorization(httpRequest.headers.Authorization);
				const when = new Date().toISOString().substr(0, 10).replace(/-/g, '');
				assert.strictEqual(auth.type, 'AWS4-HMAC-SHA256');
				assert.strictEqual(auth.params.Credential, `${process.env.AWS_ACCESS_KEY_ID}/${when}/${process.env.AWS_REGION}/es/aws4_request`);

				const bodyParts = httpRequest.body.toString('utf-8').split('\n');
				assert.strictEqual(bodyParts.length, 5);
				assert.deepEqual(JSON.parse(bodyParts[0]), { index: { _id: `${key}#1` } });
				assert.deepEqual(JSON.parse(bodyParts[1]), { data: `AZERTY${key}` });
				assert.deepEqual(JSON.parse(bodyParts[2]), { index: { _id: `${key}#2` } });
				assert.deepEqual(JSON.parse(bodyParts[3]), { data: `QWERTY${key}` });
				assert.strictEqual(bodyParts[4], "");
				return { statusCode: 200, body: "OK" };
			};

			return post.send(service, target, [ { key: `${key}#1`, data: `AZERTY${key}` }, { key: `${key}#2`, data: `QWERTY${key}` } ]);
		}).then(() => {
			done();
		}).catch((err) => {
			done(err);
		});
	});

	it('invalid status', (done) => {
		const key     = uuid();
		let   service = null;
		const target  = { destination: `search-test-abcdefghijklmnopqrstuvwxyz.us-east-1.es.amazonaws.com#type/index` };

		Promise.resolve(null).then(() => {
			return post.create(target);
		}).then((s) => {
			service = s;
		}).then(() => {
			processRequest = (httpRequest) => {
				assert.strictEqual(httpRequest.headers.Host, target.destination.split('#')[0]);
				assert.strictEqual(httpRequest.region, process.env.AWS_REGION);
				assert.strictEqual(httpRequest.headers['x-amz-security-token'], process.env.AWS_SESSION_TOKEN);
				const auth = parseAuthorization(httpRequest.headers.Authorization);
				const when = new Date().toISOString().substr(0, 10).replace(/-/g, '');
				assert.strictEqual(auth.type, 'AWS4-HMAC-SHA256');
				assert.strictEqual(auth.params.Credential, `${process.env.AWS_ACCESS_KEY_ID}/${when}/${process.env.AWS_REGION}/es/aws4_request`);

				const bodyParts = httpRequest.body.toString('utf-8').split('\n');
				assert.strictEqual(bodyParts.length, 3);
				assert.deepEqual(JSON.parse(bodyParts[0]), { index: { _id: `${key}` } });
				assert.deepEqual(JSON.parse(bodyParts[1]), { data: `AZERTY${key}` });
				assert.strictEqual(bodyParts[2], "");
				return { statusCode: 503, body: "OK" };
			};

			return post.send(service, target, [ { key: key, data: `AZERTY${key}` } ]);
		}).then(() => {
			done(new Error("An error should have been raised"));
		}).catch((err) => {
			assert(err instanceof Error);
			assert.strictEqual(err.message, "Error posting to Amazon ElasticSearch: HTTP Status Code: '503', body 'OK'");
			done();
		}).catch((err) => {
			done(err);
		});
	});

	it('error', (done) => {
		const key     = uuid();
		let   service = null;
		const target  = { destination: `search-test-abcdefghijklmnopqrstuvwxyz.us-east-1.es.amazonaws.com#type/index` };

		Promise.resolve(null).then(() => {
			return post.create(target);
		}).then((s) => {
			service = s;
		}).then(() => {
			processRequest = () => {
				throw new Error("Unable to comply");
			};

			return post.send(service, target, [ { key: key, data: `AZERTY${key}` } ]);
		}).then(() => {
			done(new Error("An error should have been raised"));
		}).catch((err) => {
			assert(err instanceof Error);
			assert.strictEqual(err.message, "Error posting to Amazon ElasticSearch: 'Error: Unable to comply'");
			done();
		}).catch((err) => {
			done(err);
		});
	});
});
