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

'use strict';

const AWS          = require('aws-sdk');
const sinon        = require('sinon');
const EventEmitter = require('events');

const core  = {};
let   mocks = {};

for (let service in AWS) {
	if (AWS[service].hasOwnProperty('serviceIdentifier')) {
		core[service] = {};
		core[service].clients = [];
		core[service].awsConstructor = AWS[service];

		sinon.stub(AWS, service, (options) => {
			const client = new core[service].awsConstructor(options);
			client.sandbox = sinon.sandbox.create();
			core[service].clients.push(client);
			updateClients(service);
			return client;
		});
	}
}

function MockRequest(mock, params) {
	EventEmitter.call(this);
	this.mock   = mock;
	this.params = params;
}

MockRequest.prototype = Object.create(EventEmitter.prototype); 

MockRequest.prototype.evalMock = function() {
	if (typeof(this.mock.data) === 'function') {
		return new Promise((resolve, reject) => {
			let wasPromise = false;
			const done = (err, result) => {
				if(wasPromise) {
					throw new Error("Either use the callback or return a promise, don't do both");
				}

				if(err) {
					reject(err);
				} else {
					resolve(result);
				}
			};

			try
			{
				const result = this.mock.data(this.params, done);
				if(result instanceof Promise) {
					wasPromise = true;
					result.then(resolve, reject);
				} else {
					resolve(result);
				}
			}
			catch(e) {
				reject(e);
			}
		});
	} else if(this.mock.data instanceof Promise)  {
		return this.mock.data;
	} else {
		return Promise.resolve(this.mock.data);
	}
};

MockRequest.prototype.promise = function() {
	return this.evalMock().then((result) => {
		this.emit('success', result);
		this.emit('complete', result);
		return Promise.resolve(result);
	}).catch((err) => {
		this.emit('error', err);
		this.emit('complete', err);
		return Promise.reject(err);
	});
};

MockRequest.prototype.send = function(callback) {
	if(callback) {
		this.promise().then((result) => {
			this.callback.apply(null, [null, result]);
		}).catch((err) => {
			this.callback.apply(null, [err, null]);
		});
	} else {
		this.promise();
	}
};

function updateClients(service) {
	const services = [];
	if (service) {
		services.push(service);
	} else {
		for (let svc in core) {
			services.push(svc);
		}
	}

	services.forEach((service) => {
		core[service].clients.forEach((client) => {
			client.sandbox.restore();
			applyMocks(client, service);
		});
	});
}

function applyMocks(client, service) {
	if (! mocks.hasOwnProperty(service)) {
		return;
	}

	mocks[service].forEach((mock) => {
		client.sandbox.stub(client, mock.method, (params, callback) => {
			const request = new MockRequest(mock, params);
			if (typeof callback == 'function') {
				request.send(callback);
			}
			return request;
		});
	});
}

AWS.mock = (service, method, data) => {
	if (service === undefined || service === null) {
		throw new Error(`You must specify a service name to mock`);
	}

	if (! core.hasOwnProperty(service)) {
		throw new Error(`Service "${service}" could not be found in the AWS SDK`);
	}

	if (method === undefined || method === null) {
		throw new Error(`You must specify a method name to mock for service "${service}"`);
	}

	if (! mocks.hasOwnProperty(service)) {
		mocks[service] = [];
	}

	const svcMocks = mocks[service];
	const i = svcMocks.map((e) => e.method).indexOf(method);
	if (i !== -1) {
		throw new Error(`A mock has already been defined for method "${method}" of service "${service}"`);
	} else {
		svcMocks.push({ service: service, method: method, data: data });
	}

	updateClients(service);
};

AWS.restore = (service, method) => {
	if (service === undefined || service === null) {
		mocks = {};
		updateClients();
		return;
	}

	if (! mocks.hasOwnProperty(service)) {
		throw new Error(`No mock defined for service "${service}"`);
	}

	if (method === undefined || method === null) {
		delete mocks[service];
		updateClients(service);
		return;
	}

	const svcMocks = mocks[service];
	const i = svcMocks.map((e) => e.method).indexOf(method);
	if (i !== -1) {
		svcMocks.splice(i, 1);
		if(svcMocks.length == 0) {
			delete mocks[service];
		}
	} else {
		throw new Error(`No mock defined for method "${method}" of service "${service}"`);
	}

	updateClients(service);
};

module.exports = AWS;
