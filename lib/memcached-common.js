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
 * This Node.js library provides some common memcached primitives.
 */
'use strict';

// Modules
const net = require('net');
const newLine = new Buffer("\r\n", 'utf-8');

exports.connect = (host, port) => {
	return new Promise((resolve, reject) => {
		const client = net.connect({ host: host, port: port });
		client.setEncoding = () => { throw new Error("You can not change the encoding for a memcached client"); };
		client.on('connect', () => {
			resolve(client);
		});
		/* istanbul ignore next */
		client.on('error', (err) => {
			reject(err);
		});
		/* istanbul ignore next */
		client.on('timeout', () => {
			reject(Error("Timeout occured"));
		});
	});
};

exports.sendRequestAndReadResponse = (client, request) => {
	return new Promise((resolve, reject) => {
		const responseElements = [];
		let data = new Buffer(0);
		if(client.hasOwnProperty('memcacheData')) {
			data = client.memcacheData;
			delete client.memcacheData;
		}

		let done = null;

		const processData = (chunk) => {
			if(Buffer.isBuffer(chunk)) {
				if(data.length == 0) {
					data = chunk;
				} else {
					data = Buffer.concat([data, chunk]);
				}
			}

			const commandEnd = data.indexOf(newLine);
			if(commandEnd != -1) {
				const commandLine = data.slice(0, commandEnd).toString('utf-8');
				const commandParts = commandLine.split(' ');
				const command = commandParts[0];
				switch(command) {
					case "CONFIG":
					case "VALUE": {
						if(commandParts.length == 4 || commandParts.length == 5) {
							const key = commandParts[1];
							const flags = Number(commandParts[2]);
							const bytes = Number(commandParts[3]);
							const cas = commandParts.length == 5 ? Number(commandParts[4]) : null;
							if(data.length >= ((commandEnd + 2) + (bytes + 2))) {
								if(data.slice((commandEnd + 2) + bytes, (commandEnd + 2) + (bytes + 2)).compare(newLine) === 0) {
									const value = data.slice(commandEnd + 2, (commandEnd + 2) + bytes);
									data = data.slice((commandEnd + 2) + (bytes + 2));
									responseElements.push({ code: command, key: key, flags: flags, cas: cas, bytes: bytes, value: value });
									// There must be other elements in the response
									setImmediate(processData);
								} else {
									done(new Error(`Invalid response data received from server for code ${command}`));
								}
							} else {
								// Do nothing, wait for more data
							}
						} else {
							done(new Error(`Invalid response format received from server for code ${command}`));
						}
						break;
					}
					case "ERROR":
					case "STORED":
					case "NOT_STORED":
					case "EXISTS":
					case "NOT_FOUND":
					case "END":
					case "CLIENT_ERROR":
					case "SERVER_ERROR": {
						data = data.slice(commandEnd + 2);
						responseElements.push({ code: command, message: commandParts.slice(1).join(' ') });
						done();
						break;
					}
					default: {
						done(new Error(`Unknown response '${commandLine}'`));
						break;
					}
				}
			}
		};

		const close = () => { done(); };

		done = (err) => {
			client.removeListener('close', close);
			client.removeListener('data', processData);
			if(data.length > 0) {
				client.memcacheData = data;
			}
			if(err) {
				reject(err);
			} else {
				resolve(responseElements);
			}
		};

		client.on('data', processData);
		client.on('close', close);

		if(request) {
			client.write(request);
		}

		processData();
	});
};
