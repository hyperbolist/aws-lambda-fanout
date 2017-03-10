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
 * This Node.js library manages the Amazon ElastiCache Redis protocol.
 */
'use strict';

// Modules
const net = require('net');

const newLine = new Buffer("\r\n", 'utf-8');

function extractHashTag(key) {
	const bracketIndex = key.indexOf("{");
	if (bracketIndex != -1) {
		const endBracket = key.indexOf("}", bracketIndex+1);
		if((endBracket != -1) && (endBracket != (bracketIndex + 1))) {
			return key.substring(bracketIndex + 1, endBracket);  
		}
	}
	return key;
}

// Serializes an object to be sent to redis
function serialize(object) {
	if(object === null || object === undefined) {
		// No support for serializing Null array, all nulls will be serialized as a null bulk string
		return new Buffer("$-1\r\n", 'utf-8');
	} else if(Array.isArray(object)) {
		const values = object.map(serialize);
		return Buffer.concat([ new Buffer(`*${values.length}`, 'utf-8'), newLine ].concat(values));
	} else if(typeof(object) == "string") {
		let isBinary = false;
		for(let i = 0; i < object.length; ++i) {
			if(object.charCodeAt(i) < 32 || object.charCodeAt(i) >= 128) {
				isBinary = true;
			}
		}
		if(! isBinary) {
			return new Buffer(`+${object}\r\n`, 'utf-8');
		} else {
			// This is a string with control characters, process it as a binary stream
			const value = new Buffer(object, 'utf-8');
			return serialize(value);
		}
	} else if(Buffer.isBuffer(object)) {
		return Buffer.concat([ new Buffer(`\$${object.length}`, 'utf-8'), newLine, object, newLine ]);
	} else if((typeof object == "number") && (!isNaN(object)) && (object != Infinity) && (object != -Infinity) && (Math.floor(object) == object)) {
		return new Buffer(`\:${object}\r\n`, 'utf-8');
	} else if(object instanceof Error) {
		const message = object.message;
		let value = "";
		for(let i = 0; i < message.length; ++i) {
			if(message.charCodeAt(i) >= 32 && message.charCodeAt(i) < 128) {
				value += message.charAt(i);
			} else {
				value += " ";
			}
		}
		return new Buffer(`\-${value}\r\n`, 'utf-8');
	} else {
		throw new Error("Unsupported data type");
	}
}

// Deserializes a response from redis
function deserialize(buffer, index) {
	if(index === null || index === undefined) {
		index = 0;
	}
	if(! Buffer.isBuffer(buffer)) {
		throw new Error("You must provide a buffer");
	}
	if(buffer.length == index) {
		return { result: null, processed: false, needMore: true, index: index };
	}
	if(buffer.length < index) {
		throw new Error("Trying to read past end of buffer");
	}

	const typeCode = buffer[index];
	if((typeCode < 32) || (typeCode >= 128)) {
		const hexCode = buffer.slice(index, index+1).toString('hex');
		throw new Error(`Unknown object type 0x${hexCode}`);
	}

	const objectType = String.fromCharCode(typeCode);
	switch(objectType) {
		case '+': { // Simple String
			// Read line content until end of line
			const endIndex = buffer.indexOf(newLine, index);
			if(endIndex == -1) {
				return { result: null, processed: false, needMore: true, index: index };
			}
			return { result: buffer.slice(index+1, endIndex).toString('utf-8'), processed: true, needMore: false, index: endIndex + 2 };
		}
		case ':': { // Integer
			// Read line content until end of line, and cast as int
			const endIndex = buffer.indexOf(newLine, index);
			if(endIndex == -1) {
				return { result: null, processed: false, needMore: true, index: index };
			}
			const value = buffer.slice(index+1, endIndex).toString('utf-8');
			const result = parseInt(value);
			if(isNaN(result)) {
				throw new Error(`Invalid integer value '${value}' supplied in response`);
			}
			return { result: result, processed: true, needMore: false, index: endIndex + 2 };
		}
		case '-': { // Error
			// Read line content until end of line, and create error with that message
			const endIndex = buffer.indexOf(newLine, index);
			if(endIndex == -1) {
				return { result: null, processed: false, needMore: true, index: index };
			}
			return { result: new Error(buffer.slice(index+1, endIndex).toString('utf-8')), processed: true, needMore: false, index: endIndex + 2 };
		}
		case '$': { // Bulk String
			// Read line content until end of line, and cast as int to get content size
			const endIndex = buffer.indexOf(newLine, index);
			if(endIndex == -1) {
				return { result: null, processed: false, needMore: true, index: index };
			}
			const length = parseInt(buffer.slice(index+1, endIndex).toString('utf-8'));
			if(isNaN(length) || length < -1) {
				throw new Error("Invalid length supplied in response");
			} else if(length == -1) {
				// Null bulk string, there is no value provided
				return { result: null, processed: true, needMore: false, index: endIndex + 2 };
			}
			if(buffer.length < (endIndex + 2) + (length + 2)) {
				return { result: null, processed: false, needMore: true, index: index };
			}
			if(buffer.slice((endIndex + 2) + length, (endIndex + 2) + (length + 2)).compare(newLine) !== 0) {
				throw new Error("Invalid data in buffer");
			}
			return { result: buffer.slice((endIndex + 2), (endIndex + 2) + length), processed: true, needMore: false, index: (endIndex + 2) + (length + 2) };
		}
		case '*': { // Array
			// Read line content until end of line, and cast as int to get content size
			const endIndex = buffer.indexOf(newLine, index);
			if(endIndex == -1) {
				return { result: null, processed: false, needMore: true, index: index };
			}
			const length = parseInt(buffer.slice(index+1, endIndex).toString('utf-8'));
			if(isNaN(length) || length < -1) {
				throw new Error("Invalid length supplied in response");
			} else if(length == -1) {
				// Null array, there is no value provided
				return { result: null, processed: true, needMore: false, index: endIndex + 2 };
			}
			let newIndex = endIndex + 2;
			let results = [];
			for(let i = 0; i < length; ++i) {
				const item = deserialize(buffer, newIndex);
				if(item.needMore) {
					return { result: null, processed: false, needMore: true, index: index };
				} else {
					newIndex = item.index;
					results.push(item.result);
				}
			}
			return { result: results, processed: true, needMore: false, index: newIndex };
		}
		default: {
			throw new Error(`Unknown object type '${objectType}'`);
		}
	}
}

function connect(host, port) {
	return new Promise((resolve, reject) => {
		const client = net.connect({ host: host, port: port });
		client.setEncoding = () => { throw new Error("You can not change the encoding for a redis client"); };
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
}

function sendRequestAndReadResponse(client, request) {
	if(request !== null && request !== undefined) {
		if(!Array.isArray(request)) {
			throw new Error("All requests must be arrays");
		}
		request = request.map((r) => (typeof r) == "string" ? new Buffer(r, 'utf-8'): r);
	}

	return new Promise((resolve, reject) => {
		let data = new Buffer(0);
		if(client.hasOwnProperty('redisData')) {
			data = client.redisData;
			delete client.redisData;
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

			try {
				const response = deserialize(data, 0);
				if(response.processed) {
					if(response.index < data.length) {
						data = data.slice(response.index);
					} else {
						data = new Buffer(0);
					}
					done(null, response.result);
				}
			} catch(e) {
				done(e);                
			}
		};

		const close = () => { done(); };

		done = (err, result) => {
			client.removeListener('close', close);
			client.removeListener('data', processData);
			if(data.length > 0) {
				client.redisData = data;
			}
			if(err) {
				reject(err);
			} else {
				resolve(result);
			}
		};

		client.on('data', processData);
		client.on('close', close);

		if(request !== null && request !== undefined) {
			client.write(serialize(request));
		}

		processData();
	});
}

module.exports.extractHashTag = extractHashTag;
module.exports.deserialize = deserialize;
module.exports.serialize = serialize;
module.exports.connect = connect;
module.exports.sendRequestAndReadResponse = sendRequestAndReadResponse;
