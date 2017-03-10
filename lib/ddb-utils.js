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
 * This Node.js script provides utility functions for Amazon DynamoDB and Amazon DynamoDB Streams.
 */

'use strict';

//********
// This function duplicates a simple Javascript object
function duplicateObject(value) {
	if(value === null) {
		return null;
	} else if (((typeof value) == "string") || ((typeof value) == "number") || ((typeof value) == "boolean")) {
		return value;
	} else if (Buffer.isBuffer(value)) {
		return new Buffer(value);
	} else if (Array.isArray(value)) {
		return value.map(duplicateObject);
	} else if (value instanceof Date) {
		return value;
	} else if ((typeof value) == "object") {
		const result = Object.create(Object.getPrototypeOf(value));
		const properties = Object.keys(value);
		properties.forEach(function(propertyName) {
			result[propertyName] = duplicateObject(value[propertyName]);
		});
		return result;
	} else {
		throw new Error("Unsupported value type");
	}
}

//********
// This function generates a DynamoDB object
function generateDynamoDBProperty(value) {
	if(value === null) {
		return { NULL: true };
	} else if ((typeof value) == "string") {
		if(value.length === 0) {
			return { NULL: true };
		} else {
			return { S: value };
		}
	} else if ((typeof value) == "number") {
		return { N: value.toString() };
	} else if ((typeof value) == "boolean") {
		return { BOOL: value };
	} else if (Buffer.isBuffer(value)) {
		return { B: value.toString('base64') };
	} else if (Array.isArray(value)) {
		if(value.every((val) => (typeof val) == "string")) {
			return { SS: [].concat(value) };
		} else if(value.every((val) => (typeof val) == "number")) {
			return { NS: value.map((val) => val.toString()) };
		} else if(value.every((val) => Buffer.isBuffer(val))) {
			return { BS: value.map((val) => val.toString('base64')) };
		} else {
			return { L: value.map(generateDynamoDBProperty) };
		}
	} else if (value instanceof Date) {
		return { S: value.toISOString() };
	} else if ((typeof value) == "object") {
		const result = { M: {} };
		const properties = Object.keys(value);
		properties.forEach(function(propertyName) {
			result.M[propertyName] = generateDynamoDBProperty(value[propertyName]);
		});
		return result;
	} else {
		throw new Error("Unsupported value type");
	}
}

//********
// This function generates a DynamoDB object
function generateDynamoDBObject(value) {
	const result = {};
	if ((value === null) || (value === undefined)) {
		return result;
	}

	if (((typeof value) !== "object") || (Buffer.isBuffer(value)) || (Array.isArray(value)) || (value instanceof Date)) {
		throw new Error("Unsupported entry, expecting object");
	}

	const properties = Object.keys(value);
	properties.forEach(function(propertyName) {
		result[propertyName] = generateDynamoDBProperty(value[propertyName]);
	});
	return result;
}

//********
// This function transforms an object from an Amazon DynamoDB format to a Javascript object
function parseDynamoDBObject(value, defaultValues, options) {
	const result = duplicateObject(defaultValues || {});
	if ((value === null) || (value === undefined)) {
		return result;
	}

	if (((typeof value) !== "object") || (Buffer.isBuffer(value)) || (Array.isArray(value)) || (value instanceof Date)) {
		throw new Error("Unsupported entry, expecting object");
	}

	const properties = Object.keys(value);
	properties.forEach(function(propertyName) {
		result[propertyName] = parseDynamoDBPropertyValue(value[propertyName], options);
	});
	return result;
}

//********
// This function transforms the value of an Amazon DynamoDB Object property
function parseDynamoDBPropertyValue(value, options) {
	if(value === null || value === undefined) {
		throw new Error("Can not process null or undefined properties");
	}
	const properties = Object.keys(value);
	if(properties.length === 0) {
		throw new Error("Can not process empty properties");
	}
	const dataType = properties[0];
	switch(dataType) {
		case "S": {
			return value.S;
		}
		case "B": {
			if((options !== null) && (options !== undefined) && options.hasOwnProperty('base64Buffers') && options.base64Buffers) {
				return value.B;
			} else {
				return new Buffer(value.B, 'base64');
			}
		}
		case "N": {
			return Number(value.N);
		}
		case "NULL": {
			return null;
		}
		case "BOOL": {
			return value.BOOL;
		}
		case "NS": {
			return value.NS.map(function(entry) {
				return Number(entry);
			});
		}
		case "SS": {
			return value.SS;
		}
		case "BS": {
			if((options !== null) && (options !== undefined) && options.hasOwnProperty('base64Buffers') && options.base64Buffers) {
				return value.BS;
			} else {
				return value.BS.map(function(entry) {
					return new Buffer(entry, 'base64');
				});
			}
		}
		case "L": {
			return value.L.map(function(entry) {
				return parseDynamoDBPropertyValue(entry);
			});
		}
		case "M": {
			const result = {};
			const mapProperties = Object.keys(value.M);
			mapProperties.forEach(function(propertyName) {
				result[propertyName] = parseDynamoDBPropertyValue(value.M[propertyName]);
			});
			return result;
		}
		default: {
			throw new Error("Unknown property type " + dataType);
		}
	}
}

exports.duplicateObject = duplicateObject;
exports.generateDynamoDBProperty = generateDynamoDBProperty;
exports.generateDynamoDBObject = generateDynamoDBObject;
exports.parseDynamoDBObject = parseDynamoDBObject;
exports.parseDynamoDBPropertyValue = parseDynamoDBPropertyValue;