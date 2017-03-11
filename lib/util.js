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
 * This Node.js script provides utility functions javascript object manipulation.
 */

'use strict';

// Load configuration information from environment variables
module.exports.getEnvString = (name, defaultValue) => {
	if(process.env.hasOwnProperty(name)) {
		return process.env[name];
	}
	return defaultValue;
};

module.exports.getEnvNumber = (name, defaultValue) => {
	if(process.env.hasOwnProperty(name)) {
		const valueAsString = process.env[name];
		if(valueAsString.match(/^-?([1-9][0-9]*|[0-9])(.[0-9]+)?$/)) {
			return Number(valueAsString);
		}
	}
	return defaultValue;
};

module.exports.getEnvBool = (name, defaultValue) => {
	if(process.env.hasOwnProperty(name)) {
		const valueAsString = process.env[name].toLowerCase();
		if(valueAsString == "true" || valueAsString == "1" || valueAsString == "on") {
			return true;
		} else if(valueAsString == "false" || valueAsString == "0" || valueAsString == "off") {
			return false;
		}
	}
	return defaultValue;
};

//********
// This function duplicates a simple Javascript object
function duplicate(value) {
	if(value === undefined) {
		return undefined;
	} else if(value === null) {
		return null;
	} else if ((typeof value == "string") || (typeof value == "number") || (typeof value == "boolean") || (value instanceof Date)) {
		return value;
	} else if (Buffer.isBuffer(value)) {
		return new Buffer(value);
	} else if (Array.isArray(value)) {
		return value.map(duplicate);
	} else if (typeof value == "object") {
		const result = Object.create(Object.getPrototypeOf(value));
		const properties = Object.keys(value);
		properties.forEach(function(propertyName) {
			result[propertyName] = duplicate(value[propertyName]);
		});
		return result;
	} else {
		throw new Error("Unsupported value type");
	}
}

function applyChanges(destination, source) {
	let dest = destination;
	if(typeof dest !== "object") {
		dest = {};
	}

	if(typeof source == "object") {
		for(let key in source) {
			const value = source[key];
			if ((value === undefined) || (value === null) || (typeof value == "string") || (typeof value == "number") || (typeof value == "boolean") || Buffer.isBuffer(value) || (value instanceof Date)) {
				dest[key] = value;
			} else if (Array.isArray(value)) {
				if(key.endsWith("Append")) {
					const srcKey = key.substr(0, key.length - "Append".length);
					if(! Array.isArray(dest[srcKey])) {
						dest[srcKey] = [];
					}
					dest[srcKey] = dest[srcKey].concat(value);
				} else if(key.endsWith("Prepend")) {
					const srcKey = key.substr(0, key.length - "Prepend".length);
					if(! Array.isArray(dest[srcKey])) {
						dest[srcKey] = [];
					}
					dest[srcKey] = value.concat(dest[srcKey]);
				} else {
					dest[key] = value;
				}
			} else if (typeof value == "object" && Object.getPrototypeOf(value) == Object.prototype) {
				dest[key] = applyChanges(dest[key], value);
			} else {
				dest[key] = value;
			}
		}
	}
	return dest;
}

module.exports.duplicate = duplicate;
module.exports.applyChanges = applyChanges;