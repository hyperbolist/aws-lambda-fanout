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
 * This Node.js script provides a memcached compliant host for testing.
 */
'use strict';

const net = require('net');
const EventEmitter = require('events');

const debug = process.env.DEBUG_MODE == "true";
const MAX_DELTA_EXPIRATION_TIME=  60*60*24*30; // if expiration time <= 30 days, will be a delta, if not will be a unix epoch
const newLine = new Buffer("\r\n", 'utf-8');

function deserializeCommand(buffer) {
	const endIndex = buffer.indexOf(newLine);
	if(endIndex == -1) {
		return { result: null, processed: false, needMore: true, index: 0, error: null };
	}

	const parts = buffer.slice(0, endIndex).toString('utf-8').split(" ");
	const command = parts[0];
	switch(command) {
		case "echo": {
			if(parts.length != 2) {
				return { result: null, processed: false, needMore: false, index: (endIndex + 2), error: "CLIENT_ERROR received badly formatted command" };
			}

			const bytes = Number(parts[1]);
			if(isNaN(bytes) || bytes < 0) {
				return { result: null, processed: false, needMore: false, index: (endIndex + 2), error: "CLIENT_ERROR received invalid command arguments" };
			}

			if(buffer.length < ((endIndex + 2) + (bytes + 2))) {
				return { result: null, processed: false, needMore: true, index: 0, error: null };
			}

			if(buffer.slice((endIndex + 2) + bytes, (endIndex + 2) + (bytes + 2)).compare(newLine) !== 0) {
				return { result: null, processed: false, needMore: false, index: (endIndex + 2), error: "CLIENT_ERROR received invalid command data" };
			}

			/* !! WARNING: we don't add a default newline at the end, it has to be included in the initial data !! */
			const value = buffer.slice(endIndex + 2, (endIndex + 2) + bytes);
			return { command: command, value: value, processed: true, needMore: false, index: (endIndex + 2) + (bytes + 2), error: null };
		}
		case "set":
		case "add":
		case "replace": {
			if(parts.length != 5 && parts.length != 6) {
				return { result: null, processed: false, needMore: false, index: (endIndex + 2), error: "CLIENT_ERROR received badly formatted command" };
			}

			const key = parts[1];                                                                    // Cache entry key: no control characters or whitespace
			const flags = parts[2].match(/^([1-9][0-9]{1,5}|[0-9])$/) ? parseInt(parts[2]) : NaN;    // Arbitrary flags
			let   expiration = parts[3].match(/^-?([1-9][0-9]+|[0-9])$/) ? parseInt(parts[3]) : NaN; // Expiration time
			const bytes = parts[4].match(/^([1-9][0-9]+|[0-9])$/) ? parseInt(parts[4]) : NaN;        // Data size
			const noReply = (parts.length == 6) && (parts[5] == "noreply");                          // Don't send a response

			if(isNaN(flags) || (flags > 65535) || isNaN(expiration) || isNaN(bytes) || ((parts.length == 6) && (parts[5] != "noreply"))) {
				return { result: null, processed: false, needMore: false, index: (endIndex + 2), error: "CLIENT_ERROR received invalid command arguments" };
			}

			if(key.length == 0 || key.length > 250) {
				return { result: null, processed: false, needMore: false, index: (endIndex + 2), error: "CLIENT_ERROR received invalid command arguments" };
			}

			for(let j = 0; j < key.length; ++j) {
				const code = key.charCodeAt(j);
				if(code <= 32 || code >= 128) {
					return { result: null, processed: false, needMore: false, index: (endIndex + 2), error: "CLIENT_ERROR received invalid command arguments" };
				}
			}

			if(buffer.length < ((endIndex + 2) + (bytes + 2))) {
				return { result: null, processed: false, needMore: true, index: 0 };
			}

			if(buffer.slice((endIndex + 2) + bytes, (endIndex + 2) + (bytes + 2)).compare(newLine) !== 0) {
				return { result: null, processed: false, needMore: false, index: (endIndex + 2), error: "CLIENT_ERROR received invalid command data" };
			}

			const value = buffer.slice(endIndex + 2, (endIndex + 2) + bytes);
			if(expiration < 0) {
				expiration = Date.now() - 1000; // Force expiration to 1 second ago
			} else if(expiration === 0) {
				expiration = Number.MAX_VALUE;  // Never expires
			} else if(expiration <= MAX_DELTA_EXPIRATION_TIME) {
				expiration = Date.now() + (expiration * 1000); // Expires n seconds from now
			} else {
				expiration = expiration * 1000; // Expires at the specified epoch second
			}
			return { command: command, key: key, value: value, expiration: expiration, flags: flags, noReply: noReply, processed: true, needMore: false, index: (endIndex + 2) + (bytes + 2), error: null };
		}
		case "get":
		case "gets": {
			if(parts.length < 2) {
				return { result: null, processed: false, needMore: false, index: (endIndex + 2), error: "CLIENT_ERROR received badly formatted command" };
			}
			const keys = parts.slice(1);
			for(let i = 0; i < keys.length; ++i) {
				const key = keys[i];
				if(key.length == 0 || key.length > 250) {
					return { result: null, processed: false, needMore: false, index: (endIndex + 2), error: "CLIENT_ERROR received invalid command arguments" };
				}
				for(let j = 0; j < key.length; ++j) {
					const code = key.charCodeAt(j);
					if(code <= 32 || code >= 128) {
						return { result: null, processed: false, needMore: false, index: (endIndex + 2), error: "CLIENT_ERROR received invalid command arguments" };
					}
				}
			}
			return { command: command, keys: keys, processed: true, needMore: false, index: (endIndex + 2), error: null };
		}
		case "config": {
			if(parts.length < 2) {
				return { result: null, processed: false, needMore: false, index: (endIndex + 2), error: "CLIENT_ERROR received badly formatted command" };
			}
			const parameters = parts.slice(1);
			return { command: command, parameters: parameters, processed: true, needMore: false, index: (endIndex + 2), error: null };
		}
		case "exit":
		case "quit": {
			return { command: command, processed: true, needMore: false, index: (endIndex + 2), error: null };
		}
		default: {
			return { result: null, processed: false, needMore: false, index: (endIndex + 2), error: "CLIENT_ERROR received unsupported command" };
		}
	}
}

function handleProcessConnection(c, index, config, cacheData) {
	const id = (++config.clientId);
	let data = new Buffer(0);
	/* istanbul ignore next */
	if(debug) {
		console.log(`shard[${index}]#${id}: Data client connected`);
	}

	const processData = (chunk) => {
		if(Buffer.isBuffer(chunk)) {
			if(data.length == 0) {
				data = chunk;
			} else {
				data = Buffer.concat([data, chunk]);
			}
		}

		/* istanbul ignore next */
		if(debug) {
			console.log(`shard[${index}]#${id}: received command data`);
		}

		let request = deserializeCommand(data, 0);
		if(request.needMore) {
			// Wait for more data
			return;
		}
		if(request.index < data.length) {
			// Request processed, remove from data
			data = data.slice(request.index);
		} else {
			data = new Buffer(0);
		}
		if(request.error) {
			c.write(`${request.error}\r\n`);
			setImmediate(processData);
			return;
		}

		const cas = (++config.commandId);

		if(config.nextResponses.length > 0) {
			const response = config.nextResponses.shift();
			c.write(response);
			setImmediate(processData);
			return;
		}

		const command = request.command;
		switch(request.command) {
			case "echo": {
				c.write(request.value);
				setImmediate(processData);
				break;
			}
			case "set": {
				let result = null;
				cacheData[request.key] = { key: request.key, value: request.value, expiration: request.expiration, flags: request.flags, cas: cas };
				result = "STORED";
				if (! request.noReply) {
					c.write(`${result}\r\n`);
				}
				setImmediate(processData);
				break;
			}
			case "add": {
				let result = null;
				if(! cacheData.hasOwnProperty(request.key)) {
					cacheData[request.key] = { key: request.key, value: request.value, expiration: request.expiration, flags: request.flags, cas: cas };
					result = "STORED";
				} else {
					result = "NOT_STORED";
				}
				if (! request.noReply) {
					c.write(`${result}\r\n`);
				}
				setImmediate(processData);
				break;
			}
			case "replace": {
				let result = null;
				if(cacheData.hasOwnProperty(request.key)) {
					cacheData[request.key] = { key: request.key, value: request.value, expiration: request.expiration, flags: request.flags, cas: cas };
					result = "STORED";
				} else {
					result = "NOT_STORED";
				}
				if (! request.noReply) {
					c.write(`${result}\r\n`);
				}
				setImmediate(processData);
				break;
			}
			case "get":
			case "gets": {
				request.keys.forEach((key) => {
					if(cacheData.hasOwnProperty(key)) {
						const cacheEntry = cacheData[key];
						if(cacheEntry.expiration < Date.now()) {
							/* istanbul ignore next */
							if(debug) {
								console.error(`shard[${index}]#${id}: expired entry '${key}' for command ${command}`);
							}
							delete cacheData[key];
						} else {
							/* istanbul ignore next */
							if(debug) {
								console.error(`shard[${index}]#${id}: successfuly retrieved entry '${key}' for command ${command}`);
							}
							c.write(`VALUE ${cacheEntry.key} ${cacheEntry.flags} ${cacheEntry.value.length} ${cacheEntry.cas}\r\n`);
							c.write(cacheEntry.value);
							c.write("\r\n");
						}
					} else {
						/* istanbul ignore next */
						if(debug) {
							console.error(`shard[${index}]#${id}: non-exstant entry '${key}' for command ${command}`);
						}
					}
				});
				c.write(`END\r\n`);
				break;
			}
			default: {
				/* istanbul ignore next */
				if(debug) {
					console.error(`shard[${index}]#${id}: received unknown command: ${command}`);
				}
				c.write("CLIENT_ERROR received unsupported command\r\n");
				setImmediate(processData);
				break;
			}
		}
	};

	c.on('data', processData);

	/* istanbul ignore next */
	c.on('close', () => {
		if(debug) {
			console.log(`shard[${index}]#${id}: Data client disconnected`);
		}
	});

	/* istanbul ignore next */
	c.on('timeout', () => {
		c.end();
		if(debug) {
			console.log(`shard[${index}]#${id}: Data client timeout`);
		}
	});

	/* istanbul ignore next */
	c.on('error', (err) => {
		c.end();
		c.destroy();
		if(debug) {
			console.log(`shard[${index}]#${id}: Data client error ${err}`);
		}
	});
}

function createProcessServer(index, config) {
	const cacheData = {};
	let valid = false;
	return new Promise((resolve, reject) => {
		const server = net.createServer();
		server.on('connection', (c) => handleProcessConnection(c, index, config, cacheData));

		/* istanbul ignore next */
		server.on('error', (err) => {
			if(debug) {
				console.log(`Server error, closing`, err);
			}
			if(! valid) {
				reject(new Error(`Unable to setup data server #${index}: ${err}`));
			}
		}); 

		server.on('listening', () => {
			/* istanbul ignore next */
			if(debug) {
				const port = server.address().port;
				console.log(`Data server #${index} successfuly setup, listening on port #${port}`);
			}
			resolve(server);
			valid = true;
		});

		server.on('close', () => {
			/* istanbul ignore next */
			if(debug) {
				console.log(`Data server #${index} stopping`);
			}
		});

		server.listen();
	});
}

function handleConfigConnection(c, config) {
	const id = (++config.clientId);
	let data = new Buffer(0);
	const newLine = new Buffer("\r\n", 'utf-8');
	/* istanbul ignore next */
	if(debug) {
		console.log(`config#${id}: Config client connected`);
	}

	const processData = (chunk) => {
		if(Buffer.isBuffer(chunk)) {
			if(data.length == 0) {
				data = chunk;
			} else {
				data = Buffer.concat([data, chunk]);
			}
		}

		/* istanbul ignore next */
		if(debug) {
			console.log(`config#${id}: received config command data`);
		}

		let request = deserializeCommand(data, 0);
		if(request.needMore) {
			// Wait for more data
			return;
		}
		if(request.index < data.length) {
			// Request processed, remove from data
			data = data.slice(request.index);
		} else {
			data = new Buffer(0);
		}
		if(request.error) {
			c.write(`${request.error}\r\n`);
			setImmediate(processData);
			return;
		}

		switch(request.command) {
			case "config": {
				if(request.parameters.join(" ") == "get cluster") {
					/* istanbul ignore next */
					if(debug) {
						console.log(`config#${id}: received cluster configuration description command`);
					}
					if(config.nextResponses.length > 0) {
						const response = config.nextResponses.shift();
						c.write(response);
					} else {
						const portsString = config.processPorts.map((p) => `localhost|127.0.0.1|${p}`).join(" ");
						const configString = `${config.version}\n${portsString}\n`;
						c.write(`CONFIG cluster 0 ${configString.length}\r\n`);
						c.write(`${configString}\r\n`);
						c.write("END\r\n");
					}
				} else {
					c.write("CLIENT_ERROR received unsupported command\r\n");
				}
				setImmediate(processData);
				break;
			}
			case "quit":
			case "exit": {
				/* istanbul ignore next */
				if(debug) {
					console.log(`config#${id}: received cluster shutdown`);
				}
				c.end();
				config.stop();
				break;
			}
			default: {
				/* istanbul ignore next */
				if(debug) {
					console.error(`config#${id}: received unknown command: ${request.command}`);
				}
				c.write("CLIENT_ERROR received unsupported command\r\n");
				setImmediate(processData);
			}
		}
	};

	c.on('data', processData);

	/* istanbul ignore next */
	c.on('close', () => {
		if(debug) {
			console.log(`config#${id}: Config client disconnected`);
		}
	});

	/* istanbul ignore next */
	c.on('timeout', () => {
		if(debug) {
			console.log(`config#${id}: Client timeout`);
		}
	});

	/* istanbul ignore next */
	c.on('error', (err) => {
		c.removeAllListeners();
		c.end();
		c.destroy();
		if(debug) {
			console.log(`config#${id}: Client error ${err}`);
		}
	});
}

function createConfigServer(config) {
	let valid = false;
	return new Promise((resolve, reject) => {
		const server = net.createServer();
		server.on('connection', (c) => handleConfigConnection(c, config));

		/* istanbul ignore next */
		server.on('error', (err) => {
			if(debug) {
				console.error(`Configuration server error, closing`, err);
			}
			if(! valid) {
				reject(new Error(`Unable to setup configuration server: ${err}`));
			}
		});

		server.on('listening', () => {
			/* istanbul ignore next */
			if(debug) {
				const port = server.address().port;
				console.log(`Config server successfuly setup, listening on port #${port}`);
			}
			resolve(server);
			valid = true;
		});

		server.on('close', () => {
			/* istanbul ignore next */
			if(debug) {
				console.log(`Config server stopping`);
			}
		});

		if(config.configPort != 0) {
			server.listen(config.configPort);
		} else {
			server.listen();
		}
	});
}

function MemcachedConfig() {
	EventEmitter.call(this);
	this.clientId = 0;
	this.commandId = 0;
	this.version = 1;
	this.shards = 1;
	this.configPort = 0;
	this.processPorts = [];
	this.servers = [];
	this.nextResponses = [];
}

MemcachedConfig.prototype = Object.create(EventEmitter.prototype); 

MemcachedConfig.prototype.read = function(options) {
	if((options !== null) && (options !== undefined)) {
		if(options.hasOwnProperty('shards') && (options.shards !== null) && (options.shards !== undefined)) {
			if(((typeof options.shards) != "number") || (options.shards <= 0) || (options.shards > 20)) {
				throw new Error("The number of shards must be a positive integer up to 20");
			}
			this.shards = options.shards;
		}
		if(options.hasOwnProperty('configPort') && (options.configPort !== null) && (options.configPort !== undefined)) {
			if(((typeof options.configPort) != "number") || (options.configPort < 0) || (options.configPort > 65535)) {
				throw new Error("The configuration port number must be a valid TCP port number [1-65535] or 0");
			}
			this.configPort = options.configPort;
		}
	}
}

MemcachedConfig.prototype.stop = function() {
	this.servers.forEach((server) => server.close());
	this.emit('stopped');
}

module.exports = (options) => {
	const config = new MemcachedConfig();
	config.read(options);

	const processes = [];
	processes.push(createConfigServer(config));
	for(let i = 0; i < config.shards; ++i) {
		processes.push(createProcessServer(i, config));
	}

	return Promise.all(processes).then((servers) => {
		/* istanbul ignore next */
		if(debug) {
			console.log("All processes started, ready to serve");
		}
		config.servers = servers;
		config.configPort = servers[0].address().port;
		config.processPorts = servers.slice(1).map((server) => server.address().port);
		config.version++;
		return config;
	});
};
