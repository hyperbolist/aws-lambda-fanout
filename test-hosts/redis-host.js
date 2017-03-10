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
 * This Node.js script provides a redis compliant host for testing.
 */
'use strict';

const common = require('../lib/redis-common.js');
const net = require('net');
const uuid = require('uuid');
const EventEmitter = require('events');
const crc16 = require('../lib/crc16.js');

const debug = process.env.DEBUG_MODE == "true";

function handleProcessConnection(c, index, config, allNodes, allSlots, nodesData) {
	let data = new Buffer(0);
	const id = (++config.clientId);
	const nodeData = nodesData[index];
	/* istanbul ignore next */
	if(debug) {
		console.log(`node[${allNodes[index].id}]#${id}: client connected`);
	}

	const processData = (chunk) => {
		if(Buffer.isBuffer(chunk)) {
			if(data.length == 0) {
				data = chunk;
			} else {
				data = Buffer.concat([data, chunk]);
			}
		}

		let request = null;
		try {
			request = common.deserialize(data, 0);
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
		} catch(e) {
			c.write(common.serialize(new Error(`ERR Invalid request sent ${e.message}`)));
			c.end();
			return;
		}

		if(config.nextResponses.length > 0) {
			/* istanbul ignore next */
			if(debug) {
				console.log(`node[${allNodes[index].id}]#${id}: using override response`);
			}
			const response = config.nextResponses.shift();
			c.write(response);
			setImmediate(processData);
			return;
		}

		if((! Array.isArray(request.result)) || (request.result.length === 0)) {
			c.write(common.serialize(new Error("ERR Redis expects a non-empty array as a command")));
			setImmediate(processData);
			return;
		}

		if(! Buffer.isBuffer(request.result[0])) {
			c.write(common.serialize(new Error(`ERR The first element of a command must be a buffer`)));
			setImmediate(processData);
			return;
		}
		const command = request.result[0].toString('utf-8');

		/* istanbul ignore next */
		if(debug) {
			console.log(`node[${allNodes[index].id}]#${id}: received command '${command}'`);
		}

		switch(command) {
			case "EXIT":
			case "QUIT": {
				c.end();
				config.stop();
				break;
			}
			case "PING": {
				if(request.result.length == 1) {
					c.write(common.serialize(new Buffer("PONG", "utf-8")));
					setImmediate(processData);
					return;
				}

				if(request.result.length > 2) {
					c.write(common.serialize(new Error(`ERR The '${command}' command only accepts up to one message`)));
					setImmediate(processData);
					return;
				}

				if(! Buffer.isBuffer(request.result[1])) {
					c.write(common.serialize(new Error(`ERR The message of a '${command}' command must be a bulk string`)));
					setImmediate(processData);
					return;
				}

				c.write(common.serialize(request.result[1]));
				setImmediate(processData);
				break;
			}
			case "ECHO": {
				if(request.result.length != 2) {
					c.write(common.serialize(new Error(`ERR The '${command}' command only accepts one message`)));
					setImmediate(processData);
					return;
				}

				if(! Buffer.isBuffer(request.result[1])) {
					c.write(common.serialize(new Error(`ERR The message of a '${command}' command must be a bulk string`)));
					setImmediate(processData);
					return;
				}

				c.write(common.serialize(request.result[1]));
				setImmediate(processData);
				break;
			}
			case "CLUSTER": {
				if(request.result.length == 1) {
					c.write(common.serialize(new Error(`ERR Redis '${command}' command expects a sub-command`)));
					setImmediate(processData);
					return;
				}

				if(! Buffer.isBuffer(request.result[1])) {
					c.write(common.serialize(new Error(`ERR The sub-command element of a '${command}' command must be a bulk string`)));
					setImmediate(processData);
					return;
				}
				const subCommand = request.result[1].toString('utf-8');

				switch(subCommand) {
					case "SLOTS": {
						c.write(common.serialize(allSlots.map((s) => {
							const entries = s.filter((e) => e.master).concat(s.filter((e) => ! e.master));
							const values = entries.map((e) => [ "127.0.0.1", e.node.port, e.node.id ]);
							return [ s.slot.start, s.slot.end ].concat(values);
						})));
						setImmediate(processData);
						break;
					}
					default: {
						c.write(common.serialize(new Error(`ERR Unknown command '${command} ${subCommand}'`)));
						setImmediate(processData);
						break;
					}
				}
				break;
			}
			case "MSET": {
				if((request.result.length == 1) || request.result.some((r) => ! Buffer.isBuffer(r)) || (((request.result.length - 1) % 2) != 0)) {
					c.write(common.serialize(new Error(`ERR Redis '${command}' command expects bulk string key-value pairs`)));
					setImmediate(processData);
					return;
				}

				const records = [];
				for(let i = 1; i < request.result.length; i = i+2) {
					const key = request.result[i].toString('utf-8');
					/* istanbul ignore next */
					if(debug) {
						console.log(`node[${allNodes[index].id}]#${id}: - provided key: '${key}'`);
					}
					const value = request.result[i+1];
					const hashKey = common.extractHashTag(key);
					/* istanbul ignore next */
					if(debug && (hashKey != key)) {
						console.log(`node[${allNodes[index].id}]#${id}: - detected hash tag in key: '${key}' becomes '${hashKey}'`);
					}
					const hash = crc16.xmodem(hashKey) % 16384;
					/* istanbul ignore next */
					if(debug) {
						console.log(`node[${allNodes[index].id}]#${id}: - hash computed: ${hash}`);
					}

					// Search if this node is a master for this slot
					let slot = nodeData.slots.find((slotData) => (slotData.slot.start <= hash && slotData.slot.end >= hash));

					if(slot === undefined) {
						/* istanbul ignore next */
						if(debug) {
							console.log(`node[${allNodes[index].id}]#${id}: --> no suitable slot found on this node, bailing out`);
						}
						c.write(common.serialize(new Error(`ERR This node does not host a valid slot for key ${key}`)));
						setImmediate(processData);
						return;
					}

					if(! slot.master) {
						/* istanbul ignore next */
						if(debug) {
							console.log(`node[${allNodes[index].id}]#${id}: --> current node is not the master for this slot, bailing out`);
						}
						c.write(common.serialize(new Error(`ERR This node is not a master for key ${key}`)));
						setImmediate(processData);
						return;
					}

					/* istanbul ignore next */
					if(debug) {
						console.log(`node[${allNodes[index].id}]#${id}: - slot: ${slot.slot.index}`);
					}
					records.push({ key: key, value: value, slot: slot });
				}

				records.forEach((record) => {
					// All replicas share the same data, no need to implement replication
					record.slot.data[record.key] = { key: record.key, value: record.value, expiration: Number.MAX_VALUE };
				});

				/* istanbul ignore next */
				if(debug) {
					console.log(`node[${allNodes[index].id}]#${id}: --> data stored`);
				}
				c.write(common.serialize("OK"));
				setImmediate(processData);
				break;
			}
			case "SET": {
				if(request.result.length < 3) {
					c.write(common.serialize(new Error(`ERR Redis '${command}' command expects a key name and a value`)));
					setImmediate(processData);
					return;
				}

				if(request.result.some((r) => ! Buffer.isBuffer(r))) {
					c.write(common.serialize(new Error(`ERR Redis '${command}' command expects only bulk string parameters`)));
					setImmediate(processData);
					return;
				}

				const key     = request.result[1].toString('utf-8');
				/* istanbul ignore next */
				if(debug) {
					console.log(`node[${allNodes[index].id}]#${id}: - provided key: '${key}'`);
				}
				const value   = request.result[2];
				const hashKey = common.extractHashTag(key);
				/* istanbul ignore next */
				if(debug && (hashKey != key)) {
					console.log(`node[${allNodes[index].id}]#${id}: - detected hash tag in key: '${key}' becomes '${hashKey}'`);
				}
				const hash    = crc16.xmodem(hashKey) % 16384;
				/* istanbul ignore next */
				if(debug) {
					console.log(`node[${allNodes[index].id}]#${id}: - hash computed: ${hash}`);
				}

				let   expiration        = Number.MAX_VALUE;
				let   overrideIfExists  = true;
				let   createIfNotExists = true;

				for(let i = 3; i < request.result.length; i = i+1) {
					const option = request.result[i].toString('utf-8');
					switch(option) {
						case "PX":
						case "EX": {
							i = i + 1;
							if(i >= request.result.length) {
								c.write(common.serialize(new Error(`ERR Redis '${command}' option '${option}' expects a non-negative integer as a parameter`)));
								setImmediate(processData);
								return;
							}
							const expStr = request.result[i].toString('utf-8');
							const exp = expStr.match(/^([1-9][0-9]+|[0-9])$/) ? parseInt(expStr) : NaN;
							if(isNaN(exp) || (exp == Infinity)) {
								c.write(common.serialize(new Error(`ERR Redis '${command}' option '${option}' expects a non-negative integer as a parameter`)));
								setImmediate(processData);
								return;
							}
							expiration = Date.now() + (option == "PX" ? exp : exp * 1000);
							break;
						}
						case "NX": {
							overrideIfExists = false;
							break;
						}
						case "XX": {
							createIfNotExists = false;
							break;
						}
						default: {
							c.write(common.serialize(new Error(`ERR Redis '${command}' does not accept option '${option}'`)));
							setImmediate(processData);
							return;
						}
					}
				}

				// Search if this node is a master for this slot
				const slot = nodeData.slots.find((slotData) => (slotData.slot.start <= hash && slotData.slot.end >= hash));

				if(slot === undefined) {
					/* istanbul ignore next */
					if(debug) {
						console.log(`node[${allNodes[index].id}]#${id}: --> no suitable slot found on this node, bailing out`);
					}
					c.write(common.serialize(new Error(`ERR This node does not host a valid slot for key ${key}`)));
					setImmediate(processData);
					return;
				}

				if(! slot.master) {
					/* istanbul ignore next */
					if(debug) {
						console.log(`node[${allNodes[index].id}]#${id}: --> current node is not the master for this slot, bailing out`);
					}
					c.write(common.serialize(new Error(`ERR This node is not a master for key ${key}`)));
					setImmediate(processData);
					return;
				}

				const hasKey = slot.data.hasOwnProperty(key)
				if((hasKey && (!overrideIfExists)) || ((!hasKey) && (!createIfNotExists))) {
					/* istanbul ignore next */
					if(debug) {
						console.log(`node[${allNodes[index].id}]#${id}: --> not storing data: key exists: ${hasKey}, override if exists: ${overrideIfExists}, create if not exists: ${createIfNotExists}`);
					}
					c.write(common.serialize(null));
					setImmediate(processData);
					return;
				}

				// All replicas share the same data, no need to implement replication
				slot.data[key] = { key: key, value: value, expiration: expiration };

				/* istanbul ignore next */
				if(debug) {
					console.log(`node[${allNodes[index].id}]#${id}: --> data stored`);
				}
				c.write(common.serialize("OK"));
				setImmediate(processData);
				break;
			}
			case "GET": {
				if((request.result.length != 2) || (request.result.some((r) => ! Buffer.isBuffer(r)))) {
					c.write(common.serialize(new Error(`ERR Redis '${command}' command expects a key name as a bulk string parameter`)));
					setImmediate(processData);
					return;
				}

				const key     = request.result[1].toString('utf-8');
				const value   = request.result[2];
				const hashKey = common.extractHashTag(key);
				const hash    = crc16.xmodem(hashKey) % 16384;

				// Search if this node is a master *or a slave* for this slot
				let slot = nodeData.slots.find((slotData) => (slotData.slot.start <= hash && slotData.slot.end >= hash));

				if(slot === undefined) {
					c.write(common.serialize(new Error(`ERR This node does not host a valid slot for key ${key}`)));
					setImmediate(processData);
					return;
				}

				const hasKey = slot.data.hasOwnProperty(key)
				if(! hasKey) {
					c.write(common.serialize(null));
					setImmediate(processData);
					return;
				}

				const entry = slot.data[key];
				// Expired value, remove from cache
				if(entry.expiration < Date.now()) {
					// All replicas share the same data, no need to implement replication
					delete slot.data[key];
					c.write(common.serialize(null));
					setImmediate(processData);
					return;
				}
				c.write(common.serialize(entry.value));
				setImmediate(processData);
				break;
			}
			default: {
				/* istanbul ignore next */
				if(debug) {
					console.error(`node[${allNodes[index].id}]#${id}: received unsupported command: ${command}`);
				}
				c.write(common.serialize(new Error(`ERR received unsupported command '${command}'`)));
				setImmediate(processData);
				break;
			}
		}
	};

	c.on('data', processData);

	/* istanbul ignore next */
	c.on('close', () => {
		if(debug) {
			console.log(`node[${allNodes[index].id}]#${id}: client disconnected`);
		}
	});

	/* istanbul ignore next */
	c.on('timeout', () => {
		c.end();
		if(debug) {
			console.log(`node[${allNodes[index].id}]#${id}: client timeout`);
		}
	});

	/* istanbul ignore next */
	c.on('error', (err) => {
		c.end();
		c.destroy();
		if(debug) {
			console.log(`node[${allNodes[index].id}]#${id}: client error ${err}`);
		}
	});
}

function createProcessServer(index, config, allNodes, allSlots, nodesData) {
	const currentNode = allNodes[index];
	const id          = uuid();
	let   valid       = false;

	currentNode.id = id;

	return new Promise((resolve, reject) => {
		const server = net.createServer();
		server.on('connection', (c) => handleProcessConnection(c, index, config, allNodes, allSlots, nodesData));

		/* istanbul ignore next */
		server.on('error', (err) => {
			if(debug) {
				console.log(`node[${index}]: server error, closing`, err);
			}
			if(! valid) {
				reject(new Error(`Unable to setup data server #${index}: ${err}`));
			}
		}); 

		server.on('listening', () => {
			const port = server.address().port;
			/* istanbul ignore next */
			if(debug) {
				console.log(`node[${index}]: setup successful, listening on port #${port}`);
			}
			currentNode.port = port;
			resolve(server);
			valid = true;
		});

		server.on('close', () => {
			/* istanbul ignore next */
			if(debug) {
				console.log(`node[${index}]: server stopping`);
			}
		});

		server.listen();
	});
}

function RedisConfig() {
	EventEmitter.call(this);
	this.clientId = 0;
	this.commandId = 0;
	this.version = 1;
	this.nodes = 1;
	this.slotsPerNode = 1;
	this.replicas = 0;
	this.totalSlots = 1;
	this.processPorts = [];
	this.servers = [];
	this.nextResponses = [];
}

RedisConfig.prototype = Object.create(EventEmitter.prototype); 

RedisConfig.prototype.read = function(options) {
	if((options !== null) && (options !== undefined)) {
		if(options.hasOwnProperty('nodes') && (options.nodes !== null) && (options.nodes !== undefined)) {
			if(((typeof options.nodes) != "number") || (options.nodes <= 0) || (options.nodes > 255)) {
				throw new Error("The number of nodes must be a positive integer up to 255");
			}
			this.nodes = options.nodes;
		}
		if(options.hasOwnProperty('replicas') && (options.replicas !== null) && (options.replicas !== undefined)) {
			if(((typeof options.replicas) != "number") || (options.replicas < 0) || (options.replicas > 20)) {
				throw new Error("The number of replicas must be a non-negative integer up to 20");
			}
			this.replicas = options.replicas;
		}
		if(options.hasOwnProperty('slotsPerNode') && (options.slotsPerNode !== null) && (options.slotsPerNode !== undefined)) {
			if(((typeof options.slotsPerNode) != "number") || (options.slotsPerNode <= 0) || (options.slotsPerNode > 255)) {
				throw new Error("The number of slots per node must be a positive integer up to 255");
			}
			this.slotsPerNode = options.slotsPerNode;
		}
	}

	if(this.replicas >= this.nodes) {
		throw new Error("The number of replicas must be less than the number of nodes");
	}

	this.totalSlots = this.slotsPerNode * this.nodes;
	if(this.totalSlots > 16384) {
		throw new Error("The total number of slots can not be above 16384");
	}
}

RedisConfig.prototype.stop = function() {
	this.servers.forEach((server) => server.close());
	this.emit('stopped');
}

module.exports = (options) => {
	const config = new RedisConfig();
	config.read(options);

	const allNodes = [];
	const nodesData = [];
	for(let i = 0; i < config.nodes; ++i) {
		const node = { index: i, id: null, port: 0 };
		allNodes.push(node);
		nodesData.push({ node: node, slots: [] });
	}

	const allSlots = [];
	const increment = Math.floor(16384 / config.totalSlots) - 1;
	for(let slotIndex = 0; slotIndex < config.totalSlots; ++slotIndex) {
		const nodeIndex = slotIndex % config.nodes;

		const start = Math.floor((slotIndex * 16384)/config.totalSlots);
		const end   = Math.floor(((slotIndex+1) * 16384)/config.totalSlots) - 1;

		const slot = { index: slotIndex, start: start, end: end };
		const slotNodes = [];
		slotNodes.slot = slot;

		const slotCache = {};

		const masterNode = { slot: slot, node: allNodes[nodeIndex], master: true, data: slotCache, replica: [] };
		slotNodes.push(masterNode);
		nodesData[nodeIndex].slots.push(masterNode);

		for(let i = 1; i <= config.replicas; ++i) {
			const replicaIndex = (slotIndex + i) % config.nodes;
			const replicaNode = { slot: slot, node: allNodes[replicaIndex], master: false, data: slotCache, replica: [] };
			slotNodes.push(replicaNode);
			nodesData[replicaIndex].slots.push(replicaNode);
		}

		for(let i = 0; i < slotNodes.length; ++i) {
			for(let j = 0; j < slotNodes.length; ++j) {
				if(slotNodes[i].node.index != slotNodes[j].node.index) {
					slotNodes[i].replica.push(slotNodes[j]);
				}
			}
		}
		allSlots[slotIndex] = slotNodes;
	}

	const processes = [];
	for(let i = 0; i < config.nodes; ++i) {
		processes.push(createProcessServer(i, config, allNodes, allSlots, nodesData));
	}

	return Promise.all(processes).then((servers) => {
		/* istanbul ignore next */
		if(debug) {
			console.log("All processes started, ready to serve");
		}
		config.servers = servers;
		config.processPorts = servers.map((server) => server.address().port);
		config.version++;
		return config;
	});
};
