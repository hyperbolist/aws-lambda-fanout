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

function handleProcessConnection(c, index, config, cacheData) {
    const id = (++config.clientId);
    let data = new Buffer(0);
    const newLine = new Buffer("\r\n", 'utf-8');
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

        const dataIndex = data.indexOf(newLine);
        if(dataIndex != -1) {
            const line = data.slice(0, dataIndex).toString('utf-8');
            const parts = line.split(" ");
            const command = parts[0];
            switch(parts[0]) {
                case "set":
                case "add":
                case "replace": {
                    if(parts.length == 5 || parts.length == 6) {
                        const cas = (++config.commandId);
                        const key = parts[1];                                           // Cache entry key: no control characters or whitespace
                        const flags = Number(parts[2]);                                 // Arbitrary flags
                        let   expiration = Number(parts[3]);                            // Expiration time
                        const bytes = Number(parts[4]);                                 // Data size
                        const noReply = (parts.length == 6) && (parts[5] == "noreply"); // Don't send a response

                        if(key.length == 0 || key.length > 250 || isNaN(flags) || isNaN(expiration) || isNaN(bytes) || bytes < 0 || ((parts.length == 6) && (parts[5] != "noreply"))) {
                            /* istanbul ignore next */
                            if(debug) {
                                console.error(`shard[${index}]#${id}: received badly formatted ${command} command`);
                            }
                            data = data.slice(dataIndex + 2);
                            c.write("CLIENT_ERROR received badly formatted command\r\n");
                            setImmediate(processData);
                        } else if(data.length >= ((dataIndex + 2) + (bytes + 2))) {
                            if(data.slice((dataIndex + 2) + bytes, (dataIndex + 2) + (bytes + 2)).toString() == "\r\n") {
                                const value = data.slice(dataIndex + 2, (dataIndex + 2) + bytes);
                                if(expiration === 0) {
                                    expiration = Number.MAX_VALUE;
                                } else if(expiration <= MAX_DELTA_EXPIRATION_TIME) {
                                    expiration = Date.now() + (expiration * 1000);
                                } else {
                                    expiration = expiration * 1000;
                                }

                                let result = "";
                                switch(command) {
                                    case "set": {
                                        cacheData[key] = { key: key, value: value, expiration: expiration, flags: flags, cas: cas };
                                        result = "STORED";
                                        break;
                                    }
                                    case "add": {
                                        if(! cacheData.hasOwnProperty(key)) {
                                            cacheData[key] = { key: key, value: value, expiration: expiration, flags: flags, cas: cas };
                                            result = "STORED";
                                        } else {
                                            result = "NOT_STORED";
                                        }
                                        break;
                                    }
                                    case "replace": {
                                        if(cacheData.hasOwnProperty(key)) {
                                            cacheData[key] = { key: key, value: value, expiration: expiration, flags: flags, cas: cas };
                                            result = "STORED";
                                        } else {
                                            result = "NOT_STORED";
                                        }
                                        break;
                                    }
                                }

                                /* istanbul ignore next */
                                if(debug) {
                                    console.log(`shard[${index}]#${id}: processed ${command} command for key ${key} with result ${result}`);
                                }
                                if(! noReply) {
                                    c.write(`${result}\r\n`);
                                }
                                data = data.slice((dataIndex + 2) + (bytes + 2));
                                setImmediate(processData);
                            } else {
                                /* istanbul ignore next */
                                if(debug) {
                                    const jsonData = JSON.stringify(data.slice((dataIndex + 2), bytes + 2).toString());
                                    console.error(`shard[${index}]#${id}: received invalid ${command} command data ${jsonData}`);
                                }
                                data = data.slice(dataIndex + 2);
                                c.write("CLIENT_ERROR received invalid command data\r\n");
                                setImmediate(processData);
                            }
                        } else {
                            /* istanbul ignore next */
                            if(debug) {
                                console.log(`shard[${index}]#${id}: received ${command} command, waiting for more data`);
                            }
                        }
                    } else {
                        /* istanbul ignore next */
                        if(debug) {
                            console.error(`shard[${index}]#${id}: received badly formatted ${command} command`);
                        }
                        data = data.slice(dataIndex + 2);
                        c.write("CLIENT_ERROR received badly formatted command\r\n");
                        setImmediate(processData);
                    }
                    break;
                }
                case "get":
                case "gets": {
                    if(parts.length >= 2) {
                        const keys = parts.slice(1);
                        let hasError = false;
                        for(let i = 0; (i < keys.length) && (!hasError); ++i) {
                            const key = keys[i];
                            if(key.length == 0 || key.length > 250) {
                                /* istanbul ignore next */
                                if(debug) {
                                    console.error(`shard[${index}]#${id}: received badly formatted ${command} command`);
                                }
                                c.write("CLIENT_ERROR received badly formatted command\r\n");
                                hasError = true;
                            } else if(cacheData.hasOwnProperty(key)) {
                                const cacheEntry = cacheData[key];
                                if(cacheEntry.expiration < Date.now()) {
                                    /* istanbul ignore next */
                                    if(debug) {
                                        console.error(`shard[${index}]#${id}: expired value for command ${command}`);
                                    }
                                    delete cacheData[key];
                                } else {
                                    /* istanbul ignore next */
                                    if(debug) {
                                        console.error(`shard[${index}]#${id}: successfuly retrieved value for command ${command}`);
                                    }
                                    c.write(`VALUE ${cacheEntry.key} ${cacheEntry.flags} ${cacheEntry.value.length} ${cacheEntry.cas}\r\n`);
                                    c.write(cacheEntry.value);
                                    c.write("\r\n");
                                }
                            } else {
                                /* istanbul ignore next */
                                if(debug) {
                                    console.error(`shard[${index}]#${id}: non existent value for command ${command}`);
                                }
                            }
                        }
                        if(! hasError) {
                            c.write(`END\r\n`);
                        }
                        data = data.slice(dataIndex + 2);
                        setImmediate(processData);
                    } else {
                        /* istanbul ignore next */
                        if(debug) {
                            console.error(`shard[${index}]#${id}: received badly formatted ${command} command`);
                        }
                        data = data.slice(dataIndex + 2);
                        c.write("CLIENT_ERROR received badly formatted command\r\n");
                        setImmediate(processData);
                    }
                    break;
                }
                default: {
                    /* istanbul ignore next */
                    if(debug) {
                        console.error(`shard[${index}]#${id}: received unsupported command: ${command}`);
                    }
                    data = data.slice(dataIndex + 2);
                    c.write("CLIENT_ERROR received unsupported command\r\n");
                    setImmediate(processData);
                    break;
                }
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
        const index = data.indexOf(newLine);
        if(index != -1) {
            const line = data.slice(0, index).toString('utf-8');
            data = data.slice(index + 2);
            if(line == "config get cluster") {
                /* istanbul ignore next */
                if(debug) {
                    console.log(`config#${id}: received cluster configuration description command`);
                }
                const portsString = config.processPorts.map((p) => `localhost|127.0.0.1|${p}`).join(" ");
                const configString = `${config.version}\n${portsString}\n`;
                c.write(`CONFIG cluster 0 ${configString.length}\r\n`);
                c.write(`${configString}\r\n`);
                c.write("END\r\n");
                setImmediate(processData);
            } else if((line == "quit") || (line == "exit")) {
                /* istanbul ignore next */
                if(debug) {
                    console.log(`config#${id}: received cluster shutdown`);
                }
                c.end();
                config.stop();
            } else {
                /* istanbul ignore next */
                if(debug) {
                    console.error(`config#${id}: received unknown command: ${line}`);
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
