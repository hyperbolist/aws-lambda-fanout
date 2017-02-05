/* 
 * AWS Lambda Fan-Out Utility
 * 
 * Copyright 2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
 * This Node.js script tests the features from the queue.js script
 */

'use strict';

const mc = require('../test-hosts/memcached-host.js');
const net = require('net');
const uuid = require('uuid');

const assert = require('assert');
let config = null;
const server = mc({ shards: 2 }).then((c) => config = c);

function connect(host, port) {
  return new Promise((resolve, reject) => {
    const client = net.connect({ host: host, port: port });
    //client.setEncoding('utf8');
    client.on('connect', () => {
      resolve(client);
    });
    client.on('error', (err) => {
      reject(err);
    });
    client.on('timeout', () => {
      reject(Error("Timeout occured"));
    });
  });
}

function sendRequestAndReadResponse(client, request, previousResponse) {
  const newLine = new Buffer("\r\n", 'utf-8');

  return new Promise((resolve, reject) => {
    const responseElements = [];
    let data = new Buffer(0);
    if(previousResponse) {
      client.removeListener('data', previousResponse.dataProcessor);
      if(Buffer.isBuffer(previousResponse.pendingData)) {
        data = previousResponse.pendingData;
      }
    }

    let done = () => null;

    const processData = (chunk) => {
      if((typeof chunk) == "string") {
        console.log("Was string");
        chunk = new Buffer(chunk, 'utf-8');
      }
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
            const key = commandParts[1];
            const flags = Number(commandParts[2]);
            const bytes = Number(commandParts[3]);
            const cas = commandParts.length == 5 ? Number(commandParts[4]) : null;
            if(data.length >= ((commandEnd + 2) + (bytes + 2))) {
              if(data.slice((commandEnd + 2) + bytes, (commandEnd + 2) + (bytes + 2)).toString() == "\r\n") {
                const value = data.slice(commandEnd + 2, (commandEnd + 2) + bytes);
                data = data.slice((commandEnd + 2) + (bytes + 2));
                responseElements.push({ code: command, key: key, flags: flags, cas: cas, bytes: bytes, value: value });
                // There must be other elements in the response
                setImmediate(processData);
              } else {
                console.error(" - Invalid response received from server");
                done(new Error("Invalid response received from server"));
              }
            } else {
              // Do nothing, wait for more data
            }
            break;
          }
          case "ERROR":
          case "STORED":
          case "NOT_STORED":
          case "EXISTS":
          case "NOT_FOUND":
          case "END": {
            data = data.slice(commandEnd + 2);
            responseElements.push({ code: command });
            done();
            break;
          }
          case "CLIENT_ERROR":
          case "SERVER_ERROR": {
            data = data.slice(commandEnd + 2);
            if(commandParts.length > 1) {
              responseElements.push({ code: command, message: commandParts.slice(1).join(' ') });
            } else {
              responseElements.push({ code: command, message: "" });
            }
            done();
            break;
          }
          default: {
            done(new Error(`Unknown command '${command}'`));
            break;
          }
        }
      }
    };

    const close = () => { done(); };

    done = (err) => {
      const result = { elements: responseElements, pendingData: data };
      result.dataProcessor = (chunk) => {
        if((typeof chunk) == "string") {
          chunk = new Buffer(chunk, 'utf-8');
        }
        if(Buffer.isBuffer(chunk)) {
          if(result.pendingData.length == 0) {
            result.pendingData = chunk;
          } else {
            result.pendingData = Buffer.concat([result.pendingData, chunk]);
          }
        }
      };
      client.removeListener('close', close);
      client.removeListener('data', processData);
      client.on('data', result.dataProcessor);
      if(err) {
        reject(err);
      } else {
        resolve(result);
      }
    };

    client.on('data', processData);
    client.on('close', close);

    if(request) {
      client.write(request);
    }

    processData();
  });
}

describe('memcached', () => {
  it('should provide the server list', (done) => {
    let client = null;
    server.then(() => {
      return connect('localhost', config.configPort);
    }).then((c) => {
      client = c;
    }).then(() => {
      return sendRequestAndReadResponse(client, "config get cluster\r\n");
    }).then((response) => {
      assert.strictEqual(response.elements.length, 2);
      assert.strictEqual(response.elements[0].code, "CONFIG");
      const configData = response.elements[0].value.toString('utf-8').split("\n");
      assert.strictEqual(configData.length, 3);
      const servers = configData[1].split(' ');
      assert.strictEqual(servers.length, 2);
      servers.forEach((server) => {
        const serverParts = server.split('|');
        assert.strictEqual(serverParts.length, 3);
      });
      return response;
    }).then(() => {
      client.end();
      done();
    }).catch((err) => {
      client.end();
      done(err);
    });
  });

  it('should provide the server list', (done) => {
    let client = null;
    server.then(() => {
      return connect('localhost', config.configPort);
    }).then((c) => {
      client = c;
    }).then(() => {
      return sendRequestAndReadResponse(client, "config get cluster\r\nconfig get cluster");
    }).then((response) => {
      return sendRequestAndReadResponse(client, "\r\n", response);
    }).then((response) => {
      assert.strictEqual(response.elements.length, 2);
      assert.strictEqual(response.elements[0].code, "CONFIG");
      const configData = response.elements[0].value.toString('utf-8').split("\n");
      assert.strictEqual(configData.length, 3);
      const servers = configData[1].split(' ');
      assert.strictEqual(servers.length, 2);
      servers.forEach((server) => {
        const serverParts = server.split('|');
        assert.strictEqual(serverParts.length, 3);
      });
      return response;
    }).then(() => {
      client.end();
      done();
    }).catch((err) => {
      client.end();
      done(err);
    });
  });

  it('set on non existing key', (done) => {
    let client = null;
    const key = uuid();
    server.then(() => {
      return connect('localhost', config.processPorts[0]);
    }).then((c) => {
      client = c;
    }).then(() => {
      return sendRequestAndReadResponse(client, `set ${key} 10 10 6\r\nAZERTY\r\n`);
    }).then((response) => {
      assert.strictEqual(response.elements.length, 1);
      assert.strictEqual(response.elements[0].code, "STORED");
      return response;
    }).then(() => {
      client.end();
      done();
    }).catch((err) => {
      client.end();
      done(err);
    });
  });

  it('set on existing key', (done) => {
    let client = null;
    const key = uuid();
    server.then(() => {
      return connect('localhost', config.processPorts[0]);
    }).then((c) => {
      client = c;
    }).then(() => {
      return sendRequestAndReadResponse(client, `set ${key} 10 10 6\r\nAZERTY\r\n`);
    }).then((response) => {
      return sendRequestAndReadResponse(client, `set ${key} 10 10 6\r\nQWERTY\r\n`, response);
    }).then((response) => {
      assert.strictEqual(response.elements.length, 1);
      assert.strictEqual(response.elements[0].code, "STORED");
      return response;
    }).then(() => {
      client.end();
      done();
    }).catch((err) => {
      client.end();
      done(err);
    });
  });

  it('set on large value', (done) => {
    let client = null;
    const key = uuid();
    server.then(() => {
      return connect('localhost', config.processPorts[0]);
    }).then((c) => {
      client = c;
    }).then(() => {
      return sendRequestAndReadResponse(client, `get ${key}\r\nset ${key} 10 10 512\r\n0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF`);
    }).then((response) => {
      return sendRequestAndReadResponse(client, `0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF\r\n`, response);
    }).then((response) => {
      assert.strictEqual(response.elements.length, 1);
      assert.strictEqual(response.elements[0].code, "STORED");
      return response;
    }).then(() => {
      client.end();
      done();
    }).catch((err) => {
      client.end();
      done(err);
    });
  });

  it('add on non existing key', (done) => {
    let client = null;
    const key = uuid();
    server.then(() => {
      return connect('localhost', config.processPorts[0]);
    }).then((c) => {
      client = c;
    }).then(() => {
      return sendRequestAndReadResponse(client, `add ${key} 10 10 6\r\nAZERTY\r\n`);
    }).then((response) => {
      assert.strictEqual(response.elements.length, 1);
      assert.strictEqual(response.elements[0].code, "STORED");
      return response;
    }).then(() => {
      client.end();
      done();
    }).catch((err) => {
      client.end();
      done(err);
    });
  });

  it('add on existing key', (done) => {
    let client = null;
    const key = uuid();
    server.then(() => {
      return connect('localhost', config.processPorts[0]);
    }).then((c) => {
      client = c;
    }).then(() => {
      return sendRequestAndReadResponse(client, `set ${key} 10 10 6\r\nAZERTY\r\n`);
    }).then((response) => {
      return sendRequestAndReadResponse(client, `add ${key} 10 10 6\r\nQWERTY\r\n`, response);
    }).then((response) => {
      assert.strictEqual(response.elements.length, 1);
      assert.strictEqual(response.elements[0].code, "NOT_STORED");
      return response;
    }).then(() => {
      client.end();
      done();
    }).catch((err) => {
      client.end();
      done(err);
    });
  });

  it('replace on non existing key', (done) => {
    let client = null;
    const key = uuid();
    server.then(() => {
      return connect('localhost', config.processPorts[0]);
    }).then((c) => {
      client = c;
    }).then(() => {
      return sendRequestAndReadResponse(client, `replace ${key} 10 10 6\r\nAZERTY\r\n`);
    }).then((response) => {
      assert.strictEqual(response.elements.length, 1);
      assert.strictEqual(response.elements[0].code, "NOT_STORED");
      return response;
    }).then(() => {
      client.end();
      done();
    }).catch((err) => {
      client.end();
      done(err);
    });
  });

  it('replace on existing key', (done) => {
    let client = null;
    const key = uuid();
    server.then(() => {
      return connect('localhost', config.processPorts[0]);
    }).then((c) => {
      client = c;
    }).then(() => {
      return sendRequestAndReadResponse(client, `set ${key} 10 10 6\r\nAZERTY\r\n`);
    }).then((response) => {
      return sendRequestAndReadResponse(client, `replace ${key} 10 10 6\r\nQWERTY\r\n`, response);
    }).then((response) => {
      assert.strictEqual(response.elements.length, 1);
      assert.strictEqual(response.elements[0].code, "STORED");
      return response;
    }).then(() => {
      client.end();
      done();
    }).catch((err) => {
      client.end();
      done(err);
    });
  });

  it('get on non existing key', (done) => {
    let client = null;
    const key = uuid();
    server.then(() => {
      return connect('localhost', config.processPorts[0]);
    }).then((c) => {
      client = c;
    }).then(() => {
      return sendRequestAndReadResponse(client, `get ${key}\r\n`);
    }).then((response) => {
      assert.strictEqual(response.elements.length, 1);
      assert.strictEqual(response.elements[0].code, "END");
      return response;
    }).then(() => {
      client.end();
      done();
    }).catch((err) => {
      client.end();
      done(err);
    });
  });

  it('get on non-expired key', (done) => {
    let client = null;
    const key = uuid();
    server.then(() => {
      return connect('localhost', config.processPorts[0]);
    }).then((c) => {
      client = c;
    }).then(() => {
      return sendRequestAndReadResponse(client, `set ${key} 10 100 6\r\nAZERTY\r\n`);
    }).then((response) => {
      return sendRequestAndReadResponse(client, `get ${key}\r\n`, response);
    }).then((response) => {
      assert.strictEqual(response.elements.length, 2);
      assert.strictEqual(response.elements[0].code, "VALUE");
      assert.strictEqual(response.elements[0].value.toString(), "AZERTY");
      assert.strictEqual(response.elements[0].flags, 10);
      assert.ok(response.elements[0].cas > 0);
      assert.strictEqual(response.elements[1].code, "END");
      return response;
    }).then(() => {
      client.end();
      done();
    }).catch((err) => {
      client.end();
      done(err);
    });
  });

  it('get on non-expired key (max delta)', (done) => {
    let client = null;
    const key = uuid();
    server.then(() => {
      return connect('localhost', config.processPorts[0]);
    }).then((c) => {
      client = c;
    }).then(() => {
      return sendRequestAndReadResponse(client, `set ${key} 10 2592000 6\r\nAZERTY\r\n`);
    }).then((response) => {
      return sendRequestAndReadResponse(client, `get ${key}\r\n`, response);
    }).then((response) => {
      assert.strictEqual(response.elements.length, 2);
      assert.strictEqual(response.elements[0].code, "VALUE");
      assert.strictEqual(response.elements[0].value.toString(), "AZERTY");
      assert.strictEqual(response.elements[0].flags, 10);
      assert.ok(response.elements[0].cas > 0);
      assert.strictEqual(response.elements[1].code, "END");
      return response;
    }).then(() => {
      client.end();
      done();
    }).catch((err) => {
      client.end();
      done(err);
    });
  });

  it('get on non-expired key (no expiration)', (done) => {
    let client = null;
    const key = uuid();
    server.then(() => {
      return connect('localhost', config.processPorts[0]);
    }).then((c) => {
      client = c;
    }).then(() => {
      return sendRequestAndReadResponse(client, `set ${key} 10 0 6\r\nAZERTY\r\n`);
    }).then((response) => {
      return sendRequestAndReadResponse(client, `get ${key}\r\n`, response);
    }).then((response) => {
      assert.strictEqual(response.elements.length, 2);
      assert.strictEqual(response.elements[0].code, "VALUE");
      assert.strictEqual(response.elements[0].value.toString(), "AZERTY");
      assert.strictEqual(response.elements[0].flags, 10);
      assert.ok(response.elements[0].cas > 0);
      assert.strictEqual(response.elements[1].code, "END");
      return response;
    }).then(() => {
      client.end();
      done();
    }).catch((err) => {
      client.end();
      done(err);
    });
  });

  it('get on expired key (negative)', (done) => {
    let client = null;
    const key = uuid();
    server.then(() => {
      return connect('localhost', config.processPorts[0]);
    }).then((c) => {
      client = c;
    }).then(() => {
      return sendRequestAndReadResponse(client, `set ${key} 10 -2 6\r\nAZERTY\r\n`);
    }).then((response) => {
      return sendRequestAndReadResponse(client, `get ${key}\r\n`, response);
    }).then((response) => {
      assert.strictEqual(response.elements.length, 1);
      assert.strictEqual(response.elements[0].code, "END");
      return response;
    }).then(() => {
      client.end();
      done();
    }).catch((err) => {
      client.end();
      done(err);
    });
  });

  it('get on expired key (30 days + 1 second)', (done) => {
    let client = null;
    const key = uuid();
    server.then(() => {
      return connect('localhost', config.processPorts[0]);
    }).then((c) => {
      client = c;
    }).then(() => {
      return sendRequestAndReadResponse(client, `set ${key} 10 2592001 6\r\nAZERTY\r\n`);
    }).then((response) => {
      return sendRequestAndReadResponse(client, `get ${key}\r\n`, response);
    }).then((response) => {
      assert.strictEqual(response.elements.length, 1);
      assert.strictEqual(response.elements[0].code, "END");
      return response;
    }).then(() => {
      client.end();
      done();
    }).catch((err) => {
      client.end();
      done(err);
    });
  });

  it('get on expired key (now - 1 day)', (done) => {
    let when = Math.floor(Date.now() / 1000) - (24*60*60);
    let client = null;
    const key = uuid();
    server.then(() => {
      return connect('localhost', config.processPorts[0]);
    }).then((c) => {
      client = c;
    }).then(() => {
      return sendRequestAndReadResponse(client, `set ${key} 10 ${when} 6\r\nAZERTY\r\n`);
    }).then((response) => {
      return sendRequestAndReadResponse(client, `get ${key}\r\n`, response);
    }).then((response) => {
      assert.strictEqual(response.elements.length, 1);
      assert.strictEqual(response.elements[0].code, "END");
      return response;
    }).then(() => {
      client.end();
      done();
    }).catch((err) => {
      client.end();
      done(err);
    });
  });

  it('get on expired key (1 sec)', (done) => {
    let client = null;
    const key = uuid();
    server.then(() => {
      return connect('localhost', config.processPorts[0]);
    }).then((c) => {
      client = c;
    }).then(() => {
      return sendRequestAndReadResponse(client, `set ${key} 10 1 6\r\nAZERTY\r\n`);
    }).then((response) => {
      return new Promise((resolve, reject) => { setTimeout(() => { resolve(response); }, 1010); })
    }).then((response) => {
      return sendRequestAndReadResponse(client, `get ${key}\r\n`, response);
    }).then((response) => {
      assert.strictEqual(response.elements.length, 1);
      assert.strictEqual(response.elements[0].code, "END");
      return response;
    }).then(() => {
      client.end();
      done();
    }).catch((err) => {
      client.end();
      done(err);
    });
  });

  it('get on expired key (now + 1 sec)', (done) => {
    let when = Math.floor(Date.now() / 1000) + 1;
    let client = null;
    const key = uuid();
    server.then(() => {
      return connect('localhost', config.processPorts[0]);
    }).then((c) => {
      client = c;
    }).then(() => {
      return sendRequestAndReadResponse(client, `set ${key} 10 ${when} 6\r\nAZERTY\r\n`);
    }).then((response) => {
      return new Promise((resolve, reject) => { setTimeout(() => { resolve(response); }, 1010); })
    }).then((response) => {
      return sendRequestAndReadResponse(client, `get ${key}\r\n`, response);
    }).then((response) => {
      assert.strictEqual(response.elements.length, 1);
      assert.strictEqual(response.elements[0].code, "END");
      return response;
    }).then(() => {
      client.end();
      done();
    }).catch((err) => {
      client.end();
      done(err);
    });
  });

  it('cas immutable on multiple get', (done) => {
    let client = null;
    let cas = null;
    const key = uuid();
    server.then(() => {
      return connect('localhost', config.processPorts[0]);
    }).then((c) => {
      client = c;
    }).then(() => {
      return sendRequestAndReadResponse(client, `set ${key} 10 0 6\r\nAZERTY\r\n`);
    }).then((response) => {
      return sendRequestAndReadResponse(client, `get ${key}\r\n`, response);
    }).then((response) => {
      cas = response.elements[0].cas;
      return response;
    }).then((response) => {
      return sendRequestAndReadResponse(client, `get ${key}\r\n`, response);
    }).then((response) => {
      assert.strictEqual(cas, response.elements[0].cas);
      return response;
    }).then(() => {
      client.end();
      done();
    }).catch((err) => {
      client.end();
      done(err);
    });
  });

  it('cas increases on multiple set on the same key', (done) => {
    let client = null;
    let cas = null;
    const key = uuid();
    server.then(() => {
      return connect('localhost', config.processPorts[0]);
    }).then((c) => {
      client = c;
    }).then(() => {
      return sendRequestAndReadResponse(client, `set ${key} 10 0 6\r\nAZERTY\r\n`);
    }).then((response) => {
      return sendRequestAndReadResponse(client, `get ${key}\r\n`, response);
    }).then((response) => {
      cas = response.elements[0].cas;
      return response;
    }).then((response) => {
      return sendRequestAndReadResponse(client, `set ${key} 10 0 6\r\nQWERTY\r\n`, response);
    }).then((response) => {
      return sendRequestAndReadResponse(client, `get ${key}\r\n`, response);
    }).then((response) => {
      assert.ok(cas < response.elements[0].cas);
    }).then(() => {
      client.end();
      done();
    }).catch((err) => {
      client.end();
      done(err);
    });
  });

  it('cas increases on multiple set on different keys', (done) => {
    let client = null;
    let cas = null;
    const key1 = uuid();
    const key2 = uuid();
    server.then(() => {
      return connect('localhost', config.processPorts[0]);
    }).then((c) => {
      client = c;
    }).then(() => {
      return sendRequestAndReadResponse(client, `set ${key1} 10 0 6\r\nAZERTY\r\n`);
    }).then((response) => {
      return sendRequestAndReadResponse(client, `get ${key1}\r\n`, response);
    }).then((response) => {
      cas = response.elements[0].cas;
      return response;
    }).then((response) => {
      return sendRequestAndReadResponse(client, `set ${key2} 10 0 6\r\nQWERTY\r\n`, response);
    }).then((response) => {
      return sendRequestAndReadResponse(client, `get ${key2}\r\n`, response);
    }).then((response) => {
      assert.ok(cas < response.elements[0].cas);
      return response;
    }).then(() => {
      client.end();
      done();
    }).catch((err) => {
      client.end();
      done(err);
    });
  });

  it('cas increases on multiple set on different keys on different shards', (done) => {
    let client = null;
    let cas = null;
    const key1 = uuid();
    const key2 = uuid();
    server.then(() => {
      return connect('localhost', config.processPorts[0]);
    }).then((c) => {
      client = c;
    }).then(() => {
      return sendRequestAndReadResponse(client, `set ${key1} 10 10 6\r\nAZERTY\r\n`);
    }).then((response) => {
      return sendRequestAndReadResponse(client, `get ${key1}\r\n`, response);
    }).then((response) => {
      cas = response.elements[0].cas;
      return response;
    }).then(() => {
      client.end();
    }).then(() => {
      return connect('localhost', config.processPorts[1]);
    }).then((c) => {
      client = c;
    }).then(() => {
      return sendRequestAndReadResponse(client, `set ${key2} 10 10 6\r\nQWERTY\r\n`);
    }).then((response) => {
      return sendRequestAndReadResponse(client, `get ${key2}\r\n`, response);
    }).then((response) => {
      assert.ok(cas < response.elements[0].cas);
      return response;
    }).then(() => {
      client.end();
      done();
    }).catch((err) => {
      client.end();
      done(err);
    });
  });

  it('cas increases on multiple set on same keys on different shards', (done) => {
    let client = null;
    let cas = null;
    const key = uuid();
    server.then(() => {
      return connect('localhost', config.processPorts[0]);
    }).then((c) => {
      client = c;
    }).then(() => {
      return sendRequestAndReadResponse(client, `set ${key} 10 10 6\r\nAZERTY\r\n`);
    }).then((response) => {
      return sendRequestAndReadResponse(client, `get ${key}\r\n`, response);
    }).then((response) => {
      cas = response.elements[0].cas;
      return response;
    }).then(() => {
      client.end();
    }).then(() => {
      return connect('localhost', config.processPorts[1]);
    }).then((c) => {
      client = c;
    }).then(() => {
      return sendRequestAndReadResponse(client, `set ${key} 10 10 6\r\nQWERTY\r\n`);
    }).then((response) => {
      return sendRequestAndReadResponse(client, `get ${key}\r\n`, response);
    }).then((response) => {
      assert.ok(cas < response.elements[0].cas);
      return response;
    }).then(() => {
      client.end();
      done();
    }).catch((err) => {
      client.end();
      done(err);
    });
  });

  it('data on one shard should not be available on the other shard', (done) => {
    let client = null;
    const key = uuid();
    server.then(() => {
      return connect('localhost', config.processPorts[0]);
    }).then((c) => {
      client = c;
    }).then(() => {
      return sendRequestAndReadResponse(client, `set ${key} 10 10 6\r\nAZERTY\r\n`);
    }).then(() => {
      client.end();
    }).then(() => {
      return connect('localhost', config.processPorts[1]);
    }).then((c) => {
      client = c;
    }).then(() => {
      return sendRequestAndReadResponse(client, `get ${key}\r\n`);
    }).then((response) => {
      assert.strictEqual(response.elements.length, 1);
      assert.strictEqual(response.elements[0].code, "END");
      return response;
    }).then(() => {
      client.end();
      done();
    }).catch((err) => {
      client.end();
      done(err);
    });
  });

  it('same key can be stored with different values in different shards', (done) => {
    let client = null;
    const key = uuid();
    server.then(() => {
      return connect('localhost', config.processPorts[0]);
    }).then((c) => {
      client = c;
    }).then(() => {
      return sendRequestAndReadResponse(client, `set ${key} 10 10 6\r\nAZERTY\r\n`);
    }).then(() => {
      client.end();
    }).then(() => {
      return connect('localhost', config.processPorts[1]);
    }).then((c) => {
      client = c;
    }).then(() => {
      return sendRequestAndReadResponse(client, `set ${key} 10 10 6\r\nQWERTY\r\n`);
    }).then(() => {
      return connect('localhost', config.processPorts[0]);
    }).then((c) => {
      client = c;
    }).then(() => {
      return sendRequestAndReadResponse(client, `get ${key}\r\n`);
    }).then((response) => {
      assert.strictEqual(response.elements.length, 2);
      assert.strictEqual(response.elements[0].code, "VALUE");
      assert.strictEqual(response.elements[0].value.toString('utf-8'), "AZERTY");
      return response;
    }).then(() => {
      client.end();
    }).then(() => {
      return connect('localhost', config.processPorts[1]);
    }).then((c) => {
      client = c;
    }).then(() => {
      return sendRequestAndReadResponse(client, `get ${key}\r\n`);
    }).then((response) => {
      assert.strictEqual(response.elements.length, 2);
      assert.strictEqual(response.elements[0].code, "VALUE");
      assert.strictEqual(response.elements[0].value.toString('utf-8'), "QWERTY");
      return response;
    }).then(() => {
      client.end();
    }).then(() => {
      client.end();
      done();
    }).catch((err) => {
      client.end();
      done(err);
    });
  });

  it('should work without options', (done) => {
    let client = null;
    mc().then((c) => {
      client = c;
      assert.strictEqual(client.processPorts.length, 1);
    }).then(() => {
      client.stop();
      done();
    }).catch((err) => {
      client.stop();
      done(err);
    });
  });

  it('should process configuration port parameter', (done) => {
    let ss = null;
    mc({ configPort: 8989 }).then((s) => {
      ss = s;
      assert.strictEqual(ss.configPort, 8989);
    }).then(() => {
      ss.stop();
      done();
    }).catch((err) => {
      ss.stop();
      done(err);
    });
  });

  it('should process shards parameter', (done) => {
    let ss = null;
    mc({ shards: 5 }).then((s) => {
      ss = s;
      assert.strictEqual(ss.processPorts.length, 5);
    }).then(() => {
      ss.stop();
      done();
    }).catch((err) => {
      ss.stop();
      done(err);
    });
  });

  it('should handle stop', (done) => {
    let client = null;
    let ss = null;
    mc({ shards: 5 }).then((s) => {
      ss = s;
      ss.on('stopped', () => {
        done();
      });
    }).then(() => {
      return connect('localhost', ss.configPort);
    }).then((c) => {
      client = c;
    }).then(() => {
      return sendRequestAndReadResponse(client, `exit\r\n`);
    }).catch((err) => {
      done(err);
      ss.stop();
    });
  });

  it('invalid get', (done) => {
    const key = uuid();
    let client = null;
    server.then(() => {
      return connect('localhost', config.processPorts[0]);
    }).then((c) => {
      client = c;
    }).then(() => {
      return sendRequestAndReadResponse(client, `get\r\n`);
    }).then((response) => {
      assert.strictEqual(response.elements.length, 1);
      assert.strictEqual(response.elements[0].code, "CLIENT_ERROR");
      assert.strictEqual(response.elements[0].message, "received badly formatted command");
      return response;
    }).then((response) => {
      return sendRequestAndReadResponse(client, `get \r\n`, response);
    }).then((response) => {
      assert.strictEqual(response.elements.length, 1);
      assert.strictEqual(response.elements[0].code, "CLIENT_ERROR");
      assert.strictEqual(response.elements[0].message, "received badly formatted command");
      return response;
    }).then((response) => {
      return sendRequestAndReadResponse(client, `get 0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789X\r\n`, response);
    }).then((response) => {
      assert.strictEqual(response.elements.length, 1);
      assert.strictEqual(response.elements[0].code, "CLIENT_ERROR");
      assert.strictEqual(response.elements[0].message, "received badly formatted command");
      return response;
    }).then((response) => {
      return sendRequestAndReadResponse(client, `get ${key} \r\n`, response);
    }).then((response) => {
      assert.strictEqual(response.elements.length, 1);
      assert.strictEqual(response.elements[0].code, "CLIENT_ERROR");
      assert.strictEqual(response.elements[0].message, "received badly formatted command");
      return response;
    }).then((response) => {
      return sendRequestAndReadResponse(client, `set ${key} 10 10 6\r\nAZERTY\r\n`, response);
    }).then((response) => {
      return sendRequestAndReadResponse(client, `get ${key} \r\n`, response);
    }).then((response) => {
      assert.strictEqual(response.elements.length, 2);
      assert.strictEqual(response.elements[0].code, "VALUE");
      assert.strictEqual(response.elements[0].value.toString('utf-8'), "AZERTY");
      assert.strictEqual(response.elements[1].code, "CLIENT_ERROR");
      assert.strictEqual(response.elements[1].message, "received badly formatted command");
      return response;
    }).then(() => {
      client.end();
      done();
    }).catch((err) => {
      client.end();
      done(err);
    });
  });

  it('invalid set', (done) => {
    let client = null;
    let step = 0;
    const key = uuid();
    server.then(() => {
      return connect('localhost', config.processPorts[0]);
    }).then((c) => {
      client = c;
    }).then(() => {
      return sendRequestAndReadResponse(client, `set\r\n`);
    }).then((response) => {
      assert.strictEqual(response.elements.length, 1);
      assert.strictEqual(response.elements[0].code, "CLIENT_ERROR");
      assert.strictEqual(response.elements[0].message, "received badly formatted command");
      return response;
    }).then((response) => {
      return sendRequestAndReadResponse(client, `set ${key} 10 10 0\r\nA\r\n`, response);
    }).then((response) => {
      assert.strictEqual(response.elements.length, 1);
      assert.strictEqual(response.elements[0].code, "CLIENT_ERROR");
      assert.strictEqual(response.elements[0].message, "received invalid command data");
      return response;
    }).then((response) => {
      return sendRequestAndReadResponse(client, null, response);
    }).then((response) => {
      assert.strictEqual(response.elements.length, 1);
      assert.strictEqual(response.elements[0].code, "CLIENT_ERROR");
      assert.strictEqual(response.elements[0].message, "received unsupported command");
      return response;
    }).then((response) => {
      return sendRequestAndReadResponse(client, `set  10 10 1\r\nA\r\n`, response);
    }).then((response) => {
      assert.strictEqual(response.elements.length, 1);
      assert.strictEqual(response.elements[0].code, "CLIENT_ERROR");
      assert.strictEqual(response.elements[0].message, "received badly formatted command");
      return response;
    }).then((response) => {
      return sendRequestAndReadResponse(client, null, response);
    }).then((response) => {
      assert.strictEqual(response.elements.length, 1);
      assert.strictEqual(response.elements[0].code, "CLIENT_ERROR");
      assert.strictEqual(response.elements[0].message, "received unsupported command");
      return response;
    }).then((response) => {
      return sendRequestAndReadResponse(client, `set 0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789X 10 10 1\r\nA\r\n`, response);
    }).then((response) => {
      assert.strictEqual(response.elements.length, 1);
      assert.strictEqual(response.elements[0].code, "CLIENT_ERROR");
      assert.strictEqual(response.elements[0].message, "received badly formatted command");
      return response;
    }).then(() => {
      client.end();
      done();
    }).catch((err) => {
      client.end();
      done(err);
    });
  });

  it('invalid command', (done) => {
    let client = null;
    server.then(() => {
      return connect('localhost', config.processPorts[0]);
    }).then((c) => {
      client = c;
    }).then(() => {
      return sendRequestAndReadResponse(client, `unknown\r\n`);
    }).then((response) => {
      assert.strictEqual(response.elements.length, 1);
      assert.strictEqual(response.elements[0].code, "CLIENT_ERROR");
      assert.strictEqual(response.elements[0].message, "received unsupported command");
      return response;
    }).then(() => {
      client.end();
      done();
    }).catch((err) => {
      client.end();
      done(err);
    });
  });

  it('invalid config command', (done) => {
    let client = null;
    server.then(() => {
      return connect('localhost', config.configPort);
    }).then((c) => {
      client = c;
    }).then(() => {
      return sendRequestAndReadResponse(client, `unknown\r\n`);
    }).then((response) => {
      assert.strictEqual(response.elements.length, 1);
      assert.strictEqual(response.elements[0].code, "CLIENT_ERROR");
      assert.strictEqual(response.elements[0].message, "received unsupported command");
      return response;
    }).then(() => {
      client.end();
      done();
    }).catch((err) => {
      client.end();
      done(err);
    });
  });

  it('two commands with pending data', (done) => {
    let client = null;
    const key = uuid();
    server.then(() => {
      return connect('localhost', config.processPorts[0]);
    }).then((c) => {
      client = c;
    }).then(() => {
      return sendRequestAndReadResponse(client, `get ${key}\r\nget`);
    }).then((response) => {
      assert.strictEqual(response.elements.length, 1);
      assert.strictEqual(response.elements[0].code, "END");
      return response;
    }).then((response) => {
      return sendRequestAndReadResponse(client, ` ${key}\r\n`, response);
    }).then((response) => {
      assert.strictEqual(response.elements.length, 1);
      assert.strictEqual(response.elements[0].code, "END");
      return response;
    }).then(() => {
      client.end();
      done();
    }).catch((err) => {
      client.end();
      done(err);
    });
  });

  it('commands split in two steps pending data', (done) => {
    let client = null;
    const key = uuid();
    server.then(() => {
      return connect('localhost', config.processPorts[0]);
    }).then((c) => {
      client = c;
    }).then(() => {
      client.write(`set ${key} 10 0 6\r\nAZE`);
    }).then(() => {
      return sendRequestAndReadResponse(client, `RTY\r\n`);
    }).then((response) => {
      assert.strictEqual(response.elements.length, 1);
      assert.strictEqual(response.elements[0].code, "STORED");
      return response;
    }).then(() => {
      client.end();
      done();
    }).catch((err) => {
      client.end();
      done(err);
    });
  });

  it('set with noreply does not return', (done) => {
    let client = null;
    const key = uuid();
    server.then(() => {
      return connect('localhost', config.processPorts[0]);
    }).then((c) => {
      client = c;
    }).then(() => {
      client.write(`set ${key} 10 0 6 noreply\r\nAZERTY\r\n`);
    }).then(() => {
      return sendRequestAndReadResponse(client, `get ${key}\r\n`);
    }).then((response) => {
      assert.strictEqual(response.elements.length, 2);
      assert.strictEqual(response.elements[0].code, "VALUE");
      assert.strictEqual(response.elements[0].value.toString('utf-8'), "AZERTY");
      assert.strictEqual(response.elements[1].code, "END");
      return response;
    }).then(() => {
      client.end();
      done();
    }).catch((err) => {
      client.end();
      done(err);
    });
  });

  it('should fail with invalid parameters', (done) => {
    assert.throws(() => mc({ shards: "10" }), /The number of shards must be a positive integer up to 20/);
    assert.throws(() => mc({ shards: {} }), /The number of shards must be a positive integer up to 20/);
    assert.throws(() => mc({ shards: -10 }), /The number of shards must be a positive integer up to 20/);
    assert.throws(() => mc({ shards: 0 }), /The number of shards must be a positive integer up to 20/);
    assert.throws(() => mc({ shards: 21 }), /The number of shards must be a positive integer up to 20/);
    assert.throws(() => mc({ shards: 1/0 }), /The number of shards must be a positive integer up to 20/);
    assert.throws(() => mc({ configPort: "10" }), /The configuration port number must be a valid TCP port number \[1-65535\] or 0/);
    assert.throws(() => mc({ configPort: {} }), /The configuration port number must be a valid TCP port number \[1-65535\] or 0/);
    assert.throws(() => mc({ configPort: -10 }), /The configuration port number must be a valid TCP port number \[1-65535\] or 0/);
    assert.throws(() => mc({ configPort: 100000 }), /The configuration port number must be a valid TCP port number \[1-65535\] or 0/);
    done();
  });

  it('can not bind to priviledged ports', (done) => {
    mc({ configPort: 10 }).then(() => {
      done(Error("An error should have been raised"));
    }).catch((err) => {
      if((err instanceof Error) && err.message.match(/Unable to setup configuration server/)) {
        done();
      } else {
        done(Error("Unexpected error: " + err));
      }
    });
  });
});
