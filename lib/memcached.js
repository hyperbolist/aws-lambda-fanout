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
 * This Node.js library manages the Amazon ElastiCache Memcached protocol.
 */
'use strict';
// Modules
const crypto = require('crypto');
const net = require('net');
const configuration = require('./configuration.js');

// Default values
const defaultValues = {
  debug         : configuration.getEnvironmentVariable('DEBUG_MODE', "false") == "true", // Activate debug messages
  expiration    : configuration.getEnvironmentVariable('MEMCACHED_RECORD_EXPIRATION', 0),
  tunnel        : configuration.getEnvironmentVariable('MEMCACHED_TUNNEL_MODE', "false") == "true",
  tunnelHost    : configuration.getEnvironmentVariable('MEMCACHED_TUNNEL_HOST', ""),
  tunnelCount   : Number(configuration.getEnvironmentVariable('MEMCACHED_TUNNEL_COUNT', 0)),
  tunnelBasePort: Number(configuration.getEnvironmentVariable('MEMCACHED_TUNNEL_BASE', 0)),
  timeout       : configuration.getEnvironmentVariable('MEMCACHED_CLIENT_TIMEOUT', configuration.getEnvironmentVariable('CLIENT_TIMEOUT', 0))
};

const config = {};

function configure(values) {
  if(values) {
    for(let key in values) {
      config[key] = values[key];
    }
  }
}
exports.configure = configure;
configure(defaultValues);

//********
// This function queries memcached for the list of endpoints
//
// --> Documentation: http://docs.aws.amazon.com/AmazonElastiCache/latest/UserGuide/AutoDiscovery.AddingToYourClientLibrary.html
exports.servers = (endpoint) => {
  if(config.tunnel) {
    const values = [];
    const host = config.tunnelHost;
    const count = config.tunnelCount;
    const base = config.tunnelBasePort;
    for(let i = 0; i < count; ++i) {
      const port = base + i;
      values.push(`${host}:${port}`);
    }
    return Promise.resolve(values);
  } else {
    return new Promise((resolve, reject) => {
      let response = "";
      const errors = [];
      const lines = [];
      const servers = [];
      const parts = endpoint.split(':');

      // Send specific message to server
      if(config.debug) {
        console.log(`Connecting to ${parts[0]}:${parts[1]}`);
      }
      const client = net.connect({ host: parts[0], port: parts[1] }, () => {
        if(config.debug) {
          console.log(`Connected, asking for cluster members`);
        }
        client.write("config get cluster\r\n");
      });
      client.setTimeout(config.timeout);
      client.setEncoding('utf8');

      // Buffer all data and parse response when "END" is received
      client.on('data', function(chunk) {
        response = response + chunk;

        let index = response.indexOf("\r\n");
        while(index != -1) {
          var line = response.substr(0, index); // Strip "\r\n"
          response = response.substr(index+2);
          lines.push(line);

          if(line == "END") {
            // First line (\r\n delimited) is the CONFIG response
            if(lines[0].split(" ")[0] != "CONFIG") {
              console.error("Invalid response from server when requesting MemcacheD cluster configuration", JSON.stringify(lines));
              reject(new Error("Invalid response from server when requesting MemcacheD cluster configuration" + lines[0]), servers);
              client.end();
              break;
            }

            // Second line ("\r\n" delimited) contains the response data
            //  Server names (" " separated) are in the second part ("\n" delimited)
            const serverList = lines[1].split("\n")[1].split(" ");
            for(var i = 0; i < serverList.length; ++i) {
              var info = serverList[i].split("|");
              if(config.debug) {
                console.log(`Server found: ${info[1]}:${info[2]}`);
              }
              servers.push(info[1] + ":" + info[2]);
            }

            // Done, get back the result
            resolve(servers);
            client.end();
            break;
          }
          index = response.indexOf("\r\n");
        }
      });

      client.on('timeout', function() {
        client.removeAllListeners();
        client.end();
        client.destroy();
        console.error("Timeout occured when connecting to memcached to retrieve the server list");
        reject(new Error("Timeout occured when connecting to memcached to retrieve the server list"));
      });

      client.on('error', function(err) {
        console.error("Error occured when retrieving server list:", err);
        reject(new Error("Error occured when connecting to memcached to retrieve the server list"));
      });
    });
  }
};

//********
// This function generates a command to be sent to memcached
//  - servers: a list of servers to use for ElastiCache (uses MD5 based consistend hashing for storage)
//  - records: a list of {key:<string>, data:<string>} records to be sent
//  - callback: a function expecting a single error parameter, not null if an error occured
//
// --> Documentation: https://github.com/memcached/memcached/blob/master/doc/protocol.txt
exports.set = function(servers, records) {
  // Ensure consistent hashing of records using MD5 (not secure for signing but fast for consistent hashing)
  const buckets = {};
  const serversLength = servers.length;
  for(var i = 0; i < records.length; ++i) {
    const record = records[i];
    const hash = crypto.createHash('md5').update(record.key).digest('hex');

    // Compute consistent hashing on first byte (ElastiCache only supports 20 nodes)
    const hashIndex = parseInt(hash.substr(0, 2), 16);
    let server = servers[serversLength - 1];
    for(let j = 1; j < serversLength; ++j) {
      const end = Math.floor((256 / serversLength) * j);
      if(end > hashIndex) {
        server = servers[j - 1];
        break;
      }
    }
    if(! buckets.hasOwnProperty(server)) {
      buckets[server] = [];
    }
    buckets[server].push(record);
  }

  const errors = [];
  const serverNames = Object.keys(buckets);

  return new Promise((resolve, reject) => {
    const processBucket = () => {
      if(serverNames.length > 0) {
        const serverName = serverNames.shift();
        const entries = buckets[serverName];
        const parts = serverName.split(":");

        const storeEntry = (netClient) => {
          if(entries.length > 0) {
            const record = entries.shift();
            netClient.write(`set ${record.key} 0 ${config.expiration} ${record.data.length}\r\n`);
            netClient.write(record.data);
            netClient.write("\r\n");
          } else {
            netClient.end();
            processBucket();
          }
        };

        let response = "";
        // Send first command to the server
        const client = net.connect({ host: parts[0], port: parts[1] }, () => {
          storeEntry(client);
        });
        client.setTimeout(config.timeout);
        client.setEncoding('utf8');

        // Wait for response line
        client.on('data', function(chunk) {
          response = response + chunk;
          var index = response.indexOf("\r\n");
          if(index != -1) {
            // We have an answer, run next iteration
            var code = response.substr(0, index); // Strip "\r\n"
            response = response.substr(index+2);
            if(code != "STORED") {
              console.error("Error occured while sending item to MemcacheD: " + code);
              errors.push(new Error("Error occured while sending item to MemcacheD: " + code));
            }
            storeEntry(client);
          }
        });

        client.on('timeout', () => {
          client.removeAllListeners();
          client.end();
          client.destroy();
          console.error("Timeout occured when storing data");
          errors.push(new Error("Timeout occured when storing data"));
          processBucket();
        });

        client.on('error', (err) => {
          console.error("Error occured when storing data:", err);
          errors.push(new Error("Error occured when storing data: " + err));
          processBucket();
        });
      } else {
        if(errors.length === 0) {
          resolve(null);
        } else {
          reject(new Error("Errors occured while sending data to Memcached"));
        }
      }
    };
    processBucket();
  });
};
