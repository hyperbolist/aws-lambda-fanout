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
 * This Node.js script provides a native Node.js CRC 16 implementation (initially used for Redis).
 */
'use strict';

module.exports = (buf) => {
    if((typeof buf) == "string") {
        buf = new Buffer(buf, "utf-8");
    }

    if(! Buffer.isBuffer(buf)) {
        throw new Error("crc16 only supports buffers and strings");
    }

    let crc = 0;
    for(const value of buf) {
        for(let b = 0; b < 8; ++b) {
            const bitFlag = ((crc & 0x8000) !== 0);
            crc = (crc << 1) & 0xffff;
            crc = crc | ((value >> b) & 0x01);
            if(bitFlag) {
                crc = crc ^ 0x8005;
            }
        }
    }

    for (let b = 0; b < 16; ++b) {
        const bitFlag = ((crc & 0x8000) !== 0);
        crc = (crc << 1) & 0xffff;
        if(bitFlag) {
            crc = crc ^ 0x8005;
        }
    }

    let result = 0;
    for (let b = 0; b < 16; ++b) {
        if ((crc & (0x8000 >> b)) !== 0) {
            result = result | (0x0001 << b);
        }
    }

    return result;
}