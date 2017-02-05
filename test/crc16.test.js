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
 * This Node.js script tests the features from the crc16.js script against well known CRC16
 */

'use strict';

var crc16 = require('../lib/crc16.js');

var assert = require('assert');

describe('crc', () => {
  it('should support standard values as string', (done) => {
    assert.strictEqual(crc16("123456789"), 0xBB3D);
    assert.strictEqual(crc16("1"), 0xD4C1);
    assert.strictEqual(crc16("test"), 0xF82E);
    done();
  });

  it('should support standard values as buffers', (done) => {
    assert.strictEqual(crc16(new Buffer("123456789", "utf-8")), 0xBB3D);
    assert.strictEqual(crc16(new Buffer("1", "utf-8")), 0xD4C1);
    assert.strictEqual(crc16(new Buffer("test", "utf-8")), 0xF82E);
    done();
  });

  it('should fail if not string or buffer', (done) => {
    assert.throws(() => crc16(true), Error, 'crc16 should not accept booleans as parameters');
    assert.throws(() => crc16({test: 1}), Error, 'crc16 should not accept objects as parameters');
    assert.throws(() => crc16(10), Error, 'crc16 should not accept numbers as parameters');
    assert.throws(() => crc16([1, 2, 3]), Error, 'crc16 should not accept arrays as parameters');
    done();
  });
});
