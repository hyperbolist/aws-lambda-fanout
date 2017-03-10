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
 * This Node.js script tests the features from the crc16.js script against well known CRC16
 */

/* global describe:false, it:false */

'use strict';

const crc    = require('../lib/crc16.js');
const assert = require('assert');

describe('crc', () => {
	it('should support standard values as string', (done) => {
		assert.strictEqual(crc.crc16("123456789"), 0xBB3D);
		assert.strictEqual(crc.crc16("1"), 0xD4C1);
		assert.strictEqual(crc.crc16("test"), 0xF82E);
		done();
	});

	it('should support standard values as buffers', (done) => {
		assert.strictEqual(crc.crc16(new Buffer("123456789", "utf-8")), 0xBB3D);
		assert.strictEqual(crc.crc16(new Buffer("1", "utf-8")), 0xD4C1);
		assert.strictEqual(crc.crc16(new Buffer("test", "utf-8")), 0xF82E);
		done();
	});

	it('should fail if not string or buffer', (done) => {
		assert.throws(() => crc.crc16(true), /crc16 only supports buffers and strings/);
		assert.throws(() => crc.crc16({test: 1}), /crc16 only supports buffers and strings/);
		assert.throws(() => crc.crc16(10), /crc16 only supports buffers and strings/);
		assert.throws(() => crc.crc16([1, 2, 3]), /crc16 only supports buffers and strings/);
		done();
	});
});

describe('xmodem', () => {
	it('should support standard values as string', (done) => {
		assert.strictEqual(crc.xmodem("123456789"), 0x31C3);
		assert.strictEqual(crc.xmodem("1"), 0x2672);
		assert.strictEqual(crc.xmodem("test"), 0x9B06);
		done();
	});

	it('should support standard values as buffers', (done) => {
		assert.strictEqual(crc.xmodem(new Buffer("123456789", "utf-8")), 0x31C3);
		assert.strictEqual(crc.xmodem(new Buffer("1", "utf-8")), 0x2672);
		assert.strictEqual(crc.xmodem(new Buffer("test", "utf-8")), 0x9B06);
		done();
	});

	it('should fail if not string or buffer', (done) => {
		assert.throws(() => crc.xmodem(true), /xmodem \/ crc16 only supports buffers and strings/);
		assert.throws(() => crc.xmodem({test: 1}), /xmodem \/ crc16 only supports buffers and strings/);
		assert.throws(() => crc.xmodem(10), /xmodem \/ crc16 only supports buffers and strings/);
		assert.throws(() => crc.xmodem([1, 2, 3]), /xmodem \/ crc16 only supports buffers and strings/);
		done();
	});
});
