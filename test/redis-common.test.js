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
 * This Node.js script tests the features from the redis-common.js script
 */

/* global describe:false, it:false */

'use strict';

const common = require('../lib/redis-common.js');
const assert = require('assert');

describe('redis-common', () => {
	describe('#Deserialize', () => {
		it('deserialize simple string', (done) => {
			let item = null;

			item = common.deserialize(new Buffer("+OK\r\n", 'utf-8'), 0);
			assert.strictEqual(item.processed, true);
			assert.strictEqual(item.result, "OK");

			item = common.deserialize(new Buffer("+A longer message\r\n", 'utf-8'), 0);
			assert.strictEqual(item.processed, true);
			assert.strictEqual(item.result, "A longer message");

			item = common.deserialize(new Buffer("+\r\n", 'utf-8'), 0);
			assert.strictEqual(item.processed, true);
			assert.strictEqual(item.result, "");

			item = common.deserialize(new Buffer("garbage+OK\r\ngarbage", 'utf-8'), 7);
			assert.strictEqual(item.processed, true);
			assert.strictEqual(item.result, "OK");

			item = common.deserialize(new Buffer("+OK", 'utf-8'), 0);
			assert.strictEqual(item.processed, false);
			assert.strictEqual(item.result, null);
			assert.strictEqual(item.needMore, true);

			done();
		});

		it('deserialize numbers', (done) => {
			let item = null;

			item = common.deserialize(new Buffer(":0\r\n", 'utf-8'), 0);
			assert.strictEqual(item.processed, true);
			assert.strictEqual(item.result, 0);

			item = common.deserialize(new Buffer(":-1\r\n", 'utf-8'), 0);
			assert.strictEqual(item.processed, true);
			assert.strictEqual(item.result, -1);

			item = common.deserialize(new Buffer(":101\r\n", 'utf-8'), 0);
			assert.strictEqual(item.processed, true);
			assert.strictEqual(item.result, 101);

			item = common.deserialize(new Buffer("garbage:101\r\ngarbage", 'utf-8'), 7);
			assert.strictEqual(item.processed, true);
			assert.strictEqual(item.result, 101);

			item = common.deserialize(new Buffer(":101", 'utf-8'), 0);
			assert.strictEqual(item.processed, false);
			assert.strictEqual(item.result, null);
			assert.strictEqual(item.needMore, true);

			assert.throws(() => common.deserialize(new Buffer(":\r\n", "utf-8")), /Invalid integer value '' supplied in response/);
			assert.throws(() => common.deserialize(new Buffer(":a\r\n", "utf-8")), /Invalid integer value 'a' supplied in response/);

			done();
		});

		it('deserialize error', (done) => {
			let item = null;

			item = common.deserialize(new Buffer("-ERR\r\n", 'utf-8'), 0);
			assert.strictEqual(item.processed, true);
			assert(item.result instanceof Error, "should be an Error object");
			assert.strictEqual(item.result.message, "ERR");

			item = common.deserialize(new Buffer("-A longer error\r\n", 'utf-8'), 0);
			assert.strictEqual(item.processed, true);
			assert(item.result instanceof Error, "should be an Error object");
			assert.strictEqual(item.result.message, "A longer error");

			item = common.deserialize(new Buffer("-\r\n", 'utf-8'), 0);
			assert.strictEqual(item.processed, true);
			assert(item.result instanceof Error, "should be an Error object");
			assert.strictEqual(item.result.message, "");

			item = common.deserialize(new Buffer("garbage-ERR\r\ngarbage", 'utf-8'), 7);
			assert.strictEqual(item.processed, true);
			assert(item.result instanceof Error, "should be an Error object");
			assert.strictEqual(item.result.message, "ERR");

			item = common.deserialize(new Buffer("-ERR", 'utf-8'), 0);
			assert.strictEqual(item.processed, false);
			assert.strictEqual(item.result, null);
			assert.strictEqual(item.needMore, true);

			done();
		});

		it('deserialize bulk string', (done) => {
			let item = null;

			item = common.deserialize(new Buffer("$-1\r\n", 'utf-8'), 0);
			assert.strictEqual(item.processed, true);
			assert.strictEqual(item.result, null);

			item = common.deserialize(new Buffer("$2\r\nOK\r\n", 'utf-8'), 0);
			assert.strictEqual(item.processed, true);
			assert(Buffer.isBuffer(item.result));
			assert.strictEqual(item.result.toString('utf-8'), "OK");

			item = common.deserialize(new Buffer("$12\r\nOK\tOK\r\nOK OK\r\n", 'utf-8'), 0);
			assert.strictEqual(item.processed, true);
			assert(Buffer.isBuffer(item.result));
			assert.strictEqual(item.result.toString('utf-8'), "OK\tOK\r\nOK OK");

			item = common.deserialize(new Buffer("$0\r\n\r\n", 'utf-8'), 0);
			assert.strictEqual(item.processed, true);
			assert(Buffer.isBuffer(item.result));
			assert.strictEqual(item.result.toString('utf-8'), "");

			item = common.deserialize(new Buffer("garbage$2\r\nOK\r\ngarbage", 'utf-8'), 7);
			assert.strictEqual(item.processed, true);
			assert(Buffer.isBuffer(item.result));
			assert.strictEqual(item.result.toString('utf-8'), "OK");

			item = common.deserialize(new Buffer("$123", 'utf-8'), 0);
			assert.strictEqual(item.processed, false);
			assert.strictEqual(item.result, null);
			assert.strictEqual(item.needMore, true);

			item = common.deserialize(new Buffer("$10\r\n0123456789", 'utf-8'), 0);
			assert.strictEqual(item.processed, false);
			assert.strictEqual(item.result, null);
			assert.strictEqual(item.needMore, true);

			assert.throws(() => common.deserialize(new Buffer("$\r\n0123456789\r\n", "utf-8")), /Invalid length supplied in response/);
			assert.throws(() => common.deserialize(new Buffer("$a\r\n0123456789\r\n", "utf-8")), /Invalid length supplied in response/);
			assert.throws(() => common.deserialize(new Buffer("$-2\r\n0123456789\r\n", "utf-8")), /Invalid length supplied in response/);
			assert.throws(() => common.deserialize(new Buffer("$8\r\n0123456789\r\n", "utf-8")), /Invalid data in buffer/);
			done();
		});

		it('deserialize array', (done) => {
			let item = null;

			item = common.deserialize(new Buffer("*-1\r\n", 'utf-8'), 0);
			assert.strictEqual(item.processed, true);
			assert.strictEqual(item.result, null);

			item = common.deserialize(new Buffer("*0\r\n", 'utf-8'), 0);
			assert.strictEqual(item.processed, true);
			assert.deepStrictEqual(item.result, []);

			item = common.deserialize(new Buffer("*1\r\n$2\r\nOK\r\n", 'utf-8'), 0);
			assert.strictEqual(item.processed, true);
			assert(Array.isArray(item.result));
			assert(Buffer.isBuffer(item.result[0]));
			assert.strictEqual(item.result[0].toString('utf-8'), "OK");

			item = common.deserialize(new Buffer("*1\r\n+OK\r\n", 'utf-8'), 0);
			assert.strictEqual(item.processed, true);
			assert(Array.isArray(item.result));
			assert.strictEqual(item.result[0], "OK");

			item = common.deserialize(new Buffer("*1\r\n:10\r\n", 'utf-8'), 0);
			assert.strictEqual(item.processed, true);
			assert(Array.isArray(item.result));
			assert.strictEqual(item.result[0], 10);

			item = common.deserialize(new Buffer("garbage*1\r\n$2\r\nOK\r\ngarbage", 'utf-8'), 7);
			assert.strictEqual(item.processed, true);
			assert(Array.isArray(item.result));
			assert(Buffer.isBuffer(item.result[0]));
			assert.strictEqual(item.result[0].toString('utf-8'), "OK");

			item = common.deserialize(new Buffer("*1", 'utf-8'), 0);
			assert.strictEqual(item.processed, false);
			assert.strictEqual(item.result, null);
			assert.strictEqual(item.needMore, true);

			item = common.deserialize(new Buffer("*1\r\n$10\r\n0123456789", 'utf-8'), 0);
			assert.strictEqual(item.processed, false);
			assert.strictEqual(item.result, null);
			assert.strictEqual(item.needMore, true);

			item = common.deserialize(new Buffer("*2\r\n$10\r\n0123456789\r\n", 'utf-8'), 0);
			assert.strictEqual(item.processed, false);
			assert.strictEqual(item.result, null);
			assert.strictEqual(item.needMore, true);

			assert.throws(() => common.deserialize(new Buffer("*\r\n0123456789\r\n", "utf-8")), /Invalid length supplied in response/);
			assert.throws(() => common.deserialize(new Buffer("*a\r\n0123456789\r\n", "utf-8")), /Invalid length supplied in response/);
			assert.throws(() => common.deserialize(new Buffer("*-2\r\n0123456789\r\n", "utf-8")), /Invalid length supplied in response/);
			done();
		});

		it('invalid deserialization', (done) => {
			assert.throws(() => common.deserialize(), /You must provide a buffer/);
			assert.throws(() => common.deserialize("value"), /You must provide a buffer/);
			assert.throws(() => common.deserialize(new Buffer("$-1\r\n", 'utf-8'), 6), /Trying to read past end of buffer/);
			assert.deepStrictEqual(common.deserialize(new Buffer(0)), { result: null, processed: false, needMore: true, index: 0 });
			assert.deepStrictEqual(common.deserialize(new Buffer("$-1\r\n", 'utf-8'), 5), { result: null, processed: false, needMore: true, index: 5 });
			assert.throws(() => common.deserialize(new Buffer("@", 'utf-8')), /Unknown object type '@'/);
			assert.throws(() => common.deserialize(new Buffer("\t", 'utf-8')), /Unknown object type 0x09/);
			done();
		});
	});

	describe('#Serialize', () => {
		it('serialize null and undefined', (done) => {
			let item = null;

			item = common.serialize(null);
			assert.deepEqual(item, new Buffer("$-1\r\n", 'utf-8'));

			item = common.serialize();
			assert.deepEqual(item, new Buffer("$-1\r\n", 'utf-8'));

			done();
		});

		it('serialize strings', (done) => {
			let item = null;

			item = common.serialize("OK");
			assert.deepEqual(item, new Buffer("+OK\r\n", 'utf-8'));

			item = common.serialize("A longer message");
			assert.deepEqual(item, new Buffer("+A longer message\r\n", 'utf-8'));

			item = common.serialize("");
			assert.deepEqual(item, new Buffer("+\r\n", 'utf-8'));

			item = common.serialize("A longer message with a \t control character");
			assert.deepEqual(item, new Buffer("$43\r\nA longer message with a \t control character\r\n", 'utf-8'));

			item = common.serialize("A longer message with a \n new line");
			assert.deepEqual(item, new Buffer("$34\r\nA longer message with a \n new line\r\n", 'utf-8'));

			done();
		});

		it('serialize buffers', (done) => {
			let item = null;

			item = common.serialize(new Buffer("OK", 'utf-8'));
			assert.deepEqual(item, new Buffer("$2\r\nOK\r\n", 'utf-8'));

			item = common.serialize(new Buffer(0));
			assert.deepEqual(item, new Buffer("$0\r\n\r\n", 'utf-8'));

			done();
		});

		it('serialize numbers', (done) => {
			let item = null;

			item = common.serialize(0);
			assert.deepEqual(item, new Buffer(":0\r\n", 'utf-8'));

			item = common.serialize(-0);
			assert.deepEqual(item, new Buffer(":0\r\n", 'utf-8'));

			item = common.serialize(10);
			assert.deepEqual(item, new Buffer(":10\r\n", 'utf-8'));

			item = common.serialize(-10);
			assert.deepEqual(item, new Buffer(":-10\r\n", 'utf-8'));

			done();
		});

		it('serialize arrays', (done) => {
			let item = null;

			item = common.serialize([]);
			assert.deepEqual(item, new Buffer("*0\r\n", 'utf-8'));

			item = common.serialize([10]);
			assert.deepEqual(item, new Buffer("*1\r\n:10\r\n", 'utf-8'));

			item = common.serialize([10,'OK']);
			assert.deepEqual(item, new Buffer("*2\r\n:10\r\n+OK\r\n", 'utf-8'));

			item = common.serialize([10,'OK',new Buffer("KO", 'utf-8'), [11]]);
			assert.deepEqual(item, new Buffer("*4\r\n:10\r\n+OK\r\n$2\r\nKO\r\n*1\r\n:11\r\n", 'utf-8'));

			done();
		});

		it('serialize strings', (done) => {
			let item = null;

			item = common.serialize(new Error());
			assert.deepEqual(item, new Buffer("-\r\n", 'utf-8'));

			item = common.serialize(new Error("Error occured"));
			assert.deepEqual(item, new Buffer("-Error occured\r\n", 'utf-8'));

			item = common.serialize(new Error("Error\toccured\nwith control\vcharacters"));
			assert.deepEqual(item, new Buffer("-Error occured with control characters\r\n", 'utf-8'));

			done();
		});

		it('invalid serialization', (done) => {
			assert.throws(() => common.serialize({}), /Unsupported data type/);
			assert.throws(() => common.serialize(new Date()), /Unsupported data type/);
			assert.throws(() => common.serialize(10.5), /Unsupported data type/);
			assert.throws(() => common.serialize(NaN), /Unsupported data type/);
			assert.throws(() => common.serialize(Infinity), /Unsupported data type/);
			assert.throws(() => common.serialize(-Infinity), /Unsupported data type/);
			done();
		});
	});

	describe('#extractHashTag', () => {
		it('no hash tag', (done) => {
			let item = null;

			item = common.extractHashTag("testKey");
			assert.deepEqual(item, "testKey");

			item = common.extractHashTag("");
			assert.deepEqual(item, "");

			done();
		});

		it('valid hash tag', (done) => {
			let item = null;

			item = common.extractHashTag("{full value}");
			assert.deepEqual(item, "full value");

			item = common.extractHashTag("{at beginning}with remaining values");
			assert.deepEqual(item, "at beginning");

			item = common.extractHashTag("a prefix and a value{at end}");
			assert.deepEqual(item, "at end");

			item = common.extractHashTag("a prefix and a value {in the middle} and some others");
			assert.deepEqual(item, "in the middle");

			item = common.extractHashTag("{full { value}");
			assert.deepEqual(item, "full { value");

			item = common.extractHashTag("another {full { value} with closing } tag");
			assert.deepEqual(item, "full { value");

			done();
		});

		it('two hash tags', (done) => {
			let item = null;

			item = common.extractHashTag("{first entry}{second entry}");
			assert.deepEqual(item, "first entry");

			item = common.extractHashTag("a prefix {first entry}{second entry}");
			assert.deepEqual(item, "first entry");

			item = common.extractHashTag("{first entry} with middle content {second entry}");
			assert.deepEqual(item, "first entry");

			item = common.extractHashTag("{first entry}{second entry} a suffix");
			assert.deepEqual(item, "first entry");

			item = common.extractHashTag("a prefix {first entry} with middle content {second entry}");
			assert.deepEqual(item, "first entry");

			item = common.extractHashTag("a prefix {first entry}{second entry} a suffix");
			assert.deepEqual(item, "first entry");

			item = common.extractHashTag("{first entry} with middle content {second entry} a suffix");
			assert.deepEqual(item, "first entry");

			item = common.extractHashTag("a prefix {first entry} with middle content {second entry} a suffix");
			assert.deepEqual(item, "first entry");

			done();
		});

		it('invalid hash tags', (done) => {
			let item = null;

			item = common.extractHashTag("{}");
			assert.deepEqual(item, "{}");

			item = common.extractHashTag("{}with values");
			assert.deepEqual(item, "{}with values");

			item = common.extractHashTag("with {} values");
			assert.deepEqual(item, "with {} values");

			item = common.extractHashTag("with values{}");
			assert.deepEqual(item, "with values{}");

			item = common.extractHashTag("{with values");
			assert.deepEqual(item, "{with values");

			item = common.extractHashTag("with { values");
			assert.deepEqual(item, "with { values");

			item = common.extractHashTag("with values{");
			assert.deepEqual(item, "with values{");

			item = common.extractHashTag("}{with values");
			assert.deepEqual(item, "}{with values");

			item = common.extractHashTag("with }{ values");
			assert.deepEqual(item, "with }{ values");

			item = common.extractHashTag("with values}{");
			assert.deepEqual(item, "with values}{");

			done();
		});

		it('one valid and one invalid hash tag', (done) => {
			let item = null;

			item = common.extractHashTag("{first entry} {}");
			assert.deepEqual(item, "first entry");

			item = common.extractHashTag("{first entry} {}with values");
			assert.deepEqual(item, "first entry");

			item = common.extractHashTag("{first entry} with {} values");
			assert.deepEqual(item, "first entry");

			item = common.extractHashTag("{first entry} with values{}");
			assert.deepEqual(item, "first entry");

			item = common.extractHashTag("{first entry} {with values");
			assert.deepEqual(item, "first entry");

			item = common.extractHashTag("{first entry} with { values");
			assert.deepEqual(item, "first entry");

			item = common.extractHashTag("{first entry} with values{");
			assert.deepEqual(item, "first entry");

			item = common.extractHashTag("{first entry} }{with values");
			assert.deepEqual(item, "first entry");

			item = common.extractHashTag("{first entry} with }{ values");
			assert.deepEqual(item, "first entry");

			item = common.extractHashTag("{first entry} with values}{");
			assert.deepEqual(item, "first entry");

			item = common.extractHashTag("prefix {first entry} {}");
			assert.deepEqual(item, "first entry");

			item = common.extractHashTag("prefix {first entry} {}with values");
			assert.deepEqual(item, "first entry");

			item = common.extractHashTag("prefix {first entry} with {} values");
			assert.deepEqual(item, "first entry");

			item = common.extractHashTag("prefix {first entry} with values{}");
			assert.deepEqual(item, "first entry");

			item = common.extractHashTag("prefix {first entry} {with values");
			assert.deepEqual(item, "first entry");

			item = common.extractHashTag("prefix {first entry} with { values");
			assert.deepEqual(item, "first entry");

			item = common.extractHashTag("prefix {first entry} with values{");
			assert.deepEqual(item, "first entry");

			item = common.extractHashTag("prefix {first entry} }{with values");
			assert.deepEqual(item, "first entry");

			item = common.extractHashTag("prefix {first entry} with }{ values");
			assert.deepEqual(item, "first entry");

			item = common.extractHashTag("prefix {first entry} with values}{");
			assert.deepEqual(item, "first entry");

			item = common.extractHashTag("prefix {first { entry} {}");
			assert.deepEqual(item, "first { entry");

			item = common.extractHashTag("prefix {first { entry} {}with values");
			assert.deepEqual(item, "first { entry");

			item = common.extractHashTag("prefix {first { entry} with {} values");
			assert.deepEqual(item, "first { entry");

			item = common.extractHashTag("prefix {first { entry} with values{}");
			assert.deepEqual(item, "first { entry");

			item = common.extractHashTag("prefix {first { entry} {with values");
			assert.deepEqual(item, "first { entry");

			item = common.extractHashTag("prefix {first { entry} with { values");
			assert.deepEqual(item, "first { entry");

			item = common.extractHashTag("prefix {first { entry} with values{");
			assert.deepEqual(item, "first { entry");

			item = common.extractHashTag("prefix {first { entry} }{with values");
			assert.deepEqual(item, "first { entry");

			item = common.extractHashTag("prefix {first { entry} with }{ values");
			assert.deepEqual(item, "first { entry");

			item = common.extractHashTag("prefix {first { entry} with values}{");
			assert.deepEqual(item, "first { entry");

			done();
		});
	});
});
