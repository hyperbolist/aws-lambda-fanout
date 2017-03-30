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
 * This Node.js script tests the features from the ddb-utils.js script
 */
 
/* global describe:false, it:false */

'use strict';

const util   = require('../lib/util.js');
const assert = require('assert');
const uuid   = require('uuid');

util.ensureAlways(Promise.prototype);

describe('util', () => {
	describe('#environment', () => {
		it('getEnvString', (done) => {
			const value = uuid();
			process.env.MY_FAKE_STRING = `${value}`;
			assert.strictEqual(util.getEnvString('MY_FAKE_STRING', 'nothing'), value);
			assert.strictEqual(util.getEnvString('MY_FAKE_STRING2', 'nothing'), 'nothing');
			done();
		});
		it('getEnvNumber', (done) => {
			assert.strictEqual(util.getEnvNumber('MY_FAKE_NUMBER2', 10), 10);

			let value = 0;
			process.env.MY_FAKE_NUMBER = `${value}`;
			assert.strictEqual(util.getEnvNumber('MY_FAKE_NUMBER', 10), value);
			value = 20;
			process.env.MY_FAKE_NUMBER = `${value}`;
			assert.strictEqual(util.getEnvNumber('MY_FAKE_NUMBER', 10), value);
			value = -20;
			process.env.MY_FAKE_NUMBER = `${value}`;
			assert.strictEqual(util.getEnvNumber('MY_FAKE_NUMBER', 10), value);
			value = 20.5;
			process.env.MY_FAKE_NUMBER = `${value}`;
			assert.strictEqual(util.getEnvNumber('MY_FAKE_NUMBER', 10), value);
			value = "a";
			process.env.MY_FAKE_NUMBER = `${value}`;
			assert.strictEqual(util.getEnvNumber('MY_FAKE_NUMBER', 10), 10);
			done();
		});
		it('getEnvBool', (done) => {
			assert.strictEqual(util.getEnvBool('MY_FAKE_BOOL2', false), false);
			assert.strictEqual(util.getEnvBool('MY_FAKE_BOOL2', true), true);

			let value = "true";
			process.env.MY_FAKE_BOOL = `${value}`;
			assert.strictEqual(util.getEnvBool('MY_FAKE_BOOL', false), true);
			assert.strictEqual(util.getEnvBool('MY_FAKE_BOOL', true), true);
			value = "on";
			process.env.MY_FAKE_BOOL = `${value}`;
			assert.strictEqual(util.getEnvBool('MY_FAKE_BOOL', false), true);
			assert.strictEqual(util.getEnvBool('MY_FAKE_BOOL', true), true);
			value = "1";
			process.env.MY_FAKE_BOOL = `${value}`;
			assert.strictEqual(util.getEnvBool('MY_FAKE_BOOL', false), true);
			assert.strictEqual(util.getEnvBool('MY_FAKE_BOOL', true), true);
			value = "TrUe";
			process.env.MY_FAKE_BOOL = `${value}`;
			assert.strictEqual(util.getEnvBool('MY_FAKE_BOOL', false), true);
			assert.strictEqual(util.getEnvBool('MY_FAKE_BOOL', true), true);
			value = "false";
			process.env.MY_FAKE_BOOL = `${value}`;
			assert.strictEqual(util.getEnvBool('MY_FAKE_BOOL', false), false);
			assert.strictEqual(util.getEnvBool('MY_FAKE_BOOL', true), false);
			value = "off";
			process.env.MY_FAKE_BOOL = `${value}`;
			assert.strictEqual(util.getEnvBool('MY_FAKE_BOOL', false), false);
			assert.strictEqual(util.getEnvBool('MY_FAKE_BOOL', true), false);
			value = "0";
			process.env.MY_FAKE_BOOL = `${value}`;
			assert.strictEqual(util.getEnvBool('MY_FAKE_BOOL', false), false);
			assert.strictEqual(util.getEnvBool('MY_FAKE_BOOL', true), false);
			value = "FaLsE";
			process.env.MY_FAKE_BOOL = `${value}`;
			assert.strictEqual(util.getEnvBool('MY_FAKE_BOOL', false), false);
			assert.strictEqual(util.getEnvBool('MY_FAKE_BOOL', true), false);
			value = "other";
			process.env.MY_FAKE_BOOL = `${value}`;
			assert.strictEqual(util.getEnvBool('MY_FAKE_BOOL', false), false);
			assert.strictEqual(util.getEnvBool('MY_FAKE_BOOL', true), true);
			done();
		});
	});

	describe('#duplicate()', () => {
		it('should support undefined, null, string, int, Date', (done) => {
			let source = null;
			assert.strictEqual(util.duplicate(source), source);
			source = undefined;
			assert.strictEqual(util.duplicate(source), source);
			source = "string1";
			assert.strictEqual(util.duplicate(source), source);
			source = "";
			assert.strictEqual(util.duplicate(source), source);
			source = 0;
			assert.strictEqual(util.duplicate(source), source);
			source = 1000.1;
			assert.strictEqual(util.duplicate(source), source);
			source = -1000.1;
			assert.strictEqual(util.duplicate(source), source);
			source = true;
			assert.strictEqual(util.duplicate(source), source);
			source = false;
			assert.strictEqual(util.duplicate(source), source);
			source = new Date();
			assert.strictEqual(util.duplicate(source), source);
			source = new Buffer("text", "utf-8");
			assert.strictEqual(util.duplicate(source).toString("utf-8"), source.toString("utf-8"));
			source = () => null;
			assert.throws(() => util.duplicate(source), /Unsupported value type/);
			done();
		});
		it('should support arrays', (done) => {
			let source = [];
			assert.deepStrictEqual(util.duplicate(source), source);
			source = [1, 2, 3];
			assert.deepStrictEqual(util.duplicate(source), source);
			source = [1, "2", 3];
			assert.deepStrictEqual(util.duplicate(source), source);
			source = [1, "2", false];
			assert.deepStrictEqual(util.duplicate(source), source);
			source = [1, "2", false, []];
			assert.deepStrictEqual(util.duplicate(source), source);
			source = [1, "2", false, [12, "b"]];
			assert.deepStrictEqual(util.duplicate(source), source);
			source = [1, "2", false];
			const copy = util.duplicate(source);
			assert.deepEqual(copy, source);
			source[1] = "3";
			assert.notDeepStrictEqual(copy, source);
			done();
		});
		it('should support objects', (done) => {
			let source = {};
			assert.deepStrictEqual(util.duplicate(source), source);
			source = { a: 1 };
			assert.deepStrictEqual(util.duplicate(source), source);
			source = { a: 1, b: "2" };
			assert.deepStrictEqual(util.duplicate(source), source);
			source = { a: 1, b: "2", c: false };
			assert.deepStrictEqual(util.duplicate(source), source);
			source = { a: 1, b: "2", c: false, d: [3, "4", true] };
			assert.deepStrictEqual(util.duplicate(source), source);
			source = { a: 1, b: "2", c: false, d: [3, "4", true], e: { f: 5, g: "6"} };
			assert.deepStrictEqual(util.duplicate(source), source);
			source = { a: 1, b: "2" };
			const copy = util.duplicate(source);
			assert.deepEqual(copy, source);
			source.a = 2;
			assert.notDeepStrictEqual(copy, source);
			done();
		});
	});

	describe('#applyChanges()', () => {
		it('destination not object', (done) => {
			let destination = util.applyChanges(1, { number: 10 });
			assert.deepEqual(destination, { number: 10 });
			done();
		});

		it('source not object', (done) => {
			let destination = util.applyChanges({ immutable: 10 }, 1);
			assert.deepEqual(destination, { immutable: 10 });
			done();
		});

		it('value types no override', (done) => {
			let destination = util.applyChanges({ immutable: 10 }, { number: 1 });
			assert.deepEqual(destination, { immutable: 10, number: 1 });
			destination = util.applyChanges({ immutable: 10 }, { string: "abc" });
			assert.deepEqual(destination, { immutable: 10, string: "abc" });
			destination = util.applyChanges({ immutable: 10 }, { bool: true });
			assert.deepEqual(destination, { immutable: 10, bool: true });
			destination = util.applyChanges({ immutable: 10 }, { buf: new Buffer('AZERTY', 'utf-8') });
			assert.deepEqual(destination, { immutable: 10, buf: new Buffer('AZERTY', 'utf-8') });
			let when = new Date();
			destination = util.applyChanges({ immutable: 10 }, { date: when });
			assert.deepEqual(destination, { immutable: 10, date: when });
			done();
		});

		it('value types override', (done) => {
			let destination = util.applyChanges({ immutable: 10, number: 0 }, { number: 1 });
			assert.deepEqual(destination, { immutable: 10, number: 1 });
			destination = util.applyChanges({ immutable: 10, string: "def" }, { string: "abc" });
			assert.deepEqual(destination, { immutable: 10, string: "abc" });
			destination = util.applyChanges({ immutable: 10, bool: false }, { bool: true });
			assert.deepEqual(destination, { immutable: 10, bool: true });
			destination = util.applyChanges({ immutable: 10, buf: new Buffer('QWERTY', 'utf-8') }, { buf: new Buffer('AZERTY', 'utf-8') });
			assert.deepEqual(destination, { immutable: 10, buf: new Buffer('AZERTY', 'utf-8') });
			let when = new Date();
			destination = util.applyChanges({ immutable: 10, date: new Date(when.valueOf() - 10000) }, { date: when });
			assert.deepEqual(destination, { immutable: 10, date: when });
			done();
		});

		it('object', (done) => {
			let destination = util.applyChanges({ immutable: 10 }, { object: { value1: 11 } });
			assert.deepEqual(destination, { immutable: 10, object: { value1: 11 } });
			destination = util.applyChanges({ immutable: 10, object: { value2: 12 } }, { object: { value1: 11 } });
			assert.deepEqual(destination, { immutable: 10, object: { value1: 11, value2: 12 } });
			destination = util.applyChanges({ immutable: 10, object: { value1: 9 } }, { object: { value1: 11 } });
			assert.deepEqual(destination, { immutable: 10, object: { value1: 11 } });
			done();
		});

		it('array', (done) => {
			let destination = util.applyChanges({ immutable: 10 }, { array: [ 1, 2, 3 ] });
			assert.deepEqual(destination, { immutable: 10, array: [ 1, 2, 3 ] });
			destination = util.applyChanges({ immutable: 10 }, { arrayPrepend: [ 1, 2, 3 ] });
			assert.deepEqual(destination, { immutable: 10, array: [ 1, 2, 3 ] });
			destination = util.applyChanges({ immutable: 10 }, { arrayAppend: [ 1, 2, 3 ] });
			assert.deepEqual(destination, { immutable: 10, array: [ 1, 2, 3 ] });
			destination = util.applyChanges({ immutable: 10, array: [ 4, 5, 6 ] }, { array: [ 1, 2, 3 ] });
			assert.deepEqual(destination, { immutable: 10, array: [ 1, 2, 3 ] });
			destination = util.applyChanges({ immutable: 10, array: [ 4, 5, 6 ] }, { arrayPrepend: [ 1, 2, 3 ] });
			assert.deepEqual(destination, { immutable: 10, array: [ 1, 2, 3, 4, 5, 6 ] });
			destination = util.applyChanges({ immutable: 10, array: [ 4, 5, 6 ] }, { arrayAppend: [ 1, 2, 3 ] });
			assert.deepEqual(destination, { immutable: 10, array: [ 4, 5, 6, 1, 2, 3 ] });
			done();
		});

		it('class', (done) => {
			function MyClass(what) { this.what = what; }
			const obj = new MyClass("test");
			let destination = util.applyChanges({ immutable: 10 }, { object: obj });
			assert.deepEqual(destination, { immutable: 10, object: obj });
			assert.strictEqual(destination.object, obj);
			done();
		});
	});


	describe('#ensureAlways()', () => {
		it('add when no always property', () => {
			const obj = {};
			util.ensureAlways(obj);
			assert.strictEqual(typeof(obj.always), "function");
		});

		it('override when always property not a function', () => {
			const obj = { always: true };
			util.ensureAlways(obj);
			assert.strictEqual(typeof(obj.always), "function");
		});

		it('leave when always property a different function', () => {
			const obj = { always: () => 10 };
			util.ensureAlways(obj);
			assert.strictEqual(typeof(obj.always), "function");
			assert.strictEqual(obj.always(), 10);
		});

		it('add when no always function in prototype', () => {
			function MyPromise() {}
			util.ensureAlways(MyPromise.prototype);
			const obj = new MyPromise();
			assert.strictEqual(typeof(obj.always), "function");
		});

		it('override when always property in prototype not a function', () => {
			function MyPromise() {}
			MyPromise.prototype.always = true;
			util.ensureAlways(MyPromise.prototype);
			const obj = new MyPromise();
			assert.strictEqual(typeof(obj.always), "function");
		});

		it('leave when always property in prototype a different function', () => {
			function MyPromise() {}
			MyPromise.prototype.always = function() { return 10; };
			util.ensureAlways(MyPromise.prototype);
			const obj = new MyPromise();
			assert.strictEqual(typeof(obj.always), "function");
			assert.strictEqual(obj.always(), 10);
		});

		it('manages resolved promises', () => {
			let executed = false;
			return Promise.resolve(10).always(() => {
				executed = true;
			}).then((result) => {
				assert.strictEqual(result, 10);
				assert.strictEqual(executed, true);
			});
		});

		it('manages rejected promises', () => {
			let executed = false;
			return Promise.reject(new Error("Just because")).always(() => {
				executed = true;
			}).then(() => {
				throw new Error("An error should have been raised");
			}).catch((err) => {
				assert(err instanceof Error);
				assert.strictEqual(err.message, "Just because");
				assert.strictEqual(executed, true);
			});
		});

		it('can fail on resolved promises', () => {
			return Promise.resolve(10).always(() => {
				throw new Error("Always error");
			}).then(() => {
				throw new Error("An error should have been raised");
			}).catch((err) => {
				assert(err instanceof Error);
				assert.strictEqual(err.message, "Always error");
			});
		});

		it('can fail on rejected promises', () => {
			return Promise.reject(new Error("Just because")).always(() => {
				throw new Error("Always error");
			}).then(() => {
				throw new Error("An error should have been raised");
			}).catch((err) => {
				assert(err instanceof Error);
				assert.strictEqual(err.message, "Always error");
			});
		});
	});
});
