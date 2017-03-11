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

const DDB = require('../lib/ddb-utils.js');
const assert = require('assert');

describe('ddb-utils', () => {
	describe('#parseDynamoDBPropertyValue()', () => {
		it('should support S, N, B, NULL, BOOL', () => {
			assert.strictEqual(DDB.parseDynamoDBPropertyValue({ NULL: true}), null);
			assert.strictEqual(DDB.parseDynamoDBPropertyValue({ S: "" }), "");
			assert.strictEqual(DDB.parseDynamoDBPropertyValue({ S: "string1" }), "string1");
			assert.strictEqual(DDB.parseDynamoDBPropertyValue({ N: "0" }), 0.0);
			assert.strictEqual(DDB.parseDynamoDBPropertyValue({ N: "1000.1" }), 1000.1);
			assert.strictEqual(DDB.parseDynamoDBPropertyValue({ N: "-1000.1" }), -1000.1);
			assert.strictEqual(DDB.parseDynamoDBPropertyValue({ BOOL: true }), true);
			assert.strictEqual(DDB.parseDynamoDBPropertyValue({ BOOL: false }), false);
			assert.deepEqual(DDB.parseDynamoDBPropertyValue({ B: "YjY0VmFs" }), new Buffer("b64Val"));
			assert.deepEqual(DDB.parseDynamoDBPropertyValue({ B: "YjY0VmFs" }, {}), new Buffer("b64Val"));
			assert.deepEqual(DDB.parseDynamoDBPropertyValue({ B: "YjY0VmFs" }, { base64Buffers: false }), new Buffer("b64Val"));
			assert.deepEqual(DDB.parseDynamoDBPropertyValue({ B: "YjY0VmFs" }, { base64Buffers: true }), "YjY0VmFs");
		});
		it('should support SS, NS ans BS', () => {
			assert.deepStrictEqual(DDB.parseDynamoDBPropertyValue({ SS: [ "", "string1", "string2" ]}), [ "", "string1", "string2" ]);
			assert.deepStrictEqual(DDB.parseDynamoDBPropertyValue({ NS: [ "0", "1000.1", "-1000.1" ]}), [ 0, 1000.1, -1000.1 ]);
			assert.deepEqual(DDB.parseDynamoDBPropertyValue({ BS: [ "YjY0dmFs", "YjY0VmFs" ]}), [ new Buffer("b64val"), new Buffer("b64Val") ]);
			assert.deepEqual(DDB.parseDynamoDBPropertyValue({ BS: [ "YjY0dmFs", "YjY0VmFs" ]}, {}), [ new Buffer("b64val"), new Buffer("b64Val") ]);
			assert.deepEqual(DDB.parseDynamoDBPropertyValue({ BS: [ "YjY0dmFs", "YjY0VmFs" ]}, { base64Buffers: false }), [ new Buffer("b64val"), new Buffer("b64Val") ]);
			assert.deepEqual(DDB.parseDynamoDBPropertyValue({ BS: [ "YjY0dmFs", "YjY0VmFs" ]}, { base64Buffers: true }), [ "YjY0dmFs", "YjY0VmFs" ]);
		});
		it('should support map and list', () => {
			assert.deepStrictEqual(DDB.parseDynamoDBPropertyValue({ M: { a: { NULL: true }, b: { S: "string1" }, c: { N: "1000.1" }, d: { BOOL: true } }}), { a: null, b: "string1", c: 1000.1, d: true });
			assert.deepStrictEqual(DDB.parseDynamoDBPropertyValue({ L: [ { NULL: true }, { S: "string1" }, { N: "1000.1" }, { BOOL: true } ]}), [null, "string1", 1000.1, true]);
			assert.deepStrictEqual(DDB.parseDynamoDBPropertyValue({ M: { a: { NULL: true }, b: { S: "string1" }, c: { N: "1000.1" }, d: { BOOL: true }, e: { L: [ { NULL: true }, { S: "string1" }, { N: "1000.1" }, { BOOL: true } ]} }}), { a: null, b: "string1", c: 1000.1, d: true, e: [null, "string1", 1000.1, true] });
			assert.deepStrictEqual(DDB.parseDynamoDBPropertyValue({ L: [ { NULL: true }, { S: "string1" }, { N: "1000.1" }, { BOOL: true }, { M: { a: { NULL: true }, b: { S: "string1" }, c: { N: "1000.1" }, d: { BOOL: true } }} ]}), [null, "string1", 1000.1, true, { a: null, b: "string1", c: 1000.1, d: true }]);
			assert.deepStrictEqual(DDB.parseDynamoDBPropertyValue({ M: { a: { NULL: true }, b: { S: "string1" }, c: { N: "1000.1" }, d: { BOOL: true }, e: { M: { a: { NULL: true }, b: { S: "string1" }, c: { N: "1000.1" }, d: { BOOL: true } }} }}), { a: null, b: "string1", c: 1000.1, d: true, e: { a: null, b: "string1", c: 1000.1, d: true } });
			assert.deepStrictEqual(DDB.parseDynamoDBPropertyValue({ L: [ { NULL: true }, { S: "string1" }, { N: "1000.1" }, { BOOL: true }, { L: [ { NULL: true }, { S: "string1" }, { N: "1000.1" }, { BOOL: true } ]} ]}), [null, "string1", 1000.1, true, [null, "string1", 1000.1, true]]);
		});
		it('should throw on invalid values', () => {
			assert.throws(() => DDB.parseDynamoDBPropertyValue(), /Can not process null or undefined properties/);
			assert.throws(() => DDB.parseDynamoDBPropertyValue(null), /Can not process null or undefined properties/);
			assert.throws(() => DDB.parseDynamoDBPropertyValue({}), /Can not process empty properties/);
			assert.throws(() => DDB.parseDynamoDBPropertyValue({ Z: 0 }), /Unknown property type Z/);
		});
	});

	describe('#parseDynamoDBObject()', () => {
		it('should support null and empty objects', () => {
			assert.deepStrictEqual(DDB.parseDynamoDBObject(null), {});
			assert.deepStrictEqual(DDB.parseDynamoDBObject({}), {});
		});
		it('should support one or multiple properties', () => {
			assert.deepStrictEqual(DDB.parseDynamoDBObject({ a: { S: "string1" } }), { a: "string1" });
			assert.deepStrictEqual(DDB.parseDynamoDBObject({ a: { S: "string1" }, b: { N: "1" } }), { a: "string1", b: 1 });
		});
		it('should throw on non objects', () => {
			assert.throws(() => DDB.parseDynamoDBObject("string1"), /Unsupported entry, expecting object/);
			assert.throws(() => DDB.parseDynamoDBObject(1), /Unsupported entry, expecting object/);
			assert.throws(() => DDB.parseDynamoDBObject(true), /Unsupported entry, expecting object/);
			assert.throws(() => DDB.parseDynamoDBObject([10]), /Unsupported entry, expecting object/);
		});
		it('should support default values', () => {
			const defaultValue = { b: 1 };
			assert.deepStrictEqual(DDB.parseDynamoDBObject(null, defaultValue), { b: 1 });
			assert.deepStrictEqual(DDB.parseDynamoDBObject({}, defaultValue), { b: 1 });
			assert.deepStrictEqual(DDB.parseDynamoDBObject({ a: { S: "string1" } }, defaultValue), { a: "string1", b: 1 });
			assert.deepStrictEqual(DDB.parseDynamoDBObject({ b: { S: "string1" } }, defaultValue), { b: "string1" });
		});
		it('should support options', () => {
			assert.deepEqual(DDB.parseDynamoDBObject({ a: { B: "YjY0dmFs" } }), { a: new Buffer("b64val") });
			assert.deepEqual(DDB.parseDynamoDBObject({ a: { B: "YjY0dmFs" } }, null, { base64Buffers: false }), { a: new Buffer("b64val") });
			assert.deepEqual(DDB.parseDynamoDBObject({ a: { B: "YjY0dmFs" } }, null, { base64Buffers: true }), { a: "YjY0dmFs" });
		});
	});

	describe('#generateDynamoDBProperty()', () => {
		it('should support S, N, B, NULL, BOOL', () => {
			assert.deepStrictEqual(DDB.generateDynamoDBProperty(null), { NULL: true });
			assert.deepStrictEqual(DDB.generateDynamoDBProperty(""), { NULL: true }); // DynamoDB does not support empty strings
			assert.deepStrictEqual(DDB.generateDynamoDBProperty("string1"), { S: "string1" });
			assert.deepStrictEqual(DDB.generateDynamoDBProperty(new Date("2000-01-01T00:00:00.000Z")), { S: "2000-01-01T00:00:00.000Z" });
			assert.deepStrictEqual(DDB.generateDynamoDBProperty(0.0), { N: "0" });
			assert.deepStrictEqual(DDB.generateDynamoDBProperty(1000.1), { N: "1000.1" });
			assert.deepStrictEqual(DDB.generateDynamoDBProperty(-1000.1), { N: "-1000.1" });
			assert.deepStrictEqual(DDB.generateDynamoDBProperty(true), { BOOL: true });
			assert.deepStrictEqual(DDB.generateDynamoDBProperty(false), { BOOL: false });
			assert.deepStrictEqual(DDB.generateDynamoDBProperty(new Buffer("b64Val")), { B: "YjY0VmFs" });
		});
		it('should support SS, NS ans BS', () => {
			assert.deepStrictEqual(DDB.generateDynamoDBProperty([ "", "string1", "string2" ]), { SS: [ "", "string1", "string2" ]});
			assert.deepStrictEqual(DDB.generateDynamoDBProperty([ 0, 1000.1, -1000.1 ]), { NS: [ "0", "1000.1", "-1000.1" ]});
			assert.deepStrictEqual(DDB.generateDynamoDBProperty([ new Buffer("b64val"), new Buffer("b64Val") ]), { BS: [ "YjY0dmFs", "YjY0VmFs" ]});
		});
		it('should support map and list', () => {
			assert.deepStrictEqual(DDB.generateDynamoDBProperty({ a: null, b: "string1", c: 1000.1, d: true }), { M: { a: { NULL: true }, b: { S: "string1" }, c: { N: "1000.1" }, d: { BOOL: true } }});
			assert.deepStrictEqual(DDB.generateDynamoDBProperty([null, "string1", 1000.1, true]), { L: [ { NULL: true }, { S: "string1" }, { N: "1000.1" }, { BOOL: true } ]});
			assert.deepStrictEqual(DDB.generateDynamoDBProperty({ a: null, b: "string1", c: 1000.1, d: true, e: [null, "string1", 1000.1, true] }), { M: { a: { NULL: true }, b: { S: "string1" }, c: { N: "1000.1" }, d: { BOOL: true }, e: { L: [ { NULL: true }, { S: "string1" }, { N: "1000.1" }, { BOOL: true } ]} }});
			assert.deepStrictEqual(DDB.generateDynamoDBProperty([null, "string1", 1000.1, true, { a: null, b: "string1", c: 1000.1, d: true }]), { L: [ { NULL: true }, { S: "string1" }, { N: "1000.1" }, { BOOL: true }, { M: { a: { NULL: true }, b: { S: "string1" }, c: { N: "1000.1" }, d: { BOOL: true } }} ]});
			assert.deepStrictEqual(DDB.generateDynamoDBProperty({ a: null, b: "string1", c: 1000.1, d: true, e: { a: null, b: "string1", c: 1000.1, d: true } }), { M: { a: { NULL: true }, b: { S: "string1" }, c: { N: "1000.1" }, d: { BOOL: true }, e: { M: { a: { NULL: true }, b: { S: "string1" }, c: { N: "1000.1" }, d: { BOOL: true } }} }});
			assert.deepStrictEqual(DDB.generateDynamoDBProperty([null, "string1", 1000.1, true, [null, "string1", 1000.1, true]]), { L: [ { NULL: true }, { S: "string1" }, { N: "1000.1" }, { BOOL: true }, { L: [ { NULL: true }, { S: "string1" }, { N: "1000.1" }, { BOOL: true } ]} ]});
		});
		it('should throw on invalid values', () => {
			assert.throws(() => DDB.generateDynamoDBProperty(), /Unsupported value type/);
		});
	});

	describe('#generateDynamoDBObject()', () => {
		it('should support null and empty objects', () => {
			assert.deepStrictEqual(DDB.generateDynamoDBObject(null), {});
			assert.deepStrictEqual(DDB.generateDynamoDBObject({}), {});
		});
		it('should throw on non objects', () => {
			assert.throws(() => DDB.generateDynamoDBObject("string1"), /Unsupported entry, expecting object/);
			assert.throws(() => DDB.generateDynamoDBObject(1), /Unsupported entry, expecting object/);
			assert.throws(() => DDB.generateDynamoDBObject(true), /Unsupported entry, expecting object/);
			assert.throws(() => DDB.generateDynamoDBObject([10]), /Unsupported entry, expecting object/);
			assert.throws(() => DDB.generateDynamoDBObject(new Buffer("b64Val")), /Unsupported entry, expecting object/);
		});
		it('should support one or multiple properties', () => {
			assert.deepStrictEqual(DDB.generateDynamoDBObject({ a: "string1" }), { a: { S: "string1" } });
			assert.deepStrictEqual(DDB.generateDynamoDBObject({ a: "string1", b: 1 }), { a: { S: "string1" }, b: { N: "1" } });
		});
	});
});
