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

var queue = require('../lib/queue.js');

var assert = require('assert');

describe('queue', () => {
  it('should support empty queue', (done) => {
    queue([], (i) => i).then(() => {
      done();
    }).catch((err) => {
      done(Error("An unexpected error occured"));
    });
  });

  it('should keep order', (done) => {
    queue([1, 2, 3], (i) => i).then((results) => {
      assert.strictEqual(results.length, 3);
      assert.strictEqual(results[0], 1);
      assert.strictEqual(results[1], 2);
      assert.strictEqual(results[2], 3);
      done();
    }).catch((err) => {
      done(Error("An unexpected error occured"));
    });
  });

  it('should keep order even when changed', (done) => {
    const order = [];
    queue([1, 2, 3], (i) => {
      if(i == 2) {
        return new Promise((resolve, reject) => {
          setTimeout(() => {
            order.push(i);
            resolve(i);
          }, 10);
        });
      } else {
        order.push(i);
        return i;
      }
    }, 2).then((results) => {
      assert.deepStrictEqual(results, [1, 2, 3]);
      assert.deepStrictEqual(order, [1, 3, 2]);
      done();
    }).catch((err) => {
      done(Error("An unexpected error occured: " + err));
    });
  });

  it('should manage errors gracefully', (done) => {
    queue([1], () => {
      throw new Error("just because");
    }).then(() => {
      done(Error("An error should have been thrown"));
    }).catch((err) => {
      if((err instanceof Error) && (err.message == "just because")) {
        done();
      } else {
        done(Error("Unexpected error: " + err));
      }
    });
  });

  it('should exit on first error', (done) => {
    const order = [];
    queue([1, 2, 3], (i) => {
      order.push(i);
      if(i == 2) {
        throw new Error("Stopping there");
      }
    }).then(() => {
      done(Error("An error should have been thrown"));
    }).catch((err) => {
      assert.deepStrictEqual(order, [1, 2]);
      if((err instanceof Error) && (err.message == "Stopping there")) {
        done();
      } else {
        throw new Error(`Invalid last item processed ${lastProcessed} or error ${err.message}`);
      }
    }).catch((err) => {
      done(err);
    });
  });

  it('should fail with invalid parameters', (done) => {
    assert.throws(() => queue(null, () => null), Error, 'The first argument must be an array of elements to process');
    assert.throws(() => queue(10, () => null), Error, 'The first argument must be an array of elements to process');
    assert.throws(() => queue("a", () => null), Error, 'The first argument must be an array of elements to process');
    assert.throws(() => queue({}, () => null), Error, 'The first argument must be an array of elements to process');
    assert.throws(() => queue([], null), Error, 'The second argument must a function');
    assert.throws(() => queue([], 10), Error, 'The second argument must a function');
    assert.throws(() => queue([], "a"), Error, 'The second argument must a function');
    assert.throws(() => queue([], {}), Error, 'The second argument must a function');
    assert.throws(() => queue([], () => null, null), Error, 'The optional third argument must a positive number');
    assert.throws(() => queue([], () => null, 0), Error, 'The optional third argument must a positive number');
    assert.throws(() => queue([], () => null, -10), Error, 'The optional third argument must a positive number');
    assert.throws(() => queue([], () => null, "10"), Error, 'The optional third argument must a positive number');
    assert.throws(() => queue([], () => null, {}), Error, 'The optional third argument must a positive number');
    done();
  });
});
