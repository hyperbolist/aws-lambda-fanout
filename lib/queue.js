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
 * This Node.js script implements a basic queue with Promises for sequential or parallel processing of a list of elements in an array.
 */
'use strict';

//********
// Creates a queue where all elements in the `elements` array will be passed to the `executor` function
//  - `executor` can either return a Promise object or a value directly
//  - It returns the list of all results in the same order than in the input
//  - `concurrency` An integer for determining how many worker functions should be run in parallel. If omitted, the concurrency defaults to 1.
module.exports = (elements, executor, concurrency) => {
  if(! Array.isArray(elements)) {
    throw new Error("The first argument must be an array of elements to process");
  }
  if((typeof executor) != "function") {
    throw new Error("The second argument must a function");
  }
  if((concurrency !== undefined) && (((typeof concurrency) != "number") || Number.isNaN(concurrency) || (concurrency <= 0))) {
    throw new Error("The optional third argument must a positive number");
  }

  const allElements = [].concat(elements);
  const elementsCount = allElements.length;
  let elementIndex = 0;
  const results = [];
  if(elementIndex >= elementsCount) {
    return Promise.resolve(results);
  }

  let parallelProcessors = 1;
  if(concurrency !== undefined) {
    parallelProcessors = concurrency;
  }

  const processor = () => {
    return new Promise((resolve, reject) => {
      setImmediate(() => {
        try {
          const index = (elementIndex++);
          if(index >= elementsCount) {
            resolve(index);
          } else {
            const element = allElements[index];
            let executionResult = executor(element);
            if(executionResult instanceof Promise) {
              executionResult.then((result) => {
                results[index] = result;
              }).then(processor).then(resolve).catch(reject);
            } else {
              results[index] = executionResult;
              processor().then(resolve).catch(reject);
            }
          }
        } catch(e) {
          reject(e);
        }
      });
    });
  };

  const processors = [];
  for(let i = 0; i < parallelProcessors; ++i) {
      processors.push(processor());
  }

  return Promise.all(processors).then(() => results);
};
