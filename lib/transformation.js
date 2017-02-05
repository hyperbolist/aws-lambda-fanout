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
 * This AWS Lambda Node.js package manages the record transformation process
 */

// Modules
const DDB = require('./ddb-utils.js');
const deagg = require('aws-kinesis-agg');

function accumulateRecords(records) {
  return records.reduce((a, b) => { 
    return { 
      success: a.success.concat(b.success),
      errors: a.errors.concat(b.errors)
    };
  }, { success: [], errors: [] });
}

function success(record) {
  return { success: [].concat(record), errors: [] };
}

function failure(message, err, record) {
  return { success: [], errors: [ { message: message, error: err, record: record } ] };
}

//********
// This function prepares an Amazon DynamoDB Stream record
function transformDDBRecord(record, target) {
  try {
    var entry = (record.dynamodb.hasOwnProperty("NewImage") ? record.dynamodb.NewImage : {});
    var object = target.convertDDB ? DDB.parseDynamoDBObject(entry, null, { base64Buffers: true }) : entry;
    var data = new Buffer(JSON.stringify(object), 'utf-8');
    var keys = Object.keys(record.dynamodb.Keys);
    var keyEntry = DDB.parseDynamoDBObject(record.dynamodb.Keys, null, { base64Buffers: true });
    var key = "";
    // Concatenate the keys with a '|' separator
    for(var i = 0; i < keys.length; ++i) {
      key = key + (i > 0 ? "|": "") + (keyEntry[keys[i]]);
    }

    return Promise.resolve(success({
      "key": key,
      "sequenceNumber": record.dynamodb.SequenceNumber,
      "subSequenceNumber": 0,
      "data": data,
      "size": data.length,
      "action": record.eventName,
      "source": record.eventSource,
      "region": record.awsRegion
    }));
  } catch(e) {
    return Promise.resolve(failure("Unable to deserialize DynamoDB record, removing it", e, record));
  }
}

//********
// This function prepares a single Amazon Kinesis Stream record
function transformKinesisSingleRecord(record, target) {
  try {
    var data = new Buffer(record.kinesis.data, 'base64');
    return Promise.resolve(success({
      "key": record.kinesis.partitionKey,
      "sequenceNumber": record.kinesis.sequenceNumber,
      "subSequenceNumber": 0,
      "data": data,
      "size": data.length,
      "action": "PUT",
      "source": record.eventSource,
      "region": record.awsRegion
    }));
  } catch(e) {
    return Promise.resolve(failure("Unable to deserialize Kinesis record, removing it", e, record));
  }
}

//********
// This function prepares an Amazon Kinesis Stream record aggregated with the Amazon KPL (Kinesis Producer Library)
function transformKinesisAggregatedRecords(record, target) {
  return new Promise((resolve, reject) => {
    try {
      deagg.deaggregateSync(record.kinesis, true, function(err, subRecords) {
        if (err) {
          resolve(failure("Unable to deserialize KPL record, removing it", err, record));
        } else {
          Promise.all(subRecords.map((subRecord) =>  {
            return transformKinesisSingleRecord({"kinesis": subRecord, "eventSource": record.eventSource, "region": record.region}, target);
          })).then(accumulateRecords).then(resolve).catch(reject);
        }
      });
    } catch(e) {
      resolve(failure("Unable to deserialize KPL record, removing it", e, record));
    }
  });
}

//********
// This function prepares an Amazon SNS record
function transformKinesisRecord(record, target) {
  try {
    if(target.deaggregate) {
      return transformKinesisAggregatedRecords(record, target);
    } else {
      return transformKinesisSingleRecord(record, target);
    }
  } catch(e) {
    return Promise.resolve(failure("Unable to deserialize Kinesis record, removing it", e, record));
  }
}

//********
// This function prepares an Amazon SNS record
function transformSNSRecord(record, target) {
  try {
    var data = new Buffer(record.Sns.Message);
    return Promise.resolve(success({
      "key": record.Sns.MessageId,
      "sequenceNumber": record.Sns.Timestamp,
      "subSequenceNumber": 0,
      "data": data,
      "size": data.length,
      "action": "NOTIFICATION",
      "source": record.EventSubscriptionArn,
      "region": record.EventSubscriptionArn.split(':')[3]
    }));
  } catch(e) {
    return Promise.resolve(failure("Unable to deserialize SNS record, removing it", e, record));
  }
}

//********
// This function prepares an Amazon SNS record
function transformFirehoseRecord(record, target) {
  try {
    var data = new Buffer(record.firehose.data, 'base64');
    return Promise.resolve(success({
      "key": record.invocationId,
      "sequenceNumber": record.firehose.recordId,
      "subSequenceNumber": 0,
      "data": data,
      "size": data.length,
      "action": "PUT",
      "source": record.eventSourceARN,
      "region": record.awsRegion
    }));
  } catch(e) {
    return Promise.resolve(failure("Unable to deserialize Firehose record, removing it", e, record));
  }
}

//********
// This function prepares the records for further processing
module.exports = (sourceRecords, target) => {
  if(! Array.isArray(sourceRecords)) {
    return Promise.reject(Error("Invalid attribute, expecting records array"));
  }
  return Promise.all(sourceRecords.map((record) => {
    try {
      const eventSource = record.eventSource || record.EventSource;
      switch(eventSource) {
        case "aws:kinesis": {
          return transformKinesisRecord(record, target);
        }
        case "aws:dynamodb": {
          return transformDDBRecord(record, target);
        }
        case "aws:sns": {
          return transformSNSRecord(record, target);
        }
        case "aws:firehose": {
          return transformFirehoseRecord(record, target);
        }
        default: {
          return Promise.resolve(failure("Unknown record type, removing it", Error("Unknown event type: " + (record.eventSource || record.EventSource)), record));
        }
      }
    } catch(e) {
      return Promise.resolve(failure("Unable to deserialize SNS record, removing it", e, record));
    }
  })).then(accumulateRecords);
};
