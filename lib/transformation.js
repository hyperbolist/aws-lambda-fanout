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
'use strict';

/// http://docs.aws.amazon.com/lambda/latest/dg/eventsources.html

// Modules
const DDB   = require('./ddb-utils.js');
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
function transformDDBRecord(record, convertDDB) {
	try {
		var entry = (record.dynamodb.hasOwnProperty("NewImage") ? record.dynamodb.NewImage : {});
		var object = convertDDB ? DDB.parseDynamoDBObject(entry, null, { base64Buffers: true }) : entry;
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
function transformKinesisSingleRecord(record) {
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
function transformKinesisAggregatedRecords(record) {
	return new Promise((resolve, reject) => {
		try {
			deagg.deaggregateSync(record.kinesis, true, function(err, subRecords) {
				if (err) {
					resolve(failure("Unable to deserialize KPL record, removing it", err, record));
				} else {
					Promise.all(subRecords.map((subRecord) =>  {
						return transformKinesisSingleRecord({"kinesis": subRecord, "eventSource": record.eventSource, "region": record.region}, false);
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
function transformSNSRecord(record) {
	try {
		var data = new Buffer(record.Sns.Message, 'utf-8');
		return Promise.resolve(success({
			"key": record.Sns.MessageId,
			"sequenceNumber": record.Sns.Timestamp,
			"subSequenceNumber": 0,
			"data": data,
			"size": data.length,
			"action": record.Sns.Type,
			"source": record.Sns.TopicArn,
			"region": record.Sns.TopicArn.split(':')[3]
		}));
	} catch(e) {
		return Promise.resolve(failure("Unable to deserialize SNS record, removing it", e, record));
	}
}

//********
// This function prepares an Amazon Kinesis Firehose record
function transformFirehoseRecord(record) {
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
module.exports.records = (sourceRecords, target) => {
	if(! Array.isArray(sourceRecords)) {
		return Promise.reject(Error("Invalid attribute, expecting records array"));
	}
	return Promise.all(sourceRecords.map((record) => {
		try {
			const eventSource = record.eventSource;
			switch(eventSource) {
				case "aws:kinesis": {
					if(target.deaggregate) {
						return transformKinesisAggregatedRecords(record);
					} else {
						return transformKinesisSingleRecord(record);
					}
				}
				case "aws:dynamodb": {
					return transformDDBRecord(record, target.convertDDB);
				}
				case "aws:sns": {
					return transformSNSRecord(record);
				}
				case "aws:firehose": {
					return transformFirehoseRecord(record);
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

module.exports.extractRecords = (event) => {
	let records = [];
	// Kinesis Firehose, to be transformed at the event level
	if(event.hasOwnProperty('records')) {
		records = event.records.map((record) => {
			return {
				invocationId: event.invocationId,
				awsRegion: event.region,
				eventSource: "aws:firehose",
				eventSourceARN: event.deliveryStreamArn,
				firehose: record
			};
		});
	} else if(event.hasOwnProperty('Records')) {
		records = event.Records.map((record) => {
			// SNS Records provide an event source type
			if(record.hasOwnProperty('EventSource')) {
				record.eventSource = record.EventSource;
			}

			// SNS Records provide a Topic ARN
			if(record.hasOwnProperty('Sns')) {
				record.eventSourceARN = record.Sns.TopicArn;
			}
			return record;
		});
	} else {
		throw new Error("This event is invalid, unable to identify a list of records");
	}
	if(records.length > 0) {
		const eventSource = records[0].eventSource;
		const eventSourceARN = records[0].eventSourceARN;
		records.forEach((record) => {
			if((! record.hasOwnProperty('eventSource')) || (! record.hasOwnProperty('eventSourceARN'))) {
				throw new Error(`This event contains invalid records, unable to identify an event or an event source`);
			}
			if((record.eventSource != eventSource) || (record.eventSourceARN != eventSourceARN)) {
				throw new Error(`This event contains multiple event sources, found events from '${record.eventSourceARN}' of type '${record.eventSource}' and '${eventSourceARN}' of type '${eventSource}'`);
			}
			if(! record.hasOwnProperty('awsRegion')) {
				record.awsRegion = record.eventSourceARN.split(':')[3];
			}
		});
	}
	return records;
};
