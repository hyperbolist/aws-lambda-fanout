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

/* global describe:false, it:false, before:false, after:false */

'use strict';

// Environment settings for AWS SDK
process.env.AWS_REGION            = 'xx-test-1'; 
process.env.AWS_ACCESS_KEY_ID     = "AKIAENVEXAMPLE";
process.env.AWS_SECRET_ACCESS_KEY = "SecretKeyExample";
process.env.AWS_SESSION_TOKEN     = "SessionTokenExample";

const AWS      = require('./mock-aws.js');
const services = require('../lib/services.js');
const uuid     = require('uuid');
const assert   = require('assert');
const sinon    = require('sinon');

describe('services', () => {
	let processRequest = null;
	let createService = null;
	before(() => {
		AWS.mock('STS','assumeRole', (params) => {
			return processRequest(params);
		});
		AWS.mock('STS','credentialsFrom', () => {
			return {};
		});
	});

	after(() => {
		AWS.restore('STS','credentialsFrom');
		AWS.restore('STS','assumeRole');
	});

	it('configure', (done) => {
		let customConfig  = { value1: 11, value2: 12, value3: 13, value4: 14 };
		let customConfig2 = { value1: 21, value2: 22, value3: 23, value4: 24 };
		services.definitions.custom = {
			isCustom: true,
			configure: (conf) => {
				for(const i in conf) {
					customConfig[i] = conf[i];
				}
			},
			create: (target, options) => {
				return createService(target, options);
			}
		};
		services.definitions.custom2 = {
			configure: (conf) => {
				for(const i in conf) {
					customConfig2[i] = conf[i];
				}
			}
		};
		services.configure({ custom: { value1: 31 }, custom2: { value2: 32 }, value3: 33, value5: 35, unknown: {} });
		assert.strictEqual(customConfig.value1, 31);
		assert.strictEqual(customConfig.value2, 12);
		assert.strictEqual(customConfig.value3, 33);
		assert.strictEqual(customConfig.value4, 14);
		assert.strictEqual(customConfig.value5, 35);
		assert.strictEqual(customConfig2.value1, 21);
		assert.strictEqual(customConfig2.value2, 32);
		assert.strictEqual(customConfig2.value3, 33);
		assert.strictEqual(customConfig2.value4, 24);
		assert.strictEqual(customConfig2.value5, 35);
		done();
	});

	it('setAWS', (done) => {
		services.setAWS(AWS);
		done();
	});

	it('get', () => {
		const key = uuid();
		services.definitions[key] = {
			customId: key,
			create: (target, options) => {
				return createService(target, options);
			}
		};

		createService = (target) => {
			return target.id;
		};
		return services.get({ id: key, type: key }).then((serviceReference) => {
			assert.strictEqual(serviceReference.definition.customId, key);
			assert.strictEqual(serviceReference.service, key);
			serviceReference.dispose();
		});
	});

	it('get empty region', () => {
		const key = uuid();
		services.definitions[key] = {
			customId: key,
			create: (target, options) => {
				return createService(target, options);
			}
		};

		createService = (target) => {
			return target.id;
		};
		return services.get({ id: key, type: key, region: '' }).then((serviceReference) => {
			assert.strictEqual(serviceReference.definition.customId, key);
			assert.strictEqual(serviceReference.service, key);
			serviceReference.dispose();
		});
	});

	it('get empty role', () => {
		const key = uuid();
		services.definitions[key] = {
			customId: key,
			create: (target, options) => {
				return createService(target, options);
			}
		};

		createService = (target) => {
			return target.id;
		};
		return services.get({ id: key, type: key, role: '' }).then((serviceReference) => {
			assert.strictEqual(serviceReference.definition.customId, key);
			assert.strictEqual(serviceReference.service, key);
			serviceReference.dispose();
		});
	});

	it('get with region', () => {
		const key = uuid();
		services.definitions[key] = {
			customId: key,
			create: (target, options) => {
				return createService(target, options);
			}
		};

		createService = (target, options) => {
			assert.strictEqual(options.region, 'xx-test-2');
			return target.id;
		};
		return services.get({ id: key, type: key, region: 'xx-test-2' }).then((serviceReference) => {
			assert.strictEqual(serviceReference.definition.customId, key);
			assert.strictEqual(serviceReference.service, key);
			serviceReference.dispose();
		});
	});

	it('get unknown', () => {
		const key = uuid();
		return services.get({ id: key, type: key }).then(() => {
			throw new Error("An error should have been raised");
		}).catch((err) => {
			assert(err instanceof Error);
			assert.strictEqual(err.message, `Unknown service type '${key}'`);
		});
	});

	it('get from pool', () => {
		const key = uuid();
		services.definitions[key] = {
			customId: key,
			create: (target, options) => {
				return createService(target, options);
			}
		};

		createService = (target) => {
			return target.id;
		};
		return services.get({ id: `${key}#1`, type: key }).then((serviceReference) => {
			serviceReference.dispose();
		}).then(() => {
			return services.get({ id: `${key}#2`, type: key });
		}).then((serviceReference) => {
			assert.strictEqual(serviceReference.definition.customId, key);
			assert.strictEqual(serviceReference.service, `${key}#1`);
			serviceReference.dispose();
		});
	});

	it('get from empty pool', () => {
		const key = uuid();
		services.definitions[key] = {
			customId: key,
			create: (target, options) => {
				return createService(target, options);
			}
		};

		let ref1 = null;
		createService = (target) => {
			return target.id;
		};
		return services.get({ id: `${key}#1`, type: key }).then((serviceReference) => {
			ref1 = serviceReference;
		}).then(() => {
			return services.get({ id: `${key}#2`, type: key });
		}).then((serviceReference) => {
			assert.strictEqual(serviceReference.definition.customId, key);
			assert.strictEqual(serviceReference.service, `${key}#2`);
			serviceReference.dispose();
			ref1.dispose();
		});
	});

	it('get after expiration', () => {
		let sessionDuration = 900;
		let clock = sinon.useFakeTimers();
		const key = uuid();
		services.definitions[key] = {
			customId: key,
			create: (target, options) => {
				return createService(target, options);
			}
		};

		processRequest = (params) => {
			assert.strictEqual(params.RoleArn, `${key}#role`);
			assert.strictEqual(params.RoleSessionName, 'Lambda');
			assert.strictEqual(params.DurationSeconds, 900);
			sessionDuration = params.DurationSeconds;
			return Promise.resolve({});
		};
		createService = (target) => {
			return target.id;
		};
		return services.get({ id: `${key}#1`, type: key, role: `${key}#role` }).then((serviceReference) => {
			serviceReference.dispose();
		}).then(() => {
			clock.tick(sessionDuration*1000);
			return services.get({ id: `${key}#2`, type: key, role: `${key}#role` });
		}).then((serviceReference) => {
			assert.strictEqual(serviceReference.definition.customId, key);
			assert.strictEqual(serviceReference.service, `${key}#2`);
			serviceReference.dispose();
		}).then((result) => {
			clock.restore();
			return Promise.resolve(result);
		}).catch((err) => {
			clock.restore();
			return Promise.reject(err);
		});
	});

	it('get from pool after expiration', () => {
		let sessionDuration = 900;
		let clock = sinon.useFakeTimers();
		const key = uuid();
		services.definitions[key] = {
			customId: key,
			create: (target, options) => {
				return createService(target, options);
			}
		};

		processRequest = (params) => {
			assert.strictEqual(params.RoleArn, `${key}#role`);
			assert.strictEqual(params.RoleSessionName, 'Lambda');
			assert.strictEqual(params.DurationSeconds, 900);
			sessionDuration = params.DurationSeconds;
			return Promise.resolve({});
		};
		createService = (target) => {
			return target.id;
		};
		let ref1 = null;
		let ref2 = null;
		return services.get({ id: `${key}#1`, type: key, role: `${key}#role` }).then((serviceReference) => {
			ref1 = serviceReference;
		}).then(() => {
			clock.tick(sessionDuration*1000);
			return services.get({ id: `${key}#2`, type: key, role: `${key}#role` });
		}).then((serviceReference) => {
			ref2 = serviceReference;
			ref1.dispose();
			return services.get({ id: `${key}#3`, type: key, role: `${key}#role` });
		}).then((serviceReference) => {
			assert.strictEqual(serviceReference.definition.customId, key);
			assert.strictEqual(serviceReference.service, `${key}#3`);
			serviceReference.dispose();
			ref2.dispose();
		}).then((result) => {
			clock.restore();
			return Promise.resolve(result);
		}).catch((err) => {
			clock.restore();
			return Promise.reject(err);
		});
	});

	it('get with role', () => {
		const key = uuid();
		services.definitions[key] = {
			customId: key,
			create: (target, options) => {
				return createService(target, options);
			}
		};

		processRequest = (params) => {
			assert.strictEqual(params.RoleArn, `${key}#role`);
			assert.strictEqual(params.RoleSessionName, 'Lambda');
			assert.strictEqual(params.DurationSeconds, 900);
			return Promise.resolve({});
		};
		createService = (target) => {
			return target.id;
		};
		return services.get({ id: key, type: key, role: `${key}#role` }).then((serviceReference) => {
			assert.strictEqual(serviceReference.definition.customId, key);
			assert.strictEqual(serviceReference.service, key);
			serviceReference.dispose();
		});
	});

	it('get with role and externalId', () => {
		const key = uuid();
		services.definitions[key] = {
			customId: key,
			create: (target, options) => {
				return createService(target, options);
			}
		};

		processRequest = (params) => {
			assert.strictEqual(params.ExternalId, `${key}#id`);
			assert.strictEqual(params.RoleArn, `${key}#role`);
			assert.strictEqual(params.RoleSessionName, 'Lambda');
			assert.strictEqual(params.DurationSeconds, 900);
			return Promise.resolve({});
		};
		createService = (target) => {
			return target.id;
		};
		return services.get({ id: key, type: key, role: `${key}#role`, externalId: `${key}#id` }).then((serviceReference) => {
			assert.strictEqual(serviceReference.definition.customId, key);
			assert.strictEqual(serviceReference.service, key);
			serviceReference.dispose();
		});
	});

	it('get with role error', () => {
		const key = uuid();
		services.definitions[key] = {
			customId: key,
			create: (target, options) => {
				return createService(target, options);
			}
		};

		processRequest = () => {
			return Promise.reject(new Error('Not working'));
		};
		createService = (target) => {
			return target.id;
		};
		return services.get({ id: key, type: key, role: `${key}#role` }).then(() => {
			throw new Error("An error should have been raised");
		}).catch((err) => {
			assert(err instanceof Error);
			assert.strictEqual(err.message, `Error assuming role '${key}#role'`);
		});
	});
});
