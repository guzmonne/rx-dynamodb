'use strict';

// Common dependencies
const Rx = require('rx')
const AWS = require('aws-sdk')
/**
 * Constructor that returns an object with function wrappers
 * for every method of the DynamoDB API.
 */
function RxDynamoObsConstructor (options) {
	options || (options = {})
	const db = options.dynamo || new AWS.DynamoDB.DocumentClient({
		region: process.env.SERVERLESS_REGION || options.region || 'us-east-1'
	})
	const dynamoMethods = [
		'batchGet',
		'batchWrite',
		'createSet',
		'delete',
		'get',
		'put',
		'query',
		'scan',
		'update',
	]
	/**
	 * Takes in a method string name and return a function
	 * wrapper around the corresponding DynamoDB API method.
	 * The returning function just returns an observable with
	 * the result of the method call or an error.
	 * @param  {String} method  DynamoDB API method name
	 * @return {Function}       Rx observable function wrapper
	 */
	const dynamoMethodToObservable = (method) => (params, scheduler) => {
		return Rx.Observable.create(observer => {
			db[method](params, (err, data) => {
				if (err)
					observer.onError(err)
				observer.onNext(data)
				observer.onCompleted()
			})
		})
	}
	//////////////////
	// Return value //
	//////////////////
	return dynamoMethods
		.map(method => ({
			[`${method}`]: dynamoMethodToObservable(method)
		}))
		.reduce((acc, pair) => Object.assign({}, acc, pair), {})
}
///////////////////
// Export Object //
///////////////////
exports = module.exports = RxDynamoObsConstructor