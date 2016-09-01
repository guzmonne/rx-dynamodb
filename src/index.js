"use strict";

const config = { region: process.env.SERVERLESS_REGION }

// Common
const Rx  = require('rx')
const AWS = require('aws-sdk')

const dynamodb = new AWS.DynamoDB.DocumentClient(config)

// TODO:
// -----
// Improve this module functionality.
// The default functionality should require the inclusion of
// only the Item or Keys being modified. It should provide some
// default DynamoDB default keys, which should be modifiable
// using the options object.
function DynamoDBObservablesConstructor() {
	/**
	 * Returns a function that returns an observable
	 * from a DynamoDB Document client method
	 * @param  {String} method  DynamoDB document client method name
	 * @return {Function}       Function that returns an observable from a DynamoDB method
	 */
	const observableFromDynamodbDocumentClient = (method) => (params, options) => {
	 	// @param  {Object} params  Full DynamoDB document client object or Item if options.default === true
	 	// @param  {Object} options Options to configure the method call
		options      || (options = {})      // options default value
		options.item || (options.item = {}) // options.item default value
		// If default === true, we merge the default Dynamo options
		// with default options.
		if (options.default === true) {
			// If options.table is undefined throw
			if (!options.table) throw new Error('"options.table" is not defined')
			const table    = options.table
			const _params  = options.params || {}
			const defaults = defaultParams[method](params, table)
			// if _params.Item exists it will override the defauls
			params = Object.assign({}, defaults, _params)
		}
		return callDynamodbDocumentClientMethod(method, params)
	}
	/**
	 * Calls the apropiate DynamoDB Document Client method
	 * @param  {String} method Method name
	 * @param  {Object} params Params object
	 * @return {Observer}      DynamoDB document client method observable
	 */
	const callDynamodbDocumentClientMethod = (method, params) => Rx.Observable
		.create(observer => {
			try {
				dynamodb[method](params, (err, data) => {
					if (err) return observer.onError(err)
					observer.onNext(data)
					observer.onCompleted()
				})
			} catch (error) {
				observer.onError(error)
			}
		})
	/**
	 * Returns the params of a basic session
	 * @param  {Object} data Event data
	 * @param  {Date}   date Current date 
	 * @return {Object}      Session Initialization params
	 */
	const putParams = (item, table) => ({
		TableName: table,
		Item: item,
		ReturnConsumedCapacity: 'NONE',
	  ReturnItemCollectionMetrics: 'NONE',
	  ReturnValues: 'NONE'
	})
	const defaultParams = Object.freeze({
		put: putParams
	})
	return Object.freeze({
		getItem   : observableFromDynamodbDocumentClient('get'),
		updateItem: observableFromDynamodbDocumentClient('update'),
		putItem   : observableFromDynamodbDocumentClient('put'),
		queryItem : observableFromDynamodbDocumentClient('query'),
		scam      : observableFromDynamodbDocumentClient('scan'),
	})
}

const DynamodbObs = new DynamoDBObservablesConstructor()

exports = module.exports = DynamodbObs