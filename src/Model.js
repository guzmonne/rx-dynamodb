'use strict';

const debug = require('./debug.js')
const RxDynamoConstructor = require('./RxDynamo/Constructor.js')
const Joi = require('joi')
const moment = require('moment')
const base64url = require('base64-url')
const deepAssign = require('deep-assign')
const omitEmpty = require('omit-empty')
const isObject = require('lodash/isObject')

const objectError = (key) => new Error(`"${key}" is not defined`)

/**
 * Model wrapper to access a DynamoDB table
 * @param {Object} config            Model configuration config.
 * @param {String} config.TableName* Name of the table
 * @param {String} config.Schema     Joi Schema
 * @param {String} config.HashKey*   Table Hash Key
 * @param {String} config.RangeKey*  Table Range Key
 * @return {Object}                   Wrapper object.
 */
function ModelConstructor (config) {
	///////////////
	// CONSTANTS //
	///////////////
	const RxDynamo = RxDynamoConstructor({dynamo: config.Dynamo})
	const TableName = config.TableName
	const Schema = config.Schema
	const HashKey = config.HashKey || 'ID'
	const RangeKey = config.RangeKey || null
	const OperandMapping = {
		eq: '=',
		ne: '<>',
		le: '<=',
		lt: '<',
		ge: '>=',
		gt: '>'
	}
	/////////////
	// PRIVATE //
	/////////////
	/**
	 * Takes the options object and returna another object
	 * based on DynamoDB Document Client API.
	 * @param  {Object} options Options object.
	 * @return {Object}         DynamoDB Document Client object
	 */
	const _buildOptions = (options) => {
		debug('= Model._buildOptions', JSON.stringify(options))
		const limit = _limitOptions(options)
		const page = _pageOptions(options)
		const filter = _filterOptions(options)
		const fields = _fieldsOptions(options)
		const result = deepAssign({}, limit, page, filter, fields)
		debug('= Model._buildOptions result', JSON.stringify(result))
		return result
	}
	/**
	 * DynamoDB limit option constructor.
	 * @param  {Object} options Options object.
	 * @return {Object}         DynamoDB limit option.
	 */
	const _limitOptions = (options) => {
		if (!isObject(options)) throw objectError('options')
		return !!options.limit ? {Limit: options.limit} : {}
	}
	/**
	 * DynamoDB page options constructor.
	 * @param  {object} options Options object
	 * @return {object}         DynamoDB page option.
	 */
	const _pageOptions = (options) => {
		debug('= Model._pageOptions', JSON.stringify(options))
		const page = options.page;
		if (!!page) {
			if (page.charAt(0) === '-') {
				const prevPage = page.substring(1, page.length)
				return {
					ExclusiveStartKey: lastEvaluatedKey(prevPage),
					ScanIndexForward: true,
				}
			} else {
				return {
					ExclusiveStartKey: lastEvaluatedKey(page),
					ScanIndexForward: false,
				}
			}
		}
		return {}
	}
	/**
	 * DynamoDb fields option.
	 * @param  {Object} options Options object.
	 * @return {Object}         DynamoDB fields options.
	 */
	const _fieldsOptions = (options) => {
		debug('= Model._fieldsOptions', JSON.stringify(options))
		const params = {}
		if (String(options.include_fields) === 'true' && options.fields) {
			const fields = options.fields.split(',')
			params.ProjectionExpression = fields
				.map(field => `#${field}`)
				.join(',')
			const mapping = fields
				.reduce((acc, attrName) => {
					acc[`#${attrName}`] = attrName
					return acc
				}, {})
			params.ExpressionAttributeNames = mapping
		}
		return params
	}
	/**
	 * DynamoDB filters options.
	 * @param  {Object} options Options object.
	 * @return {Object}         DynamoDB filters object.
	 */
	const _filterOptions = (options) => {
		debug('= Model._filterOptions', JSON.stringify(options))
		const dbOptions = {}
		if (options.filters) {
			const attributeNamesMapping = {}
			const attributeValuesMapping = {}
			const filterExpression = []
			Object.keys(options.filters).forEach(key => {
				const attrName = `#${key}`
				attributeNamesMapping[attrName] = key
				const conditions = options.filters[key]
				Object.keys(conditions).forEach(operand => {
					const value = conditions[operand]
					const attrValue = `:${key}`
					attributeValuesMapping[attrValue] = value
					filterExpression.push(
						_buildFilter(
							attrName,
							attrValue,
							OperandMapping[operand]
						)
					)
				})
			})
			dbOptions.ExpressionAttributeNames = attributeNamesMapping
			dbOptions.ExpressionAttributeValues = attributeValuesMapping
			dbOptions.FilterExpression = filterExpression.join(' AND ')
		}
		return dbOptions
	}
	/**
	 * Filter builder function.
	 * @param  {String} key     Key name.
	 * @param  {String} value   Key value.
	 * @param  {String} operand Key value operand.
	 * @return {String}         Resulting filter.
	 */
	const _buildFilter = (key, value, operand) => 
		[key, operand, value].join(' ')
	/**
	 * Remove unwanted fields from the object.
	 * @param  {Object} item    Item object.
	 * @param  {Object} options Options object.
	 * @return {Object}         Refined item.
	 */
	const _refineItem = (item, options) => {
		debug('= Model._refineItem', JSON.stringify(options))
		const refined = Object.assign({}, item)
		if (String(options.include_fields) === 'false' && options.fields) {
			const fields = options.fields.split(',')
			fields.map(field => delete refined[field])
		}
		return refined
	}
	/**
	 * Applies _refineItem() to a list of items
	 * @param  {Array}  items   Items array.
	 * @param  {Object} options Options object.
	 * @return {Object}         Refined items.
	 */
	const _refineItems = (items, options) => {
		debug('= Model._refineItems', JSON.stringify(options))
		if (String(options.include_fields) === 'false' && options.fields) {
			return items.map(item => _refineItem(item, options))
		} else {
			return items
		}
	}
	/**
	 * Builds the resonse for the client, applying the 
	 * filter options provided for every item.
	 * @param  {Array}  result  DynamoDB items result.
	 * @param  {Object} params  DynamoDB query params.
	 * @param  {Object} options Documen objects.
	 * @return {Object}         Refined response.
	 */
	const _buildResponse = (result, params, options) => {
		const items = result.Items;
		if (_isPaginatingBackwards(options)) items.reverse()
		const response = {items: _refineItems(items, options)}
		const paginationKeys = _buildPaginationKey(
			result,
			params,
			items,
			options
		)
		return deepAssign(response, paginationKeys)
	}
	/**
	 * Builds the pagination key
	 * @param  {Object} result  DynamoDB result.
	 * @param  {Object} params  DynamoDB params.
	 * @param  {Array}  items   DynamoDB result items.
	 * @param  {Object} options Pagination key additional options.
	 * @return {Object}         Pagination key.
	 */
	const _buildPaginationKey = (result, params, items, options) => {
		debug('= Model._buildPaginationKey', JSON.stringify(params))
		const paginationKey = {}
		if (items && items.length > 0) {
			if (_hasNextPage(result, options)) {
				const lastItem = items[items.length - 1]
				const nextPage = _buildNextKey(lastItem)
				Object.assign(paginationKey, nextPage)
			}
			if (!_isFirstPage(result, params, options)) {
				const firstItem = items[0]
				const prevKey = _buildPrevKey(firstItem)
				Object.assign(paginationKey, prevKey)
			}
		}
		return paginationKey;
	}
	/**
	 * Checks wether the result is incomplete and has a next page.
	 * @param  {Object} result  DynamoDB result
	 * @param  {Object} options Additional objects.
	 * @return {Boolean}        Next page query result.
	 */
	const _hasNextPage = (result, options) => 
		!!result.LastEvaluatedKey || _isPaginatingBackwards(options)
	/**
	 * Build the next key
	 * @param  {Object} lastItem Last item
	 * @return {Object}          Next page key.
	 */
	const _buildNextKey = (lastItem) => {
		debug('= Model._buildNextKey', lastItem)
		const lastKey = _buildItemKey(lastItem)
		return {nextPage: nextPage(lastKey)}
	}
	/**
	 * Build the previous key
	 * @param  {Object} firstItem Last item
	 * @return {Object}          Previous page key.
	 */
	const _buildPrevKey = (firstItem) => {
		debug('= Model._buildPrevKey', firstItem)
		const firstItemKey = _buildItemKey(firstItem)
		return {prevPage: prevPage(firstItemKey)}
	}
	/**
	 * Checks if it is the first page.
	 * @param  {Object} result  DynamoDB result
	 * @param  {Object} params  DynamoDB params.
	 * @param  {Object} options Additional options.
	 * @return {Boolean}        First page query result.
	 */
	const _isFirstPage = (result, params, options = {}) => {
    return !params.ExclusiveStartKey ||
      (!!params.ExclusiveStartKey && 
    	_isPaginatingBackwards(options) &&
    	!result.LastEvaluatedKey);
  }
	/**
	 * Checks if paginatio is configured backward.
	 * @param  {Object} options Additional options.
	 * @return {Boolean}        Pagination backwards query result.
	 */
	const _isPaginatingBackwards = (options) => 
		options.page && options.page.charAt(0) === '-'
 	/**
 	 * Validates the model with the schema using Joi.
 	 * @param  {Object} model Item object.
 	 * @return {Boolean}      Schema validation query result.
 	 */
	const _validateSchema = (model) => {
		if (!Schema) return true
		const result = Joi.validate(model, Schema)
		return !result.error
	}
	/**
	 * Builds the DynamoDB Key
	 * @param  {String} hash  Hash key.
	 * @param  {String} range Range key.
	 * @return {Object}       DynamoDB Key.
	 */
	const _buildKey = (hash, range) => {
		const key = {}
		key[HashKey] = hash
		if (!!RangeKey) {
			key[RangeKey] = range
		}
		return key
	}
	/**
	 * Build the DynamoDB Key from an Item
	 * @param  {Object} item Item object.
	 * @return {Object}      DynamoDB Key.
	 */
	const _buildItemKey = (item) => {
		const key = {}
		key[HashKey] = item[HashKey]
		if (!!RangeKey) {
			key[RangeKey] = item[RangeKey]
		}
		return key
	}
	// TODO:
	// Find out what is this for...
	const _buildAttributesUpdates = (params) => {
		const attrUpdates = {}
		for (let key in params) {
			if (key !== HashKey && key !== RangeKey) {
				attrUpdates[key] = {
					Action: 'PUT',
					Value: params[key]
				}
			}
		}
		return attrUpdates
	}
 	////////////
	// PUBLIC //
	////////////
	/**
	 * Save an item to the table.
	 * @param  {Object} item Item to be saved
	 * @return {Observable}  DynamoDB get observable.
	 */
	const save = (item) => {
		debug('= Model.save', item)
		const params = {
			TableName,
			Item: item,
			ReturnValues: 'ALL_OLD',
		}
		params.Item.CreatedAt = moment().unix()
		return RxDynamo.put(params)
	}
	/**
	 * Save all the items to the table as a batch job.
	 * @param  {Array} items List of items to be saved.
	 * @return {Observable}  DynamoDB batchWrite observable.
	 */
	const saveAll = (items) => {
		debug('= Model.saveAll', items)
		const params = {RequestItems: {}}
		params.RequestItems[TableName] = items.map(item => ({
			PutRequest: {Item: omitEmpty(Object.assign({}, item, {
				CreatedAt: moment().unix()
			}))}
		}))
		return RxDynamo.batchWrite(params)
	}
	/**
	 * Delete all the items associated to the keys list
	 * @param  {Array} keys List of object keys to delete.
	 * @return {Observable} DynamoDB batchWrite observable.
	 */
	const destroyAll = (keys) => {
		debug('= Model.destroyAll', keys)
		const params = {RequestItems: {}}
		params.RequestItems[TableName] = keys.map(key => ({
			DeleteRequest: {Key: _buildKey(key[0], key[1])}
		}))
		return RxDynamo.batchWrite(params)
	}
	/**
	 * Gets an item from the table
	 * @param  {String} hash    Hash key value.
	 * @param  {String} range   Range key value.
	 * @param  {Object} options Options to define how to get the item.
	 * @return {Observable}     DynamoDB get observable.
	 */
	const get = (hash, range, options={}) => {
		const defaultParams = {
			TableName,
			Key: _buildKey(hash, range),
		}
		const optionalParams = _buildOptions(options)
		const params = Object.assign({}, defaultParams, optionalParams)
		return RxDynamo.get(params)
			.map(result => {
				return !!result.Item ? _refineItem(result.Item, options) : {}
			})
	}
	/**
	 * Updates an item from the table.
	 * @param  {Object} attrs  Fields to edit.
	 * @param  {String} hash   Hash key.
	 * @param  {String} range  Range key.
	 * @return {Observable}    DynamoDB update observable.
	 */
	const update = (attrs, hash, range) => {
		debug('= Model.update', hash, range, JSON.stringify(attrs))
		const params = {
			TableName,
			Key: _buildKey(hash, range),
			AttributeUpdates: _buildAttributesUpdates(attrs),
			ReturnValues: 'ALL_NEW',
		}
		return RxDynamo.update(params)
			.map(result => result.Attributes)
	}
	/**
	 * Deletes an item from the table
	 * @param  {String} hash  Hash key.
	 * @param  {String} range Range key.
	 * @return {Observable}   DynamoDB delete observable.
	 */
	const destroy = (hash, range) => {
		debug('= Model.destroy', hash)
		const params = {
			TableName,
			Key: _buildKey(hash, range)
		}
		return RxDynamo.delete(params)
			.map(() => true)
	}
	/**
	 * Return all items from a given key
	 * @param  {String} key     Key name.
	 * @param  {String} value   Key value.
	 * @param  {Object} options Options object.
	 * @return {Observable}     DynamoDB query observable.
	 */
	const allBy = (key, value, options = {}) => {
		debug('= Model.allBy', key, value)
		const defaultParams = {
			TableName,
			KeyConditionExpression: '#hkey = :hvalue',
			ExpressionAttributeNames: {'#hkey': key},
			ExpressionAttributeValues: {':hvalue': value},
			ScanIndexForward: false,
		}
		const optionalParams = _buildOptions(options, params)
		const params = deepAssign(defaultParams, optionalParams)
		return RxDynamo.query(params)
			.map(result => _buildResponse(result, defaultParams, options))
	}
	/**
	 * Counts the ammount of items by key.
	 * @param  {String} key   Key name.
	 * @param  {String} value Key value.
	 * @return {Observable}   DynamoDB query observable.
	 */
	const countBy = (key, value) => {
		debug('= Model.countBy', key, value)
		const params = {
			TableName,
			KeyConditionExpression: '#hkey = :hvalue',
			ExpressionAttributeNames: {'#hkey': key},
			ExpressionAttributeValues: {':hvalue': value},
			Select: 'COUNT',
		}
		return RxDynamo.query(params)
			.map(result => result.Count)
	}
	/**
	 * Increments an attribute from an item.
	 * @param  {String} attribute Attribute key name.
	 * @param  {Number} count     Attribute count value.
	 * @param  {String} hash      Item hash key.
	 * @param  {String} range     Item range key.
	 * @return {Observable}       DynamoDB update observable.
	 */
	const increment = (attribute, count, hash, range) => {
		debug('= Model.increment', hash, range, attribute, count)
		const params = {
			TableName,
			Key: _buildKey(hash, range),
			AttributeUpdates: {}
		}
		params.AttributeUpdates[attribute] = {
			Action: 'ADD',
			Value: count,
		}
		return RxDynamo.update(params)
	}
	/**
	 * Increment multiple attributes.
	 * @param  {String} hash         Hash key.
	 * @param  {String} range        Range key.
	 * @param  {Object} attrValuesObj Attribute value key and count value.
	 * @return {Observable}          DynamoDB update observable.
	 */
	const incrementAll = (hash, range, attrValuesObj) => {
		debug('= Model.incrementAttrs', hash, range, attrValuesObj)
		const params = {
			TableName,
			Key: _buildKey(hash, range),
			AttributeUpdates: {},
		}
		for (let key in attrValuesObj) {
			if ({}.hasOwnProperty.call(attrValuesObj, key)) {
				params.AttributeUpdates[key] = {
					Action: 'ADD',
					Value: attrValuesObj[key],
				}
    	}
		}
		return RxDynamo.update(params)
	}
	/**
	 * Previous page constructor.
	 * @param  {Object} key Item key.
	 * @return {String}     Base64 encoded previous key.
	 */
	const prevPage = (key) => {
		debug('= Model.prevPage', key)
		return `-${new Buffer(JSON.stringify(key)).toString('base64')}`
	}
	/**
	 * Next page constructor.
	 * @param  {Object} key Item key.
	 * @return {String}     Base64 encoded previous key.
	 */
	const nextPage = (key) =>
		base64url.encode(JSON.stringify(key))
	/**
	 * Last evaluated key decoder
	 * @param  {String} key Base64 encoded next key.
	 * @return {Object}          Last evaluated key.
	 */
	const lastEvaluatedKey = (key) =>
		JSON.parse(base64url.decode(key))

	const isValid = (object) => {
		debug('= Model.isValid', JSON.stringify(object))
		return _validateSchema(object)
	}
	//////////////////
	// RETURN VALUE //
	//////////////////
	return Object.freeze({
		// PUBLIC
		save,
		saveAll,
		destroyAll,
		get,
		update,
		destroy,
		allBy,
		countBy,
		increment,
		incrementAll,
		prevPage,
		nextPage,
		lastEvaluatedKey,
		isValid,
		// PRIVATE
		_buildOptions,
		_limitOptions,
		_pageOptions,
		_fieldsOptions,
		_filterOptions,
		_buildFilter,
		_refineItem,
		_refineItems,
		_buildResponse,
		_buildPaginationKey,
		_hasNextPage,
		_buildNextKey,
		_buildPrevKey,
		_isFirstPage,
		_isPaginatingBackwards,
		_validateSchema,
		_buildKey,
		_buildItemKey,
		_buildAttributesUpdates,
	})
}

exports = module.exports = ModelConstructor
