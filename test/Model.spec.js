'use strict'

const chai = require('chai')
const sinon = require('sinon')
const sinonChai = require('sinon-chai')
const Rx = require('rx')
const Joi = require('joi')
const base64url = require('base64-url')
const RxDynamo = require('../src/RxDynamo/')
const Model = require('../src/Model.js')
const expect = chai.expect
chai.use(sinonChai)

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
const just = (arg) => Rx.Observable.just(arg)

describe('Model', () => {
	let TableName, CustomModel, stubs

	before(() => {
		stubs = dynamoMethods.map(method => 
			({[`${method}`]: sinon.stub(RxDynamo, method, just)})
		).reduce((acc, x) => Object.assign({}, acc, x), {})
		TableName = 'dynamdb-table-example'
		CustomModel = Model({
			TableName,
			Schema: Joi.object().keys({
				ID: Joi.string().required(),
				Range: Joi.string().required(),
				Test: Joi.string().required(),
			}),
			RangeKey: 'Range'
		})
	})

	after(() => {
		dynamoMethods.map(method => RxDynamo[method].restore())
	})

	describe('#_buildOptions(options)', () => {
		it('should return an empty object if options is invalid', () => {
			expect(JSON.stringify(CustomModel._buildOptions({}))).to.equal('{}')
		})

		it('should return a valid DynamoDB params object if options is a valid object', () => {
			const key = {ID:1, Range:2}
			const encodedKey = base64url.encode(JSON.stringify(key))
			const options = {
				limit: 4,
				page: encodedKey,
				filters: {
					Range: {gt: 2},
					Test: {eq: 'Example'},
				},
				include_fields: true,
				fields: 'ID,Range,Test'
			}
			const expected = JSON.stringify({
				Limit: 4,
				ExclusiveStartKey: key,
				ScanIndexForward: false,
				ExpressionAttributeNames: {
					'#Range': 'Range',
					'#Test': 'Test',
					'#ID': 'ID',
				},
				ExpressionAttributeValues: {
					':Range': 2,
					':Test': 'Example'
				},
				FilterExpression: '#Range > :Range AND #Test = :Test',
				ProjectionExpression: '#ID,#Range,#Test',
			})
			const actual = JSON.stringify(CustomModel._buildOptions(options))
			expect(actual).to.equal(expected)
		})
	})

	describe('#_limitOptions(options)', () => {
		it('should throw if "options" is undefined', () => {
			expect(() => {CustomModel._limitOptions()}).to.throw('"options" is not defined')
		})

		it('should not throw if "options" is defined', () => {
			expect(() => {CustomModel._limitOptions({})}).to.not.throw
		})

		it('should return an empty object if options.limit is not defined', () => {
			expect(JSON.stringify(CustomModel._limitOptions({}))).to.equal('{}')
		})

		it('should return a valid Limit object if options.limit is set', () => {
			const expected = JSON.stringify({Limit: 1})
			const actual = JSON.stringify(CustomModel._limitOptions({limit: 1}))
			expect(actual).to.equal(expected)
		})
	})

	describe('#_pageOptions(options)', () => {
		it('should return an empty object if options.page is undefined', () => {
			expect(JSON.stringify(CustomModel._pageOptions({}))).to.equal('{}')
		})

		it('should return a valid DynamoDB params object if options.page is defined forward', () => {
			const key = {ID:1, Range:2}
			const encodedKey = base64url.encode(JSON.stringify(key))
			const options = {
				page: encodedKey
			}
			const expected = JSON.stringify({
				ExclusiveStartKey: key,
				ScanIndexForward: false,
			})
			const actual = JSON.stringify(CustomModel._pageOptions(options))
			expect(actual).to.equal(expected)
		})

		it('should return a valid DynamoDB params object if options.page is defined backwards', () => {
			const key = {ID:1, Range:2}
			const encodedKey = '-' + base64url.encode(JSON.stringify(key))
			const options = {
				page: encodedKey
			}
			const expected = JSON.stringify({
				ExclusiveStartKey: key,
				ScanIndexForward: true,
			})
			const actual = JSON.stringify(CustomModel._pageOptions(options))
			expect(actual).to.equal(expected)
		})
	})

	describe('#_filterOptions(options)', () => {
		it('should return an empty object if options.filters is undefined', () => {
			const expected = '{}'
			const actual = JSON.stringify(CustomModel._filterOptions({}))
			expect(actual).to.equal(expected)
		})

		it('should return a valid DynamoDB filter is options.filters is correctly defined', () => {
			const filters = {
				Range: {gt: 2},
				Test: {eq: 'Example'},
			}
			const expected = JSON.stringify({
				ExpressionAttributeNames: {
					'#Range': 'Range',
					'#Test': 'Test'
				},
				ExpressionAttributeValues: {
					':Range': 2,
					':Test': 'Example'
				},
				FilterExpression: '#Range > :Range AND #Test = :Test'
			})
			const actual = JSON.stringify(CustomModel._filterOptions({filters}))
			expect(actual).to.equal(expected)
		})
	})

	describe('#_fieldsOptions(options)', () => {
		it('should return an empty object if options.include_fields or options.fields is false or undefined', () => {
			const expected = '{}'
			expect(JSON.stringify(CustomModel._fieldsOptions({}))).to.equal(expected)
			expect(JSON.stringify(CustomModel._fieldsOptions({fields: ['Test']}))).to.equal(expected)
			expect(JSON.stringify(CustomModel._fieldsOptions({include_fields: false}))).to.equal(expected)
			expect(JSON.stringify(CustomModel._fieldsOptions({include_fields: true}))).to.equal(expected)
		})

		it('should return a valid DynamoDB params object if options.include_fields and options.fields is correctly defined', () => {
			const options = {
				include_fields: true,
				fields: 'ID,Range,Test'
			}
			const expected = JSON.stringify({
				ProjectionExpression: '#ID,#Range,#Test',
				ExpressionAttributeNames: {
					'#ID': 'ID',
					'#Range': 'Range',
					'#Test': 'Test',
				}
			})
			const actual = JSON.stringify(CustomModel._fieldsOptions(options))
			expect(actual).to.equal(expected)
		})
	})

	describe('#_buildFilter(key, value, operand)', () => {
		it('should join the three parameters', () => {
			expect(CustomModel._buildFilter('Test', 'Example', '=')).to.equal('Test = Example')
		})
	})

	describe('#_refineItem(item, options)', () => {
		const item = {ID:1, Range: 3, Test: 'Example'}
		const options = {include_fields: false, fields: 'Test,Range'}
		
		it('should return the same item if options.include_fields or options.fields is false or undefined', () => {
			const expected = JSON.stringify(item)
			expect(JSON.stringify(CustomModel._refineItem(item, {}))).to.equal(expected)
			expect(JSON.stringify(CustomModel._refineItem(item, {include_fields: false}))).to.equal(expected)
			expect(JSON.stringify(CustomModel._refineItem(item, {include_fields: true}))).to.equal(expected)
			expect(JSON.stringify(CustomModel._refineItem(item, {fields: false}))).to.equal(expected)
		})

		it('should remove the items stated in options.fields when options.include_fields is set to false', () => {
			const expected = JSON.stringify({ID: 1})
			const actual = JSON.stringify(CustomModel._refineItem(item, options))
			expect(actual).to.equal(expected)
		})
	})

	describe('#_refineItems(items, options)', () => {
		const items = [
			{ID:1, Range: 3, Test: 'Example'},
			{ID:2, Range: 3, Test: 'Example'},
		]
		const options = {include_fields: false, fields: 'Test,Range'}
		
		it('should return the same items if options.include_fields or options.fields is false or undefined', () => {
			const expected = JSON.stringify(items)
			expect(JSON.stringify(CustomModel._refineItems(items, {}))).to.equal(expected)
			expect(JSON.stringify(CustomModel._refineItems(items, {include_fields: false}))).to.equal(expected)
			expect(JSON.stringify(CustomModel._refineItems(items, {include_fields: true}))).to.equal(expected)
			expect(JSON.stringify(CustomModel._refineItems(items, {fields: false}))).to.equal(expected)
		})

		it('should remove the items stated in options.fields when options.include_fields is set to false', () => {
			const expected = JSON.stringify([{ID: 1}, {ID: 2}])
			const actual = JSON.stringify(CustomModel._refineItems(items, options))
			expect(actual).to.equal(expected)
		})
	})

	const key = {ID:1, Range:3}
	const encodedKey = base64url.encode(JSON.stringify(key))
	const items =  [
		{ID:1, Range: 3, Test: 'Example'},
		{ID:2, Range: 3, Test: 'Example'},
	]
	const result = {
		Items: items
	}
	const params = {
		TableName,
		KeyConditionExpression: '#hkey = :hvalue',
		ExpressionAttributeNames: {'#hkey': 'Test'},
		ExpressionAttributeValues: {':hvalue': 'Example'},
		ScanIndexForward: false,
	}
	const options = {
		include_fields: false,
		fields: 'Test,Range',
	}

	describe('#_buildResponse(result, params, options)', () => {
		it('should return all the items if params and options are default', () => {
			const expected = JSON.stringify({items})
			const actual = JSON.stringify(CustomModel._buildResponse(result, params, {}))
			expect(actual).to.equal(expected)
		})

		it('should return the procesed items when options.include_fields and options.fields is defined', () => {
			const expected = JSON.stringify({
				items: [{ID:1},{ID:2}]
			})
			const actual = JSON.stringify(CustomModel._buildResponse(result, params, options))
			expect(actual).to.equal(expected)
		})

		it('should return the procesed items when options.include_fields and options.fields and pagination key is defined', () => {
			const expected = JSON.stringify({
				items: [{ID:1},{ID:2}],
			})
			const _options = Object.assign({}, options, {page: encodedKey})
			const actual = JSON.stringify(CustomModel._buildResponse(result, params, _options))
			expect(actual).to.equal(expected)
		})

		it('should return the reverse procesed items when options.include_fields and options.fields and pagination key is defined', () => {
			const expected = JSON.stringify({
				items: [{ID:2},{ID:1}],
				nextPage: encodedKey,
			})
			const _options = Object.assign({}, options, {page: '-' + encodedKey})
			const actual = JSON.stringify(CustomModel._buildResponse(result, params, _options))
			expect(actual).to.equal(expected)
		})
	})

	describe('#_buildPaginationKey(result, params, items, options)', () => {
		const params = {TableName}
		const options = {}
		
		it('it should return an empty object if items.length is zero', () => {
			const items = []
			const result = {items}
			const actual = JSON.stringify(CustomModel._buildPaginationKey(result, params, items, options))
			expect(actual).to.equal('{}')
		})

		const LastEvaluatedKey = {ID:2, Range:3}
		const encodedKey = base64url.encode(JSON.stringify(LastEvaluatedKey))
		const items = [{ID:1, Range: 2}, LastEvaluatedKey]
		
		it ('should return the nextPage object with the encoded key', () => {
			const result = {items, LastEvaluatedKey}
			const expected = JSON.stringify({nextPage: encodedKey})
			const actual = JSON.stringify(CustomModel._buildPaginationKey(result, params, items, options))
			expect(actual).to.equal(expected)
		})

		const ExclusiveStartKey = {ID:1, Range:2}
		const encodedExclusiveKey = base64url.encode(JSON.stringify(ExclusiveStartKey))
		it('should return the prevPage object with the encoded key', () => {
			const result = {items, LastEvaluatedKey}
			const _params = Object.assign({}, params, {ExclusiveStartKey})
			const expected = JSON.stringify({nextPage: encodedKey, prevPage: '-' + encodedExclusiveKey})
			const actual = JSON.stringify(CustomModel._buildPaginationKey(result, _params, items, options))
			expect(actual).to.equal(expected)
		})
	})

	describe('#save(item)', () => {
		it('should return a correct DynamoDB params object based on the item to be saved', () => {
			const item = {ID:1, Range:2, Test:'Example'}
			CustomModel.save(item)
			.subscribe(result => {
				expect(result).to.include.keys(
					'TableName', 'Item', 'ReturnValues'
				)
				expect(result.Item).to.include.keys('CreatedAt')
			})	
		})
	})

	describe('#saveAll(items)', () => {
		it('should build a correct DynamoDB params object', () => {
			const items = [
				{ID:1, Range:2, Test: 'Example'}, 
				{ID:2, Range:3, Test: 'Example'}
			]
			CustomModel.saveAll(items)
			.subscribe(result => {
				expect(result).to.include.keys('RequestItems')
				expect(Object.keys(result).length).to.equal(1)
				const table = result.RequestItems[TableName]
				expect(table.length).to.equal(2)
				table.map(row => {
					expect(row).to.include.keys('PutRequest')
					const request = row.PutRequest
					expect(request).to.include.keys('Item')
					expect(request.Item.CreatedAt).to.not.be.undefined
				})
			})
		})
	})

	describe('#destroyAll(keys)', () => {
		it('should build a correct DynamoDB params object', () => {
			const keys = [[1, 2], [2, 2]]
			const schema = {
				RequestItems: Joi.object().keys({
					[`${TableName}`]: Joi.array().items(Joi.object().keys({
						DeleteRequest: Joi.object().keys({
							Key: Joi.object().keys({
								ID: Joi.number().required(),
								Range: Joi.number().required(),
							})
						}).required()
					})).required()
				}).required()
			}
			CustomModel.destroyAll(keys)
			.subscribe(result => {
				expect(!!Joi.validate(result, schema).error).to.be.false
			})
		})
	})
})