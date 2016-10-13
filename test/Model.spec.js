const expect = require('chai').expect
const Joi = require('joi')
const base64url = require('base64-url')
const RxDynamo = require('./stubs/RxDynamo.stub.js').RxDynamo
const TableName = 'dynamdb-table-example'
const Model = require('../src/Model.js')({
	TableName,
	Schema: Joi.object().keys({
		ID: Joi.string().required(),
		Range: Joi.string().required(),
		Test: Joi.string().required(),
	}),
	RangeKey: 'Range',
})

describe('Model', () => {
	describe('#_buildOptions(options)', () => {
		it('should return an empty object if options is invalid', () => {
			expect(JSON.stringify(Model._buildOptions({}))).to.equal('{}')
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
			const actual = JSON.stringify(Model._buildOptions(options))
			expect(actual).to.equal(expected)
		})
	})

	describe('#_limitOptions(options)', () => {
		it('should throw if "options" is undefined', () => {
			expect(() => {Model._limitOptions()}).to.throw('"options" is not defined')
		})

		it('should not throw if "options" is defined', () => {
			expect(() => {Model._limitOptions({})}).to.not.throw
		})

		it('should return an empty object if options.limit is not defined', () => {
			expect(JSON.stringify(Model._limitOptions({}))).to.equal('{}')
		})

		it('should return a valid Limit object if options.limit is set', () => {
			const expected = JSON.stringify({Limit: 1})
			const actual = JSON.stringify(Model._limitOptions({limit: 1}))
			expect(actual).to.equal(expected)
		})
	})

	describe('#_pageOptions(options)', () => {
		it('should return an empty object if options.page is undefined', () => {
			expect(JSON.stringify(Model._pageOptions({}))).to.equal('{}')
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
			const actual = JSON.stringify(Model._pageOptions(options))
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
			const actual = JSON.stringify(Model._pageOptions(options))
			expect(actual).to.equal(expected)
		})
	})

	describe('#_filterOptions(options)', () => {
		it('should return an empty object if options.filters is undefined', () => {
			const expected = '{}'
			const actual = JSON.stringify(Model._filterOptions({}))
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
			const actual = JSON.stringify(Model._filterOptions({filters}))
			expect(actual).to.equal(expected)
		})
	})

	describe('#_fieldsOptions(options)', () => {
		it('should return an empty object if options.include_fields or options.fields is false or undefined', () => {
			const expected = '{}'
			expect(JSON.stringify(Model._fieldsOptions({}))).to.equal(expected)
			expect(JSON.stringify(Model._fieldsOptions({fields: ['Test']}))).to.equal(expected)
			expect(JSON.stringify(Model._fieldsOptions({include_fields: false}))).to.equal(expected)
			expect(JSON.stringify(Model._fieldsOptions({include_fields: true}))).to.equal(expected)
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
			const actual = JSON.stringify(Model._fieldsOptions(options))
			expect(actual).to.equal(expected)
		})
	})

	describe('#_buildFilter(key, value, operand)', () => {
		it('should join the three parameters', () => {
			expect(Model._buildFilter('Test', 'Example', '=')).to.equal('Test = Example')
		})
	})

	describe('#_refineItem(item, options)', () => {
		const item = {ID:1, Range: 3, Test: 'Example'}
		const options = {include_fields: false, fields: 'Test,Range'}
		
		it('should return the same item if options.include_fields or options.fields is false or undefined', () => {
			const expected = JSON.stringify(item)
			expect(JSON.stringify(Model._refineItem(item, {}))).to.equal(expected)
			expect(JSON.stringify(Model._refineItem(item, {include_fields: false}))).to.equal(expected)
			expect(JSON.stringify(Model._refineItem(item, {include_fields: true}))).to.equal(expected)
			expect(JSON.stringify(Model._refineItem(item, {fields: false}))).to.equal(expected)
		})

		it('should remove the items stated in options.fields when options.include_fields is set to false', () => {
			const expected = JSON.stringify({ID: 1})
			const actual = JSON.stringify(Model._refineItem(item, options))
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
			expect(JSON.stringify(Model._refineItems(items, {}))).to.equal(expected)
			expect(JSON.stringify(Model._refineItems(items, {include_fields: false}))).to.equal(expected)
			expect(JSON.stringify(Model._refineItems(items, {include_fields: true}))).to.equal(expected)
			expect(JSON.stringify(Model._refineItems(items, {fields: false}))).to.equal(expected)
		})

		it('should remove the items stated in options.fields when options.include_fields is set to false', () => {
			const expected = JSON.stringify([{ID: 1}, {ID: 2}])
			const actual = JSON.stringify(Model._refineItems(items, options))
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
			const actual = JSON.stringify(Model._buildResponse(result, params, {}))
			expect(actual).to.equal(expected)
		})

		it('should return the procesed items when options.include_fields and options.fields is defined', () => {
			const expected = JSON.stringify({
				items: [{ID:1},{ID:2}]
			})
			const actual = JSON.stringify(Model._buildResponse(result, params, options))
			expect(actual).to.equal(expected)
		})

		it('should return the procesed items when options.include_fields and options.fields and pagination key is defined', () => {
			const expected = JSON.stringify({
				items: [{ID:1},{ID:2}],
			})
			const _options = Object.assign({}, options, {page: encodedKey})
			const actual = JSON.stringify(Model._buildResponse(result, params, _options))
			expect(actual).to.equal(expected)
		})

		it('should return the reverse procesed items when options.include_fields and options.fields and pagination key is defined', () => {
			const expected = JSON.stringify({
				items: [{ID:2},{ID:1}],
				nextPage: encodedKey,
			})
			const _options = Object.assign({}, options, {page: '-' + encodedKey})
			const actual = JSON.stringify(Model._buildResponse(result, params, _options))
			expect(actual).to.equal(expected)
		})
	})

	describe('#_buildPaginationKey(result, params, items, options)', () => {
		it('should return an empty object if there are zero items', () => {
			const actual = JSON.stringify(Model._buildPaginationKey({items: []}, params, [], options))
			expect(actual).to.equal('{}')
		})
	})
})