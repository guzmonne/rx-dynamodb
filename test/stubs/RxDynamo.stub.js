const RxDynamoConstructor = require('../../src/RxDynamo/Constructor.js')

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

const dynamo = dynamoMethods
	.map(method => ({
		[`${method}`]: (params, cb) => cb(null, params) 
	}))
	.reduce((acc, pair) => Object.assign({}, acc, pair), {})

exports = module.exports = {
	RxDynamo: RxDynamoConstructor({dynamo}),
	dynamoMethods,
}
