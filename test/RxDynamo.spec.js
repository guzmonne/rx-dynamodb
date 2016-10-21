'use strict'

const expect = require('chai').expect
const RxDynamo = require('../src/RxDynamo/')

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

describe('RxDynamo', () => {
	dynamoMethods.map(method => {
		describe(`#${method}(params)`, () => {
			it('should be defined', () => {
				expect(!!RxDynamo[method]).to.be.true
			})

			it('should return an observable', () => {
				expect(RxDynamo[method]({pass: true}).subscribe).to.be.instanceof(Function)
			})
		})
	})
})