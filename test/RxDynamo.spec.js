const expect = require('chai').expect
const RxDynamoStub = require('./stubs/RxDynamo.stub.js')
const RxDynamo = RxDynamoStub.RxDynamo

describe('RxDynamo', () => {
	RxDynamoStub.dynamoMethods.map(method => {
		describe(`#${method}(params)`, () => {
			it('should be defined', done => {
				const expected = 'pass'
				RxDynamo[method]('pass')
				.subscribe(
					actual => expect(actual).to.equal(expected),
					error => console.log(error),
					() => done()
				)
			})
		})
	})
})