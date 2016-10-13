'use strict'

const serverless = process.env.SERVERLESS_STAGE
const stage = process.env.STAGE

exports = module.exports = function() {
	if (serverless === 'dev' || stage === 'dev') {
		console.log.apply(console, arguments)
	}
}
