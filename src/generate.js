try { require('dotenv').config() } catch (ex) { /* Do nothing */ }

const fs = require('fs')
const path = require('path')
const _ = require('underscore')
const moment = require('moment-timezone')
const mysql = require('mysql')

const basePath = path.resolve(__dirname, '..')
const outputFile = path.resolve(basePath, 'data', moment().utc().toISOString() + '.tsv')

console.log('Output file:', path.relative(basePath, outputFile))

const concavaClient = mysql.createConnection({
	host: process.env.MYSQL_HOST,
	user: process.env.MYSQL_USER,
	password: process.env.MYSQL_PASSWORD,
	database: process.env.MYSQL_CONCAVA_DATABASE,
})
concavaClient.connect()

const measurementsClient = mysql.createConnection({
	host: process.env.MYSQL_HOST,
	user: process.env.MYSQL_USER,
	password: process.env.MYSQL_PASSWORD,
	database: process.env.MYSQL_MEASUREMENTS_DATABASE,
})
measurementsClient.connect()

var done = function (err) {
	if (err) console.error(err)

	concavaClient.end()
	measurementsClient.end()
}

var createSpreadsheet = function (rows, delimiter = '\t') {
	var columns = _.chain(rows)
		.map((row) => _.keys(row))
		.flatten()
		.uniq()
		.value()

	var defaultValues = _.object(columns, new Array(columns.length).fill(''))

	return columns.join(delimiter) +
		'\n' +
		_.chain(rows)
			.map((row) => _.pick(row, ...columns))
			.map((row) => Object.assign({}, defaultValues, row))
			.map((row) => _.values(row).join(delimiter))
			.value()
			.join('\n')
}

concavaClient.query(`
	SELECT d.name AS deviceName, d.udid, t.name AS templateName
	FROM devices d
	INNER JOIN templates t ON t.id = d.template_id
	ORDER BY d.name ASC

`, (err, results) => {
	if (err) return done(err)

	var rows = []

	measurementsClient.query(
		'SELECT ' + results.map((result) => `
			(SELECT UNIX_TIMESTAMP(timestamp) FROM \`${result.udid}\` ORDER BY timestamp DESC LIMIT 1) AS \`${result.udid}\`
		`).join(','),
		(err, results2) => {
			if (err) return done(err)

			var lastTimestamps = results2[0]

			results.forEach((result) => {
				var lastTimestamp = lastTimestamps[result.udid] * 1000

				rows.push({
					'Device name': result.deviceName,
					'UDID': result.udid,
					'ConCaVa template': result.templateName,
					'Last timestamp': (
						lastTimestamp > 0
						? moment.utc(lastTimestamp).format('YYYY-MM-DD HH:mm:ss')
						: ''
					),
				})
			})

			fs.writeFileSync(outputFile, createSpreadsheet(rows), { encoding: 'UTF-8' })
			done()
		}
	)
})
