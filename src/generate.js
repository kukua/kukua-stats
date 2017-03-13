try { require('dotenv').config() } catch (ex) { /* Do nothing */ }

const fs = require('fs')
const path = require('path')
const _ = require('underscore')
const moment = require('moment-timezone')
const mysql = require('mysql')
const parallel = require('node-parallel')
const mailgun = require('mailgun-js')({
	apiKey: process.env.MAILGUN_API_KEY,
	domain: process.env.MAILGUN_DOMAIN,
})

const basePath = path.resolve(__dirname, '..')
const outputFile = path.resolve(basePath, 'reports', 'kukua.stats.' + moment().utc().toISOString() + '.tsv')

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

const days = 7
const start = moment().subtract(days, 'days')
const expectedCount = days * 24 * 12 /* per hour (60/5) */
const validPercentage = (count) => Math.min(Math.round(count * 1000 / expectedCount) / 1000, 1)

measurementsClient.config.queryFormat = function (query, values) {
	if ( ! values) return query

	return query
		.replace(/\:\:(\w+)/g, function (txt, key) {
			if (values.hasOwnProperty(key)) {
				return this.escapeId(values[key])
			}
			return txt
		}.bind(this))
		.replace(/\:(\w+)/g, function (txt, key) {
			if (values.hasOwnProperty(key)) {
				return this.escape(values[key])
			}
			return txt
		}.bind(this))
}

const statsQuery = `
	SELECT
		(SELECT UNIX_TIMESTAMP(timestamp)
			FROM ::udid
			WHERE timestamp <= NOW()
			ORDER BY timestamp DESC
			LIMIT 1) AS lastTimestamp,
		(SELECT battery
			FROM ::udid
			WHERE timestamp <= NOW()
			ORDER BY timestamp DESC
			LIMIT 1) AS lastBatteryLevel,
		(SELECT COUNT(*)
			FROM ::udid
			WHERE timestamp >= '${start.toISOString()}'
				AND timestamp <= NOW()
			LIMIT 1) AS rowCount,
		(SELECT COUNT(temp)
			FROM ::udid
			WHERE timestamp >= '${start.toISOString()}'
				AND timestamp <= NOW()
				AND temp < 300
			LIMIT 1) AS validTempCount,
		(SELECT COUNT(humid)
			FROM ::udid
			WHERE timestamp >= '${start.toISOString()}'
				AND timestamp <= NOW()
				AND humid <= 100
			LIMIT 1) AS validHumidCount,
		(SELECT COUNT(rain)
			FROM ::udid
			WHERE timestamp >= '${start.toISOString()}'
				AND timestamp <= NOW()
				AND rain < 300
			LIMIT 1) AS validRainCount,
		(SELECT COUNT(windDir)
			FROM ::udid
			WHERE timestamp >= '${start.toISOString()}'
				AND timestamp <= NOW()
				AND windDir < 360
			LIMIT 1) AS validWindDirCount,
		(SELECT COUNT(windSpeed)
			FROM ::udid
			WHERE timestamp >= '${start.toISOString()}'
				AND timestamp <= NOW()
				AND windSpeed < 1000
			LIMIT 1) AS validWindSpeedCount,
		(SELECT COUNT(gustDir)
			FROM ::udid
			WHERE timestamp >= '${start.toISOString()}'
				AND timestamp <= NOW()
				AND gustDir < 360
			LIMIT 1) AS validGustDirCount,
		(SELECT COUNT(gustSpeed)
			FROM ::udid
			WHERE timestamp >= '${start.toISOString()}'
				AND timestamp <= NOW()
				AND gustSpeed < 1000
			LIMIT 1) AS validGustSpeedCount,
		(SELECT COUNT(pressure)
			FROM ::udid
			WHERE timestamp >= '${start.toISOString()}'
				AND timestamp <= NOW()
				AND pressure < 2000
			LIMIT 1) AS validPressureCount,
		(SELECT COUNT(battery)
			FROM ::udid
			WHERE timestamp >= '${start.toISOString()}'
				AND timestamp <= NOW()
				AND battery < 5000
			LIMIT 1) AS validBatteryCount,
		(SELECT COUNT(*)
			FROM ::udid
			WHERE timestamp > NOW()
			LIMIT 1) AS futureRowCount
`

const determineStatus = (percentages, values) => {
	var minPercentage = 0.95
	var noUploads = 'No uploads.'

	if (values.lastTimestamp.isBefore(start) && values.lastBatteryLevel < 3600) {
		noUploads += ' Probably due to empty battery.'
	}
	if (percentages.measurements === 0) {
		return noUploads
	}
	if (percentages.measurements < minPercentage) {
		return 'Gaps in measurements.'
	}

	var keys = []
	_.forEach(percentages, (percentage, key) => {
		if (key === 'measurements') return
		if (percentage >= minPercentage) return
		keys.push(key)
	})

	if (keys.length === 0) return 'OK'

	return 'Problems with: ' + keys.join(', ')
}

concavaClient.query(`
	SELECT d.name AS deviceName, d.udid, t.name AS templateName
	FROM devices d
	INNER JOIN templates t ON t.id = d.template_id
	WHERE d.name LIKE 'KUKUA_%' OR d.name LIKE 'IITA_%'
	ORDER BY d.name ASC
`, (err, results) => {
	if (err) return done(err)

	var p = parallel().timeout(60 * 1000)
	var rows = []

	results.forEach((result) => {
		p.add((done) => {
			measurementsClient.query(statsQuery, { udid: result.udid }, (err, values) => {
				if (err) return done(`Error in fetching data for ${result.udid}: ${err.message}`)

				values = values[0]

				values.lastTimestamp = (
					values.lastTimestamp > 0
					? moment.utc(values.lastTimestamp * 1000)
					: moment.invalid()
				)

				var percentages = {
					measurements: validPercentage(values.rowCount),
					temperature: validPercentage(values.validTempCount),
					humidity: validPercentage(values.validHumidCount),
					rainfall: validPercentage(values.validRainCount),
					'wind direction': validPercentage(values.validWindDirCount),
					'wind speed': validPercentage(values.validWindSpeedCount),
					'gust direction': validPercentage(values.validGustDirCount),
					'gust speed': validPercentage(values.validGustSpeedCount),
					'pressure': validPercentage(values.validPressureCount),
					'battery level': validPercentage(values.validBatteryCount),
				}

				rows.push({
					'Device name': result.deviceName,
					'UDID': result.udid,
					'ConCaVa template': result.templateName,
					'Last timestamp': (
						values.lastTimestamp.isValid()
						? values.lastTimestamp.format('YYYY-MM-DD HH:mm:ss')
						: ''
					),
					'Measurements': percentages.measurements,
					'Valid temperatures': percentages.temperature,
					'Valid humidities': percentages.humidity,
					'Valid rainfalls': percentages.rainfall,
					'Valid wind direction': percentages['wind direction'],
					'Valid wind speed': percentages['wind speed'],
					'Valid gust direction': percentages['gust direction'],
					'Valid gust speed': percentages['gust speed'],
					'Valid pressure': percentages['pressure'],
					'Valid battery level': percentages['battery level'],
					'Future timestamps': values.futureRowCount,
					'Status': determineStatus(percentages, values),
				})

				done()
			})
		})
	})

	p.done((err) => {
		if (err) return done(err)

		// Write report to file
		fs.writeFileSync(outputFile, createSpreadsheet(rows), { encoding: 'UTF-8' })

		// Send emails
		mailgun.messages().send({
			from: process.env.MAIL_FROM,
			to: process.env.MAIL_TO,
			subject: process.env.MAIL_SUBJECT,
			text: process.env.MAIL_TEXT,
			attachment: outputFile,
		}, (err, body) => done(err))
	})
})
