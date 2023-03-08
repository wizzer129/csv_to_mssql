const fs = require('fs');
const { EOL } = require('os');
const { parse } = require('@fast-csv/parse');

const outputMsSqlInsertQuery = (data) => {
	let table = '[dbo].[table]';
	if (process.argv[2]) {
		table = process.argv[2];
	}
	const headers = Object.keys(data[0]).map((key) => key);
	const formattedValues = data.map((item) => {
		/**
		 * UPDATE THIS FORMAT YOUR DATA STRUCTURE NEEDS (DECIMALS, STRINGS, ETC...)
		 */
		return `('${item.SERIES}', ${item.SIZE}, '${item.SIZE_DISPLAY}', '${item.UNIT}')`;
	});
	/**
	 * Update schema and table name
	 */
	const query = `INSERT INTO ${table} (
	${headers.join(',\n\t')}
)
VALUES
	${formattedValues.join(',\n\t')};`;

	console.log(query);
};

const parseCsv = (data) => {
	const formattedRows = [];
	const stream = parse({ headers: true })
		.on('error', (error) => console.error(error))
		.on('data', (row) => {
			if (row.UNIT === '') {
				row.UNIT = '"';
			}

			formattedRows.push(row);
		})
		.on('end', (rowCount) => {
			console.log(`Total Rows: ${rowCount}`);
			outputMsSqlInsertQuery(formattedRows);
		});

	stream.write(data);
	stream.end();
};

const main = () => {
	const csv = [];
	fs.createReadStream('./pipes_sizes.csv')
		.pipe(parse({ delimiter: ',', from_line: 1 }))
		.on('data', function (row) {
			// csvString += row.join();
			csv.push(row.join(',').replace('"', '""'));
		})
		.on('error', function (error) {
			console.log(error.message);
		})
		.on('end', function () {
			// console.log(csv);
			parseCsv(csv.join(EOL));
		});
};

main();
