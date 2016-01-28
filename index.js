"use strict";

const config = require("./config.json");

const mysql = require("promise-mysql");
const co = require("co");
const _ = require("lodash");

console.log("***: Staring up Schemer");

co(function* coWrap() {
	const schemas = [];
	// loop through every config env in the config
	for (const db of config.databases) {
		// set up the sub-object for this env schema
		const connection = yield getConnection(db);
		const tables = yield getTables(db, connection);
		const schema = yield getSchema(db, connection, tables);
		schemas.push({id: db.id, schema: schema});
	}
	compareTables(schemas);
	console.log("***: Shutting down Schemer");
	return process.exit();
}).catch((err) => {
	console.error(err.stack);
	console.log("***: Schemer is dying...");
	return process.exit();
});

function* getConnection(dbConfig) {
	// set up the connection
	console.log(`** : Getting connection for ${dbConfig.id}`);
	const connection = yield mysql.createConnection({
		host: dbConfig.host,
		user: dbConfig.user,
		password: dbConfig.password,
		database: dbConfig.database
	});
	return connection;
}

function* getTables(dbConfig, connection) {
	// get all the tables for this specific database
	console.log("** : Getting all tables");
	const results = yield connection.query("SHOW TABLES;");
	return results;
}

function* getSchema(dbConfig, connection, tables) {
	// TODO: make dynamic for postgres too?
	const tableKey = `Tables_in_${dbConfig.database}`;
	const schemaObj = {};
	console.log("** : Getting schema for all tables");
	for (const table of tables) {
		schemaObj[table[tableKey]] = yield connection.query(`DESCRIBE \`${table[tableKey]}\``);
		console.log(`*  : Describing table ${table[tableKey]} (${schemaObj[table[tableKey]].length} columns found)`);
	}
	return schemaObj;
}

function compareTables(schemas) {
	console.log("** : Comparing tables to master");
	for (const table in schemas[0].schema) {
		// guard for in for linter
		if (table.hasOwnProperty.call(schemas[0].schema, table)) {
			compareTable(schemas, table);
		}
	}
}

function compareTable(schemas, table) {
	console.log(`*  : Comparing table '${table}' to master`);
	// the change handler will handle what to do once changes are found, reporting is default
	const changeHandler = {
		report: doReporting
	};
	const change = {};
	// some error checking
	if (schemas.length <= 1) {
		// there's not enough databases to do a comparison
		throw new Error("Not enough databases found in schemas array");
	}
	if (!schemas[0]) {
		// the first index doesn't exist, this is bad
		throw new Error("No master index found in schemas array");
	}
	if (!schemas[0].schema[table]) {
		// the table doesn't exist, this is bad too
		throw new Error(`No table match found for ${table} in master schema`);
	}
	change[schemas[0].id] = {};
	change[schemas[0].id].disposition = "master";
	change[schemas[0].id].schema = schemas[0].schema[table];
	const master = schemas[0];
	// cycle through all env schemas
	let i = 1;
	while (i < schemas.length) {
		if (schemas[i].schema[table]) {
			// there's a schema match
			if (_.isEqual(schemas[0].schema[table], schemas[i].schema[table])) {
				// it's equal
				change[schemas[i].id] = {
					table: table,
					disposition: "match",
					result: "No Action Needed",
					schema: schemas[i].schema[table]
				};
			} else {
				// it's not equal
				// find differences on a per table basis
				// TODO: sort the arrays!
				const differences = findDifferencesInArrays(schemas[0].schema[table], schemas[i].schema[table]);
				change[schemas[i].id] = {
					table: table,
					disposition: "no match",
					result: differences,
					schema: schemas[i].schema[table]
				};
			}
		} else {
			// no schema match for this key
			change[schemas[i].id] = {
				table: table,
				disposition: "missing",
				result: "Table Missing",
				schema: schemas[i].schema[table]
			};
		}
		i++;
	}
	// return the output
	changeHandler["report"](change);
	function doReporting(changeObj) {
		let message = "";
		for (const db of config.databases) {
			if (change[db.id].disposition === "no match") {
				// it doesn't match, let's find out why
				// console.log(`*  : ${change[db.id].result}`);
				for (const difference of change[db.id].result) {
					// this cycles through all the differences
					console.log(`** : Difference found in column '${difference.name}', property '${difference.property}', for ${db.id} database`);
					console.log(`** : Master Value: [${difference.master_value}] Peer Value: [${difference.peer_value}]`);
				}
			}
			message += `${db.id}: [${change[db.id].disposition}] `;
		}
		console.log(`*  : Reporting Change - ${message}`);
	}
}

function findDifferencesInArrays(masterArr, peerArr) {
	let differences = [];
	let i = 0;
	while (i < masterArr.length) {
		const newDiff = findDifferencesInObjects(masterArr[i], peerArr[i]);
		differences = _.concat(differences, newDiff);
		i++;
	}
	return differences;
}

function findDifferencesInObjects(masterObj, peerObj) {
	const differences = _.reduce(masterObj, function reduceArr(result, value, key) {
		return _.isEqual(value, peerObj[key]) ? result : result.concat(key);
	}, []);
	const returnDifferences = [];
	for (const difference of differences) {
		const diffObj = {
			name: masterObj.Field,
			property: difference,
			master_value: masterObj[difference],
			peer_value: peerObj[difference]
		};
		returnDifferences.push(diffObj);
	}
	return returnDifferences;
}
