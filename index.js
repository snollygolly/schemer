"use strict";

const config = require("./config.json");

const mysql = require("promise-mysql");
const co = require("co");

console.log("***: Staring up Schemer");

co(function* coWrap() {
	const schemas = {};
	// loop through every config env in the config
	for (const db of config.databases) {
		// set up the sub-object for this env schema
		const connection = yield getConnection(db);
		const tables = yield getTables(db, connection);
		schemas[db.id] = yield getSchema(db, connection, tables);
		console.log("***: Shutting down Schemer");
	}
	// we've gotten all the schema for all the tables for all envs.
	for (const schema in schemas) {
		// guard for in for linter
		if (schema.hasOwnProperty.call(schemas, schema)) {
			// this is a list of all envs
			console.log("schema", schema);
		}
	}
	return process.exit();
}).catch((err) => {
	console.error(err.stack);
	console.log("***: Dying...");
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

function compareColumn(schemas, key) {
	
}
