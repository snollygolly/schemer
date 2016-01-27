"use strict";

const config = require("./config.json");

const mysql = require("promise-mysql");
const co = require("co");

console.log("***: Staring up Schemer");

co(function* coWrap() {
	for (const db of config.databases) {
		const connection = yield getConnection(db);
		const tables = yield getTables(db, connection);
		const schema = yield getSchema(db, connection, tables);
		console.log("***: Shutting down Schemer");
		return process.exit();
	}
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
		const result = yield connection.query(`DESCRIBE \`${table[tableKey]}\``);
		console.log(`*  : Describing table ${table[tableKey]} (${result.length} columns found)`);
		schemaObj[table] = result;
	}
	return schemaObj;
}
