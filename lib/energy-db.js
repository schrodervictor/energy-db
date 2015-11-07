'use strict';

var DynamoDB = require('aws-sdk').DynamoDB;

var EnergyDB = function EnergyDB(settings, dynamoDB) {
    this.settings = settings || {};
    this.connector = dynamoDB;
};

EnergyDB.prototype.describeTable = function describeTable(tableName, callback) {
    this.connector.describeTable({TableName: tableName}, callback);
};

EnergyDB.prototype.getConnector = function getConnector() {
    return this.connector;
};

module.exports.EnergyDB = EnergyDB;

module.exports.connect = function(settings, callback) {
    var dynamoDB = new DynamoDB(settings)
    return callback(null, new EnergyDB(settings, dynamoDB));
};
