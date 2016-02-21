'use strict';

var DynamoDB = require('aws-sdk').DynamoDB;
var energyTable = require('./energy-table');

var EnergyDB = function EnergyDB(settings, dynamoDB) {
  this.settings = settings || {};
  this.connector = dynamoDB;
};

EnergyDB.prototype.table = function table(tableName, callback) {
  return energyTable.getInstance(this, tableName, callback);
};

EnergyDB.prototype.describeTable = function describeTable(tableName, callback) {
  this.connector.describeTable({TableName: tableName}, callback);
};

EnergyDB.prototype.getConnector = function getConnector() {
  return this.connector;
};

module.exports.EnergyDB = EnergyDB;

module.exports.connect = function(settings, dynamoDB, callback) {
  if (!callback && typeof dynamoDB === 'function') {
    callback = dynamoDB;
    dynamoDB = null;
  }

  if (!dynamoDB) {
    var dynamoDB = new DynamoDB(settings)
  }

  return callback(null, new EnergyDB(settings, dynamoDB));
};
