'use strict';

var DynamoDB = require('aws-sdk').DynamoDB;

var EnergyDB = function EnergyDB(settings, dynamoDB) {
    this.settings = settings || {};
    this.connector = dynamoDB;
};

module.exports.EnergyDB = EnergyDB;

module.exports.connect = function(settings, callback) {
    var dynamoDB = new DynamoDB(settings)
    return callback(null, new EnergyDB(settings, dynamoDB));
};
