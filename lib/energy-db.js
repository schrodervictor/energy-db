'use strict';

var EnergyDB = function EnergyDB(settings) {
    this.settings = settings || {};
};

module.exports.EnergyDB = EnergyDB;

module.exports.connect = function(settings, callback) {
    return callback(null, new EnergyDB(settings));
};
