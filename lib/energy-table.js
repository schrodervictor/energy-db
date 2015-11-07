'use strict';

var EnergyTable = function(db, tableName) {
    this.db = db;
    this.tableName = tableName;
};

module.exports.EnergyTable = EnergyTable;

module.exports.getInstance = function(db, tableName, callback) {
    return callback(null, new EnergyTable(db, tableName));
};
