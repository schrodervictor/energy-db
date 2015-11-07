'use strict';

var EnergyTable = function(db, tableName) {
    this.db = db;
    this.tableName = tableName;
    this.hashKey = null;
    this.hashKeyType = null;
    this.rangeKey = null;
    this.rangeKeyType = null;
};

EnergyTable.prototype.setKeySchema = function setKeySchema(tableInfo) {

    var self = this;

    tableInfo.Table.KeySchema.forEach(function(elem) {
        if (elem.KeyType === 'HASH') {
            self.hashKey = elem.AttributeName;
        }
        if (elem.KeyType === 'RANGE') {
            self.rangeKey = elem.AttributeName;
        }
    });

    tableInfo.Table.AttributeDefinitions.forEach(function(elem) {
        if (elem.AttributeName === self.hashKey) {
            self.hashKeyType = elem.AttributeType;
        }
        if (elem.AttributeName === self.rangeKey) {
            self.rangeKeyType = elem.AttributeType;
        }
    });

    return this;
};

module.exports.EnergyTable = EnergyTable;

module.exports.getInstance = function(db, tableName, callback) {
    return callback(null, new EnergyTable(db, tableName));
};
