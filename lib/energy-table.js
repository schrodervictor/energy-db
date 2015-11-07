'use strict';

var EnergyTable = function(db, tableName) {
    this.db = db;
    this.tableName = tableName;
    this.hashKey = null;
    this.hashKeyType = null;
    this.rangeKey = null;
    this.rangeKeyType = null;
    this.queryBase = {};
};

EnergyTable.prototype.init = function init(callback) {

    var self = this;

    this.db.describeTable(this.tableName, function(err, tableInfo) {
        if (err) return callback(err);
        self.setKeySchema(tableInfo).initQueryBase();
        return callback(null, self);
    });
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

EnergyTable.prototype.initQueryBase = function initQueryBase() {
    this.queryBase['TableName'] = this.tableName
    return this;
};

EnergyTable.prototype.addQueryParam = function addQueryParam(key, value) {
    this.queryBase[key] = value;
    return this;
};

EnergyTable.prototype.addQueryParams = function addQueryParams(params) {
    for (var param in params) {
        this.queryBase[param] = params[param];
    }
    return this;
};

EnergyTable.prototype.returnConsumedCapacity = function returnConsumedCapacity() {
    return this.addQueryParam('ReturnConsumedCapacity', 'TOTAL');
};

EnergyTable.prototype.returnOldValues = function returnOldValues() {
    return this.addQueryParam('ReturnValues', 'ALL_OLD');
};

module.exports.EnergyTable = EnergyTable;

module.exports.getInstance = function(db, tableName, callback) {
    var table = new EnergyTable(db, tableName);
    return table.init(callback);
};
