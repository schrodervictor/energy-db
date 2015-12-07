'use strict';

var dynamoDoc = require('dynamo-doc');
var simpleClone = require('./utils/clone').simpleClone;
var uuid = require('node-uuid');

var EnergyTable = function(db, tableName) {
    this.db = db;
    this.tableName = tableName;
    this.hashKey = null;
    this.hashKeyType = null;
    this.autoHashKey = true;
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

EnergyTable.prototype.putItem = function putItem(item, callback) {

    var self = this;

    if (this.autoHashKey && !(this.hashKey in item)) {
        item[this.hashKey] = uuid.v4();
    }

    dynamoDoc.jsToDynamo(item, function(err, dynamoItem) {
        if (err) return callback(err);
        self.putDynamoItem(dynamoItem, callback);
    });
};

EnergyTable.prototype.putDynamoItem = function putDynamoItem(dynamoItem, callback) {

    var self = this;

    this.validateDynamoItem(dynamoItem, function(err, validDynamoItem) {
        if (err) return callback(err);

        var query = simpleClone(self.queryBase);
        query['Item'] = validDynamoItem;
        self.db.getConnector().putItem(query, callback);
    });
};

EnergyTable.prototype.validateDynamoItem = function validateDynamoItem(dynamoItem, callback) {

    if (!(this.hashKey in dynamoItem)) {
        return callback(new Error('The HashKey [' + this.hashKey + '] is mandatory'));
    }

    if (!(this.hashKeyType in dynamoItem[this.hashKey])) {
        return callback(new Error('Invalid HashKeyType. Expected [' +
            this.hashKeyType + '], got [' + Object.keys(dynamoItem[this.hashKey])[0] + ']')
        );
    }

    if (this.rangeKey && !(this.rangeKey in dynamoItem)) {
        return callback(new Error('The RangeKey [' + this.rangeKey + '] is mandatory'));
    }

    if (this.rangeKey && !(this.rangeKeyType in dynamoItem[this.rangeKey])) {
        return callback(new Error('Invalid RangeKeyType. Expected [' +
            this.rangeKeyType + '], got [' + Object.keys(dynamoItem[this.rangeKey])[0] + ']')
        );
    }

    return callback(null, dynamoItem);
};

EnergyTable.prototype.query = function query(doc, callback) {
    var readQuery = simpleClone(this.queryBase);
    readQuery['ExpressionAttributeNames'] = {};
    readQuery['ExpressionAttributeValues'] = {};
    var expression = '';
    var i = 0;
    for (var key in doc) {
        var expressionName = '#k' + i;
        var expressionValue = ':v' + i;
        var glue = expression ? ' AND ' : ''
        expression = expression + glue + expressionName + " = " + expressionValue;
        readQuery['ExpressionAttributeNames'][expressionName] = key;
        readQuery['ExpressionAttributeValues'][expressionValue] = dynamoDoc.dynamoValue(doc[key]);
        i++;
    }

    var method = 'scan';
    if (this.hashKey in doc) {
        readQuery['KeyConditionExpression'] = expression;
        method = 'query';
    } else {
        readQuery['FilterExpression'] = expression;
        method = 'scan';
    }

    this.db.getConnector()[method](readQuery, function postQuery(err, data) {
        if (err) return callback(err);

        if (!data || !('Items' in data) || !Array.isArray(data.Items) || data.Items.length === 0) {
            return callback(null, []);
        }

        // TODO: make this asyncronous
        var results = data.Items.map(function (elem) {
            return dynamoDoc.dynamoToJs(elem);
        });

        return callback(null, results);
    });
};

EnergyTable.prototype.delete = function _delete(doc, callback) {

    if (!(this.hashKey in doc)) {
        return callback(
            new Error('HashKey is needed for delete operations')
        );
    }

    if (this.rangeKey && !(this.rangeKey in doc)) {
        return callback(
            new Error('RangeKey is needed for tables with compound key.')
        );
    }

    var deleteQuery = simpleClone(this.queryBase);

    deleteQuery['Key'] = {};
    deleteQuery['Key'][this.hashKey] = dynamoDoc.dynamoValue(doc[this.hashKey]);

    if (this.rangeKey) {
        deleteQuery['Key'][this.rangeKey] = dynamoDoc.dynamoValue(doc[this.rangeKey]);
    }

    deleteQuery['ExpressionAttributeNames'] = {};
    deleteQuery['ExpressionAttributeValues'] = {};
    var expression = '';
    var i = 0;
    for (var key in doc) {
        var expressionName = '#k' + i;
        var expressionValue = ':v' + i;
        var glue = expression ? ' AND ' : ''
        expression = expression + glue + expressionName + " = " + expressionValue;
        deleteQuery['ExpressionAttributeNames'][expressionName] = key;
        deleteQuery['ExpressionAttributeValues'][expressionValue] = dynamoDoc.dynamoValue(doc[key]);
        i++;
    }

    deleteQuery['ConditionExpression'] = expression;

    this.db.getConnector().deleteItem(deleteQuery, function postQuery(err, data) {
        if (err) return callback(err);

        if (!data || !('Attributes' in data)) {
            return callback(null, {});
        }

        // TODO: make this asyncronous
        var result = dynamoDoc.dynamoToJs(data.Attributes);

        return callback(null, result);
    });
};

module.exports.EnergyTable = EnergyTable;

module.exports.getInstance = function(db, tableName, callback) {
    var table = new EnergyTable(db, tableName);
    return table.init(callback);
};
