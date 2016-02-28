'use strict';

var dynamoDoc = require('dynamo-doc');
var simpleClone = require('./utils/clone').simpleClone;
var uuid = require('node-uuid');
var EnergyQuery = require('./energy-query').EnergyQuery;

var EnergyTable = function(db, tableName) {
  this.db = db;
  this.tableName = tableName;
  this.hashKey = null;
  this.hashKeyType = null;
  this.autoHashKey = true;
  this.rangeKey = null;
  this.rangeKeyType = null;
  this.queryBase = {};
  this.indexes = [];
};

EnergyTable.prototype.init = function init(callback) {

  var self = this;

  this.db.describeTable(this.tableName, function(err, data) {
    if (err) return callback(err);
    self.setKeySchema(data).setIndexes(data).initQueryBase();
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

EnergyTable.prototype.setIndexes = function setIndexes(tableInfo) {

  var self = this;
  var attributesInfo = tableInfo.Table.AttributeDefinitions;

  if ('GlobalSecondaryIndexes' in tableInfo.Table) {

    var globalIndexes = tableInfo.Table.GlobalSecondaryIndexes;

    globalIndexes.forEach(function(indexInfo) {
        var indexRegistry = self.getIndexKeySchema(indexInfo, attributesInfo);
        self.indexes.push(indexRegistry);
    });
  }

  if ('LocalSecondaryIndexes' in tableInfo.Table) {

    var localIndexes = tableInfo.Table.LocalSecondaryIndexes;

    localIndexes.forEach(function(indexInfo) {
        var indexRegistry = self.getIndexKeySchema(indexInfo, attributesInfo);
        self.indexes.push(indexRegistry);
    });
  }

  this.indexesHashKeys = this.indexes.map(function(index) {
    return index.hashKey;
  });

  return this;
};

EnergyTable.prototype.getIndexKeySchema = function getIndexKeySchema(indexInfo, attributesInfo) {

  var indexName = indexInfo.IndexName;
  var indexRegistry = {
    name: indexName,
    hashKey: null,
    hashKeyType: null,
    rangeKey: null,
    rangeKeyType: null
  };

  indexInfo.KeySchema.forEach(function(elem) {
    if (elem.KeyType === 'HASH') {
      indexRegistry['hashKey'] = elem.AttributeName;
    }
    if (elem.KeyType === 'RANGE') {
      indexRegistry['rangeKey'] = elem.AttributeName;
    }
  });

  attributesInfo.forEach(function(elem) {
    if (elem.AttributeName === indexRegistry['hashKey']) {
      indexRegistry['hashKeyType'] = elem.AttributeType;
    }
    if (elem.AttributeName === indexRegistry['rangeKey']) {
      indexRegistry['rangeKeyType'] = elem.AttributeType;
    }
  });

  return indexRegistry;
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

  var energyQuery = new EnergyQuery(
    'insert',
    this.queryBase,
    item,
    this.hashKey,
    this.rangeKey
  );

  var self = this;

  energyQuery.getQuery(function(err, query) {
    if (err) return callback(err);
    self.db.getConnector().putItem(query, callback);
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

  var method = 'scan';

  if (this.hashKey in doc || this.queryHasAnIndexHashKey(doc)) {
    method = 'query';
  } else {
    method = 'scan';
  }

  var energyQuery = new EnergyQuery(method, this.queryBase, doc);

  var self = this;

  energyQuery.getQuery(function(err, query) {

    if (self.queryHasAnIndexHashKey(doc)) {
      query['IndexName'] = self.findSuitableIndex(doc);
    }

    self.db.getConnector()[method](query, function postQuery(err, data) {
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

  var energyQuery = new EnergyQuery(
    'delete',
    this.queryBase,
    doc,
    this.hashKey,
    this.rangeKey
  );

  var self = this;

  energyQuery.getQuery(function(err, query) {
    if (err) return callback(err);

    self.db.getConnector().deleteItem(query, function postQuery(err, data) {
      if (err) return callback(err);
      return callback(null, data);
    });

  });

};

EnergyTable.prototype.queryHasAnIndexHashKey = function queryHasAnIndexHashKey(query) {
  return this.indexes.some(function(index) {
    return index.hashKey in query;
  });
};

EnergyTable.prototype.findSuitableIndex = function findSuitableIndex(query) {
  var suitableIndexes = this.indexes.filter(function(index) {
    return index.hashKey in query;
  });

  if (suitableIndexes.length > 0 && 'name' in suitableIndexes[0]) {
    return suitableIndexes[0].name;
  } else {
    return null;
  }
};

EnergyTable.prototype.update = function update(doc, updates, callback) {

  var energyQuery = new EnergyQuery(
    'update',
    this.queryBase,
    doc,
    this.hashKey,
    this.rangeKey
  );

  var self = this;

  energyQuery.getUpdateQuery(updates, function(err, query) {
    if (err) return callback(err);

    self.db.getConnector().updateItem(query, function postQuery(err, data) {
      if (err) return callback(err);
      return callback(null, data);
    });

  });

};

function getInstance(db, tableName, callback) {
  var table = new EnergyTable(db, tableName);
  return table.init(callback);
}

module.exports = {
  EnergyTable: EnergyTable,
  getInstance: getInstance
};
