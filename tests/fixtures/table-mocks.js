'use strict';

var infoTableHashKey = {
  Table: {
    TableName: 'Table-HashKey',
    KeySchema: [
      {AttributeName: 'some-hash-key', KeyType: 'HASH'},
    ],
    AttributeDefinitions: [
      {AttributeName: 'some-hash-key', AttributeType: 'S'},
    ]
  }
};

var infoTableHashKeyRangeKey = {
  Table: {
    TableName: 'Table-HashKey-RangeKey',
    KeySchema: [
      {AttributeName: 'some-hash-key', KeyType: 'HASH'},
      {AttributeName: 'some-range-key', KeyType: 'RANGE'}
    ],
    AttributeDefinitions: [
      {AttributeName: 'some-hash-key', AttributeType: 'S'},
      {AttributeName: 'some-range-key', AttributeType: 'S'},
    ]
  }
};

var infoTableHashKeyRangeKeyGlobalIndexLocalIndex = {
  Table: {
    TableName: 'Table-HashKey-RangeKey-GlobalIndex-LocalIndex',
    KeySchema: [
      {AttributeName: 'some-hash-key', KeyType: 'HASH'},
      {AttributeName: 'some-range-key', KeyType: 'RANGE'}
    ],
    AttributeDefinitions: [
      {AttributeName: 'some-hash-key', AttributeType: 'S'},
      {AttributeName: 'some-range-key', AttributeType: 'S'},
      {AttributeName: 'some-global-index-attr', AttributeType: 'S'},
      {AttributeName: 'some-local-index-attr', AttributeType: 'N'},
    ],
    GlobalSecondaryIndexes: [
      {
        IndexName: 'Global-Index-1',
        IndexStatus: 'ACTIVE',
        KeySchema: [
          {AttributeName: 'some-global-index-attr', KeyType: 'HASH'},
          {AttributeName: 'some-range-key', KeyType: 'RANGE'}
        ],
      }
    ],
    LocalSecondaryIndexes: [
      {
        IndexName: 'Local-Index-1',
        IndexStatus: 'ACTIVE',
        KeySchema: [
          {AttributeName: 'some-hash-key', KeyType: 'HASH'},
          {AttributeName: 'some-local-index-attr', KeyType: 'RANGE'}
        ],
      }
    ]
  }
};

var tableInfos = {
  'Table-HashKey': infoTableHashKey,
  'Table-HashKey-RangeKey': infoTableHashKeyRangeKey,
  'Table-HashKey-RangeKey-GlobalIndex-LocalIndex':
    infoTableHashKeyRangeKeyGlobalIndexLocalIndex,
};

var connectorMock = {
  describeTable: function(query, callback) {
    if (!('TableName' in query)) {
      return callback(new Error('Invalid query'));
    }
    if (query.TableName in tableInfos) {
      return callback(null, tableInfos[query.TableName]);
    } else {
      return callback(new Error('Invalid tableMock'));
    }
  },
  putItem: function(item, callback) {
    return callback();
  },
  query: function(item, callback) {
    return callback();
  },
  scan: function(item, callback) {
    return callback();
  },
  deleteItem: function(item, callback) {
    return callback();
  },
  updateItem: function(item, callback) {
    return callback();
  }
};

var dbMock = {
  describeTable: function(tableName, callback) {
    this.getConnector().describeTable({TableName: tableName}, callback);
  },
  getConnector: function() {
    return connectorMock;
  }
};

module.exports = {
  'Table-HashKey': infoTableHashKey,
  'Table-HashKey-RangeKey': infoTableHashKeyRangeKey,
  'Table-HashKey-RangeKey-GlobalIndex-LocalIndex':
    infoTableHashKeyRangeKeyGlobalIndexLocalIndex,
  connectorMock: connectorMock,
  dbMock: dbMock
};
