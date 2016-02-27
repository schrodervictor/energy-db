'use strict';

var infoTableHashKey = {
  Table: {
    TableName: 'Table-HashKey',
    KeySchema: [
      { AttributeName: 'some-hash-key', KeyType: 'HASH' },
    ],
    AttributeDefinitions: [
      { AttributeName: 'some-hash-key', AttributeType: 'S' },
      { AttributeName: 'some-other-attr', AttributeType: 'N' },
      { AttributeName: 'some-global-index-attr', AttributeType: 'S' },
      { AttributeName: 'some-local-index-attr', AttributeType: 'N' },
    ]
  }
};

var infoTableHashKeyRangeKey = {
  Table: {
    TableName: 'Table-HashKey-RangeKey',
    KeySchema: [
      { AttributeName: 'some-hash-key', KeyType: 'HASH' },
      { AttributeName: 'some-range-key', KeyType: 'RANGE' }
    ],
    AttributeDefinitions: [
      { AttributeName: 'some-hash-key', AttributeType: 'S' },
      { AttributeName: 'some-range-key', AttributeType: 'S' },
      { AttributeName: 'some-other-attr', AttributeType: 'N' },
      { AttributeName: 'some-global-index-attr', AttributeType: 'S' },
      { AttributeName: 'some-local-index-attr', AttributeType: 'N' },
    ]
  }
};

var infoTableHashKeyRangeKeyGlobalIndexLocalIndex = {
  Table: {
    TableName: 'Table-HashKey-RangeKey-GlobalIndex-LocalIndex',
    KeySchema: [
      { AttributeName: 'some-hash-key', KeyType: 'HASH' },
      { AttributeName: 'some-range-key', KeyType: 'RANGE' }
    ],
    AttributeDefinitions: [
      { AttributeName: 'some-hash-key', AttributeType: 'S' },
      { AttributeName: 'some-range-key', AttributeType: 'S' },
      { AttributeName: 'some-other-attr', AttributeType: 'N' },
      { AttributeName: 'some-global-index-attr', AttributeType: 'S' },
      { AttributeName: 'some-local-index-attr', AttributeType: 'N' },
    ],
    GlobalSecondaryIndexes: [
      {
        IndexName: 'Global-Index-1',
        IndexStatus: 'ACTIVE',
        KeySchema: [
          { AttributeName: 'some-global-index-attr', KeyType: 'HASH' },
          { AttributeName: 'some-range-key', KeyType: 'RANGE' }
        ],
      }
    ],
    LocalSecondaryIndexes: [
      {
        IndexName: 'Local-Index-1',
        IndexStatus: 'ACTIVE',
        KeySchema: [
          { AttributeName: 'some-hash-key', KeyType: 'HASH' },
          { AttributeName: 'some-local-index-attr', KeyType: 'RANGE' }
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
    if (tableName in tableInfos) {
      return callback(null, tableInfos[tableName]);
    } else {
      return callback(new Error());
    }
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
  tableInfos: tableInfos,
  connectorMock: connectorMock,
  dbMock: dbMock
};
