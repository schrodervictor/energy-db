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

var tableInfos = {
    'Table-HashKey': infoTableHashKey,
    'Table-HashKey-RangeKey': infoTableHashKeyRangeKey,
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
    tableInfos: tableInfos,
    connectorMock: connectorMock,
    dbMock: dbMock
};
