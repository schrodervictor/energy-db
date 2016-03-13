'use strict';

var chai = require('chai');
var sinon = require('sinon');
var sinonChai = require('sinon-chai');
var expect = chai.expect;
chai.use(sinonChai);

var energyQuery = require('../../lib/energy-query-factory');
var EnergyTable = require('../../lib/energy-table').EnergyTable;
var mocks = require('../fixtures/table-mocks');

describe('EnergyQueryFactory (class)', function() {

  var EnergyQueryFactory = energyQuery.EnergyQueryFactory;

  var table;
  var tableHashAndRange;

  beforeEach(function(done) {
    table = new EnergyTable(mocks.dbMock, 'Table-HashKey');
    table.init(done);
  });

  beforeEach(function(done) {
    tableHashAndRange = new EnergyTable(mocks.dbMock, 'Table-HashKey-RangeKey');
    tableHashAndRange.init(done);
  });

  describe('#getQuery()', function() {

    it('should return the correct query for "insert" operations', function(done) {
      var item = {
        'key-0': 'value-0',
        'key-1': 'value-1',
        'key-2': 12345
      };

      var expectedQuery = {
        TableName: 'Table-HashKey',
        Item: {
          'key-0': { S: 'value-0' },
          'key-1': { S: 'value-1' },
          'key-2': { N: '12345' }
        }
      };

      var instance = new EnergyQueryFactory(table);

      instance.getInsertQuery(item, null, function(err, query) {
        if (err) return done(err);
        expect(query).to.deep.equals(expectedQuery);
        done();
      });

    });

    it('should not contain options for other types of query (for insert queries)', function(done) {
      var item = {
        'key-0': 'value-0',
        'key-1': 'value-1',
        'key-2': 12345
      };

      var allQueryParams = {
        'Item': {'key': {S: 'value'}},
        'TableName': 'STRING',
        'ConditionExpression': 'STRING',
        'ExpressionAttributeNames': {'key': 'value'},
        'ExpressionAttributeValues': {'key': {S: 'value'}},
        'ReturnConsumedCapacity': 'TOTAL',
        'ReturnItemCollectionMetrics': 'SIZE',
        'ReturnValues': 'ALL_NEW',
        'ConsistentRead': true,
        'ExclusiveStartKey': {'key': {S: 'value'}},
        'FilterExpression': 'STRING',
        'IndexName': 'STRING',
        'KeyConditionExpression': 'STRING',
        'Limit': 100,
        'ProjectionExpression': 'STRING',
        'ScanIndexForward': true,
        'Select': 'ALL_ATTRIBUTES',
        'Segment': 2,
        'TotalSegments': 4,
        'Key': {'key': {S: 'value'}},
        'UpdateExpression': 'STRING',

        // Deprecated operators
        'Expected': {},
        'ConditionalOperator': 'AND',
        'AttributesToGet': {},
        'ScanFilter': {},
        'AttributeUpdates': {},
        'KeyConditions': {},
        'QueryFilter': {},
      };

      table.addQueryParams(allQueryParams);

      var instance = new EnergyQueryFactory(table);

      instance.getInsertQuery(item, null, function(err, query) {
        if (err) return done(err);

        var allowedParams = new Set([
          'Item',
          'TableName',
          'ConditionExpression',
          'ExpressionAttributeNames',
          'ExpressionAttributeValues',
          'ReturnConsumedCapacity',
          'ReturnItemCollectionMetrics',
          'ReturnValues',
        ]);

        for (var key in allQueryParams) {
          if (!allowedParams.has(key)) {
            expect(query).to.not.have.property(key);
          }
        }

        done();
      });

    });

    it('should return the correct query for "query" operations', function(done) {
      var doc = {
        'key-0': 'value-0',
        'key-1': {'$gte': 12345},
        'key-2': {
          'sub-key': 'random-value'
        }
      };

      var expectedQuery = {
        TableName: 'Table-HashKey',
        ExpressionAttributeNames: {
          '#k0': 'key-0',
          '#k1': 'key-1',
          '#k2': 'key-2'
        },
        ExpressionAttributeValues: {
          ':v0': {S: 'value-0'},
          ':v1': {N: '12345'},
          ':v2': {M: {
            'sub-key': {S: 'random-value'}
          }}
        },
        KeyConditionExpression: '#k0 = :v0 AND #k1 >= :v1 AND #k2 = :v2'
      };

      var instance = new EnergyQueryFactory(table);

      instance.getQuery(doc, null, function(err, query) {
        if (err) return done(err);
        expect(query).to.deep.equals(expectedQuery);
        done();
      });

    });

    it('should not contain options for other types of query (for query queries)', function(done) {
      var doc = {
        'key-0': 'value-0',
        'key-1': {'$gte': 12345},
        'key-2': {
          'sub-key': 'random-value'
        }
      };

      var allQueryParams = {
        'Item': {'key': {S: 'value'}},
        'TableName': 'STRING',
        'ConditionExpression': 'STRING',
        'ExpressionAttributeNames': {'key': 'value'},
        'ExpressionAttributeValues': {'key': {S: 'value'}},
        'ReturnConsumedCapacity': 'TOTAL',
        'ReturnItemCollectionMetrics': 'SIZE',
        'ReturnValues': 'ALL_NEW',
        'ConsistentRead': true,
        'ExclusiveStartKey': {'key': {S: 'value'}},
        'FilterExpression': 'STRING',
        'IndexName': 'STRING',
        'KeyConditionExpression': 'STRING',
        'Limit': 100,
        'ProjectionExpression': 'STRING',
        'ScanIndexForward': true,
        'Select': 'ALL_ATTRIBUTES',
        'Segment': 2,
        'TotalSegments': 4,
        'Key': {'key': {S: 'value'}},
        'UpdateExpression': 'STRING',

        // Deprecated operators
        'Expected': {},
        'ConditionalOperator': 'AND',
        'AttributesToGet': {},
        'ScanFilter': {},
        'AttributeUpdates': {},
        'KeyConditions': {},
        'QueryFilter': {},
      };

      table.addQueryParams(allQueryParams);

      var instance = new EnergyQueryFactory(table);

      instance.getQuery(doc, null, function(err, query) {
        if (err) return done(err);

        var allowedParams = new Set([
          'TableName',
          'ConsistentRead',
          'ExclusiveStartKey',
          'ExpressionAttributeNames',
          'ExpressionAttributeValues',
          'FilterExpression',
          'IndexName',
          'KeyConditionExpression',
          'Limit',
          'ProjectionExpression',
          'ReturnConsumedCapacity',
          'ScanIndexForward',
          'Select',
        ]);

        for (var key in allQueryParams) {
          if (!allowedParams.has(key)) {
            expect(query).to.not.have.property(key);
          }
        }

        done();
      });

    });

    it('should return the correct query for "scan" operations', function(done) {
      var doc = {
        'key-0': 'value-0',
        'key-1': {'$gte': 12345},
        'key-2': {
          'sub-key': 'random-value'
        }
      };

      var expectedQuery = {
        TableName: 'Table-HashKey',
        ExpressionAttributeNames: {
          '#k0': 'key-0',
          '#k1': 'key-1',
          '#k2': 'key-2'
        },
        ExpressionAttributeValues: {
          ':v0': {S: 'value-0'},
          ':v1': {N: '12345'},
          ':v2': {M: {
            'sub-key': {S: 'random-value'}
          }}
        },
        FilterExpression: '#k0 = :v0 AND #k1 >= :v1 AND #k2 = :v2'
      };

      var instance = new EnergyQueryFactory(table);

      instance.getScanQuery(doc, null, function(err, query) {
        if (err) return done(err);
        expect(query).to.deep.equals(expectedQuery);
        done();
      });

    });

    it('should return the correct query for full "scan" operations', function(done) {
      var doc = {};

      var expectedQuery = {
        TableName: 'Table-HashKey',
      };

      var instance = new EnergyQueryFactory(table);

      instance.getScanQuery(doc, null, function(err, query) {
        if (err) return done(err);
        expect(query).to.deep.equals(expectedQuery);
        done();
      });

    });

    it('should not contain options for other types of query (for scan queries)', function(done) {
      var doc = {
        'key-0': 'value-0',
        'key-1': {'$gte': 12345},
        'key-2': {
          'sub-key': 'random-value'
        }
      };

      var allQueryParams = {
        'Item': {'key': {S: 'value'}},
        'TableName': 'STRING',
        'ConditionExpression': 'STRING',
        'ExpressionAttributeNames': {'key': 'value'},
        'ExpressionAttributeValues': {'key': {S: 'value'}},
        'ReturnConsumedCapacity': 'TOTAL',
        'ReturnItemCollectionMetrics': 'SIZE',
        'ReturnValues': 'ALL_NEW',
        'ConsistentRead': true,
        'ExclusiveStartKey': {'key': {S: 'value'}},
        'FilterExpression': 'STRING',
        'IndexName': 'STRING',
        'KeyConditionExpression': 'STRING',
        'Limit': 100,
        'ProjectionExpression': 'STRING',
        'ScanIndexForward': true,
        'Select': 'ALL_ATTRIBUTES',
        'Segment': 2,
        'TotalSegments': 4,
        'Key': {'key': {S: 'value'}},
        'UpdateExpression': 'STRING',

        // Deprecated operators
        'Expected': {},
        'ConditionalOperator': 'AND',
        'AttributesToGet': {},
        'ScanFilter': {},
        'AttributeUpdates': {},
        'KeyConditions': {},
        'QueryFilter': {},
      };

      table.addQueryParams(allQueryParams);

      var instance = new EnergyQueryFactory(table);

      instance.getScanQuery(doc, null, function(err, query) {
        if (err) return done(err);

        var allowedParams = new Set([
          'TableName',
          'ConsistentRead',
          'ExclusiveStartKey',
          'ExpressionAttributeNames',
          'ExpressionAttributeValues',
          'FilterExpression',
          'IndexName',
          'Limit',
          'ProjectionExpression',
          'Segment',
          'TotalSegments',
          'ReturnConsumedCapacity',
          'Select',
        ]);

        for (var key in allQueryParams) {
          if (!allowedParams.has(key)) {
            expect(query).to.not.have.property(key);
          }
        }

        done();
      });

    });

    it('should return the correct query for "delete" operations', function(done) {
      var doc = {
        'some-hash-key': 'value-0',
        'some-range-key': 'value-1',
        'key-2': {'$gte': 12345},
        'key-3': {
          'sub-key': 'random-value'
        }
      };

      var expectedQuery = {
        TableName: 'Table-HashKey-RangeKey',
        Key: {
          'some-hash-key': {S: 'value-0'},
          'some-range-key': {S: 'value-1'},
        },
        ExpressionAttributeNames: {
          '#k0': 'some-hash-key',
          '#k1': 'some-range-key',
          '#k2': 'key-2',
          '#k3': 'key-3'
        },
        ExpressionAttributeValues: {
          ':v0': {S: 'value-0'},
          ':v1': {S: 'value-1'},
          ':v2': {N: '12345'},
          ':v3': {M: {
            'sub-key': {S: 'random-value'}
          }}
        },
        ConditionExpression:
          '#k0 = :v0 AND #k1 = :v1 AND #k2 >= :v2 AND #k3 = :v3'
      };

      var instance = new EnergyQueryFactory(tableHashAndRange);

      instance.getDeleteQuery(doc, null, function(err, query) {
        if (err) return done(err);
        expect(query).to.deep.equals(expectedQuery);
        done();
      });

    });

    it('should not contain options for other types of query (for delete queries)', function(done) {
      var doc = {
        'some-hash-key': 'value-0',
        'some-range-key': 'value-1',
        'key-2': {'$gte': 12345},
        'key-3': {
          'sub-key': 'random-value'
        }
      };

      var allQueryParams = {
        'Item': {'key': {S: 'value'}},
        'TableName': 'STRING',
        'ConditionExpression': 'STRING',
        'ExpressionAttributeNames': {'key': 'value'},
        'ExpressionAttributeValues': {'key': {S: 'value'}},
        'ReturnConsumedCapacity': 'TOTAL',
        'ReturnItemCollectionMetrics': 'SIZE',
        'ReturnValues': 'ALL_NEW',
        'ConsistentRead': true,
        'ExclusiveStartKey': {'key': {S: 'value'}},
        'FilterExpression': 'STRING',
        'IndexName': 'STRING',
        'KeyConditionExpression': 'STRING',
        'Limit': 100,
        'ProjectionExpression': 'STRING',
        'ScanIndexForward': true,
        'Select': 'ALL_ATTRIBUTES',
        'Segment': 2,
        'TotalSegments': 4,
        'Key': {'key': {S: 'value'}},
        'UpdateExpression': 'STRING',

        // Deprecated operators
        'Expected': {},
        'ConditionalOperator': 'AND',
        'AttributesToGet': {},
        'ScanFilter': {},
        'AttributeUpdates': {},
        'KeyConditions': {},
        'QueryFilter': {},
      };

      table.addQueryParams(allQueryParams);

      var instance = new EnergyQueryFactory(table);

      instance.getDeleteQuery(doc, null, function(err, query) {
        if (err) return done(err);

        var allowedParams = new Set([
          'Key',
          'TableName',
          'ConditionExpression',
          'ExpressionAttributeNames',
          'ExpressionAttributeValues',
          'ReturnConsumedCapacity',
          'ReturnItemCollectionMetrics',
          'ReturnValues',
        ]);

        for (var key in allQueryParams) {
          if (!allowedParams.has(key)) {
            expect(query).to.not.have.property(key);
          }
        }

        done();
      });

    });


    it('should return the correct query for "update" operations', function(done) {
      var doc = {
        'some-hash-key': 'value-0',
        'some-range-key': 'value-1',
        'key-0': 'value-2',
        'key-1': 98765,
        'key-2': 12345,
      };

      var update = {
        '$set': {
          'key-0': 'value-0-new',
          'key-1': 11111,
        },
        '$inc': {
          'key-2': 3
        },
        '$unset': {
          'key-3': 1
        },
      };

      var expectedQuery = {
        TableName: 'Table-HashKey-RangeKey',
        Key: {
          'some-hash-key': {S: 'value-0'},
          'some-range-key': {S: 'value-1'},
        },
        ExpressionAttributeNames: {
          '#k0': 'some-hash-key',
          '#k1': 'some-range-key',
          '#k2': 'key-0',
          '#k3': 'key-1',
          '#k4': 'key-2',
          '#k5': 'key-0',
          '#k6': 'key-1',
          '#k7': 'key-2',
          '#k8': 'key-3',
        },
        ExpressionAttributeValues: {
          ':v0': {S: 'value-0'},
          ':v1': {S: 'value-1'},
          ':v2': {S: 'value-2'},
          ':v3': {N: '98765'},
          ':v4': {N: '12345'},
          ':v5': {S: 'value-0-new'},
          ':v6': {N: '11111'},
          ':v7': {N: '3'},
        },
        ConditionExpression:
          '#k0 = :v0 AND #k1 = :v1 AND #k2 = :v2 AND #k3 = :v3 AND #k4 = :v4',
        UpdateExpression:
          'SET #k5 = :v5, #k6 = :v6 ADD #k7 :v7 REMOVE #k8'
      };

      var instance = new EnergyQueryFactory(tableHashAndRange);

      instance.getUpdateQuery(doc, update, null, function(err, query) {
        if (err) return done(err);
        expect(query).to.deep.equals(expectedQuery);
        done();
      });

    });

    it('should not contain options for other types of query (for update queries)', function(done) {
      var doc = {
        'some-hash-key': 'value-0',
        'some-range-key': 'value-1',
        'key-0': 'value-2',
        'key-1': 98765,
        'key-2': 12345,
      };

      var update = {
        '$set': {
          'key-0': 'value-0-new',
          'key-1': 11111,
        },
        '$inc': {
          'key-2': 3
        },
        '$unset': {
          'key-3': 1
        },
      };

      var allQueryParams = {
        'Item': {'key': {S: 'value'}},
        'TableName': 'STRING',
        'ConditionExpression': 'STRING',
        'ExpressionAttributeNames': {'key': 'value'},
        'ExpressionAttributeValues': {'key': {S: 'value'}},
        'ReturnConsumedCapacity': 'TOTAL',
        'ReturnItemCollectionMetrics': 'SIZE',
        'ReturnValues': 'ALL_NEW',
        'ConsistentRead': true,
        'ExclusiveStartKey': {'key': {S: 'value'}},
        'FilterExpression': 'STRING',
        'IndexName': 'STRING',
        'KeyConditionExpression': 'STRING',
        'Limit': 100,
        'ProjectionExpression': 'STRING',
        'ScanIndexForward': true,
        'Select': 'ALL_ATTRIBUTES',
        'Segment': 2,
        'TotalSegments': 4,
        'Key': {'key': {S: 'value'}},
        'UpdateExpression': 'STRING',

        // Deprecated operators
        'Expected': {},
        'ConditionalOperator': 'AND',
        'AttributesToGet': {},
        'ScanFilter': {},
        'AttributeUpdates': {},
        'KeyConditions': {},
        'QueryFilter': {},
      };

      table.addQueryParams(allQueryParams);

      var instance = new EnergyQueryFactory(table);

      instance.getUpdateQuery(doc, update, null, function(err, query) {
        if (err) return done(err);

        var allowedParams = new Set([
          'Key',
          'TableName',
          'ConditionExpression',
          'ExpressionAttributeNames',
          'ExpressionAttributeValues',
          'ReturnConsumedCapacity',
          'ReturnItemCollectionMetrics',
          'ReturnValues',
          'UpdateExpression',
        ]);

        for (var key in allQueryParams) {
          if (!allowedParams.has(key)) {
            expect(query).to.not.have.property(key);
          }
        }

        done();
      });

    });

    it('should return the correct query for "replace" operations', function(done) {
      var doc = {
        'key-0': 'value-0',
        'key-1': 98765,
        'key-2': {'$gt': 12345},
      };

      var replacement = {
        'key-0': 'value-0-new',
        'key-1': 55555,
        'key-2': 99999,
      };

      var expectedQuery = {
        TableName: 'Table-HashKey',
        Item: {
          'key-0': {S: 'value-0-new'},
          'key-1': {N: '55555'},
          'key-2': {N: '99999'},
        },
        ExpressionAttributeNames: {
          '#k0': 'key-0',
          '#k1': 'key-1',
          '#k2': 'key-2',
        },
        ExpressionAttributeValues: {
          ':v0': {S: 'value-0'},
          ':v1': {N: '98765'},
          ':v2': {N: '12345'},
        },
        ConditionExpression:
          '#k0 = :v0 AND #k1 = :v1 AND #k2 > :v2',
      };

      var instance = new EnergyQueryFactory(table);

      instance.getReplaceQuery(doc, replacement, null, function(err, query) {
        if (err) return done(err);
        expect(query).to.deep.equals(expectedQuery);
        done();
      });

    });
  });
});
