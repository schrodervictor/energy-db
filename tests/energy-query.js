'use strict';

var chai = require('chai');
var sinon = require('sinon');
var sinonChai = require('sinon-chai');
var expect = chai.expect;
chai.use(sinonChai);

var energyQuery = require('../lib/energy-query');

describe('EnergyQuery (class)', function() {

  var EnergyQuery = require('../lib/energy-query').EnergyQuery;

  describe('#getQuery()', function() {

    it('should return the correct query for "query" operations', function() {
      var baseQuery = {
        TableName: 'Name-Of-The-Table'
      };

      var queryDoc = {
        'key-0': 'value-0',
        'key-1': {'$gte': 12345},
        'key-2': {
          'sub-key': 'random-value'
        }
      };

      var expectedQuery = {
        TableName: 'Name-Of-The-Table',
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

      var instance = new EnergyQuery('query', baseQuery, queryDoc);

      expect(instance.getQuery()).to.deep.equals(expectedQuery);
    });

    it('should return the correct query for "scan" operations', function() {
      var baseQuery = {
        TableName: 'Name-Of-The-Table'
      };

      var queryDoc = {
        'key-0': 'value-0',
        'key-1': {'$gte': 12345},
        'key-2': {
          'sub-key': 'random-value'
        }
      };

      var expectedQuery = {
        TableName: 'Name-Of-The-Table',
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

      var instance = new EnergyQuery('scan', baseQuery, queryDoc);

      expect(instance.getQuery()).to.deep.equals(expectedQuery);
    });

    it('should return the correct query for "delete" operations', function() {
      var baseQuery = {
        TableName: 'Name-Of-The-Table'
      };

      var queryDoc = {
        'key-0': 'value-0',
        'key-1': 98765,
        'key-2': {'$gte': 12345},
        'key-3': {
          'sub-key': 'random-value'
        }
      };

      var expectedQuery = {
        TableName: 'Name-Of-The-Table',
        Key: {
          'key-0': {S: 'value-0'},
          'key-1': {N: '98765'}
        },
        ExpressionAttributeNames: {
          '#k0': 'key-0',
          '#k1': 'key-1',
          '#k2': 'key-2',
          '#k3': 'key-3'
        },
        ExpressionAttributeValues: {
          ':v0': {S: 'value-0'},
          ':v1': {N: '98765'},
          ':v2': {N: '12345'},
          ':v3': {M: {
            'sub-key': {S: 'random-value'}
          }}
        },
        ConditionExpression:
          '#k0 = :v0 AND #k1 = :v1 AND #k2 >= :v2 AND #k3 = :v3'
      };

      var instance = new EnergyQuery(
        'delete',
        baseQuery,
        queryDoc,
        'key-0',
        'key-1'
      );

      expect(instance.getQuery()).to.deep.equals(expectedQuery);
    });

    it('should return the correct query for "update" operations', function() {
      var baseQuery = {
        TableName: 'Name-Of-The-Table'
      };

      var queryDoc = {
        'key-0': 'value-0',
        'key-1': 98765,
        'key-2': 12345,
      };

      var updateDoc = {
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
        TableName: 'Name-Of-The-Table',
        Key: {
          'key-0': {S: 'value-0'},
          'key-1': {N: '98765'}
        },
        ExpressionAttributeNames: {
          '#k0': 'key-0',
          '#k1': 'key-1',
          '#k2': 'key-2',
          '#k3': 'key-0',
          '#k4': 'key-1',
          '#k5': 'key-2',
          '#k6': 'key-3',
        },
        ExpressionAttributeValues: {
          ':v0': {S: 'value-0'},
          ':v1': {N: '98765'},
          ':v2': {N: '12345'},
          ':v3': {S: 'value-0-new'},
          ':v4': {N: '11111'},
          ':v5': {N: '3'},
        },
        ConditionExpression:
          '#k0 = :v0 AND #k1 = :v1 AND #k2 = :v2',
        UpdateExpression:
          'SET #k3 = :v3, #k4 = :v4 ADD #k5 :v5 REMOVE #k6'
      };

      var instance = new EnergyQuery(
        'update',
        baseQuery,
        queryDoc,
        'key-0',
        'key-1'
      );

      expect(instance.getUpdateQuery(updateDoc)).to.deep.equals(expectedQuery);
    });
  });
});

describe('EnergyQuery (module)', function() {

  describe('#getExpressionFragment(expressionName, expressionValue, value)',
    function() {

      it('should convert to the respective expression fragment', function() {

        var f = function (test) {
          return energyQuery.getExpressionFragment('#k0', ':v0', test);
        }

        expect(f('some string')).to.equals('#k0 = :v0');
        expect(f(12345)).to.equals('#k0 = :v0');
        expect(f({key: 'random object'})).to.equals('#k0 = :v0');

        expect(f({$eq: 12345})).to.equals('#k0 = :v0');
        expect(f({$gt: 12345})).to.equals('#k0 > :v0');
        expect(f({$gte: 12345})).to.equals('#k0 >= :v0');
        expect(f({$lt: 12345})).to.equals('#k0 < :v0');
        expect(f({$lte: 12345})).to.equals('#k0 <= :v0');

      });

    }
  );

  describe('#getExpressionValue(value)',
    function() {

      it('should convert to the respective expression fragment', function() {

        var f = energyQuery.getExpressionValue;

        expect(f('some string')).to.deep.equals({S: 'some string'});
        expect(f(12345)).to.deep.equals({N: '12345'});
        expect(f({key: 'random object'})).to.deep.equals(
          {M: {key: {S: 'random object'}}}
        );

        expect(f({$eq: 12345})).to.deep.equals({N: '12345'});
        expect(f({$gt: 12345})).to.deep.equals({N: '12345'});
        expect(f({$gte: 12345})).to.deep.equals({N: '12345'});
        expect(f({$lt: 12345})).to.deep.equals({N: '12345'});
        expect(f({$lte: 12345})).to.deep.equals({N: '12345'});

      });

    }
  );

});

