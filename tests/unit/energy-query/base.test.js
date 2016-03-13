'use strict';

var expect = require('chai').expect;
var base = require('../../../lib/energy-query/base');

describe('BaseQueryBuilder', function() {

  describe('#getQueryBase()', function() {

    it('should return an object that is a clone of the table\'s query base',
      function() {
        var builder = new (base.BaseQueryBuilder)();
        var queryBase = {
          TableName: 'Table-Name-Here',
        };
        builder.configure({
          base: queryBase,
          hashKey: 'hash-key'
        });

        var query = builder.getQueryBase();

        expect(query).to.not.equals(queryBase);
        expect(query).to.deep.equals(queryBase);
      }
    );

    it('should combine the query base and query options, with preference to ' +
      'parameters in the query options object',
      function () {
        var builder = new (base.BaseQueryBuilder)();

        var queryBase = {
          TableName: 'Table-Name-Here',
          ConsistentRead: false
        };

        var options = {
          ConsistentRead: true,
          ReturnConsumedCapacity: true
        };

        var expectedQuery = {
          TableName: 'Table-Name-Here',
          ConsistentRead: true,
          ReturnConsumedCapacity: true
        };

        builder.configure({
          base: queryBase,
          options: options,
          hashKey: 'hash-key'
        });

        var query = builder.getQueryBase();

        expect(query).to.not.equals(queryBase);
        expect(query).to.not.equals(options);
        expect(query).to.deep.equals(expectedQuery);
      }
    );

  });

  describe('#getExpressionFragment(expressionName, expressionValue, value)',
    function() {

      it('should convert to the respective expression fragment', function() {

        var f = function (test) {
          return base.getExpressionFragment('#k0', ':v0', test);
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

        var f = base.getExpressionValue;

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
