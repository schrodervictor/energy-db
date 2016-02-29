var expect = require('chai').expect;
var simpleClone = require('../../../lib/utils/clone').simpleClone;

describe('simpleClone(obj)', function() {
  it('should return a simple clone (one level) of an object', function() {
    var obj = {
      aKey: 'some value',
      anotherKey: 12345
    };

    var result = simpleClone(obj);

    expect(result).to.deep.equal(obj);
    expect(result).to.not.equal(obj);
  });
});
