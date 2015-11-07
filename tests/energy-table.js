var expect = require('chai').expect;

var energyTable = require('../lib/energy-table');
var EnergyTable = require('../lib/energy-table').EnergyTable;

var dbMock = {};

describe('EnergyTable (module)', function() {
    describe('#getInstance(db, tableName, callback)', function() {

        it('should pass an EnergyTable instance to the callback', function(done) {
            energyTable.getInstance(dbMock, 'Sample-Table', function(err, table) {
                if (err) done(err);
                expect(table).to.be.an.instanceOf(EnergyTable);
                done();
            });
        });

    });
});
