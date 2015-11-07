var expect = require('chai').expect;
var energyTable = require('../lib/energy-table');
var EnergyTable = require('../lib/energy-table').EnergyTable;

var dbMock = {};

var tableInfo = {
    Table: {
        TableName: 'Sample-Table',
        KeySchema: [
            { AttributeName: 'some-hash-key', KeyType: 'HASH' },
            { AttributeName: 'some-range-key', KeyType: 'RANGE' }
        ],
        AttributeDefinitions: [
            { AttributeName: 'some-hash-key', AttributeType: 'S' },
            { AttributeName: 'some-range-key', AttributeType: 'S' },
            { AttributeName: 'some-other-attr', AttributeType: 'N' }
        ]
    }
};

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

describe('EnergyTable (class)', function() {

    describe('#setKeySchema(tableInfo)', function() {

        it('should set the table properties with keys information', function() {
            var table = new EnergyTable(dbMock, 'Sample-Table');
            table.setKeySchema(tableInfo);
            expect(table.hashKey).to.equals('some-hash-key');
            expect(table.hashKeyType).to.equals('S');
            expect(table.rangeKey).to.equals('some-range-key');
            expect(table.rangeKeyType).to.equals('S');
        });

    });

});
