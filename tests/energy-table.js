var expect = require('chai').expect;
var energyTable = require('../lib/energy-table');
var EnergyTable = require('../lib/energy-table').EnergyTable;

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

var dbMock = {
    describeTable: function(tableName, callback) {
        if (tableName === tableInfo.Table.TableName) {
            return callback(null, tableInfo);
        } else {
            return callback(new Error());
        }
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

    describe('EnergyTable(db, tableName) - constructor', function() {

        it('should set the hashKey and rangeKey properties when constructing', function(done) {
            energyTable.getInstance(dbMock, 'Sample-Table', function(err, table) {
                if (err) done(err);
                expect(table.hashKey).to.equals('some-hash-key');
                expect(table.rangeKey).to.equals('some-range-key');
                done();
            });
        });

        it('should initialize the queryBase object', function(done) {
            energyTable.getInstance(dbMock, 'Sample-Table', function(err, table) {
                if (err) done(err);
                var queryBase = {
                    TableName: 'Sample-Table'
                };
                expect(table.queryBase).to.deep.equal(queryBase);
                done();
            });
        });

    });

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

    describe('#initQueryBase()', function() {

        it('should initialize the query base object', function() {
            var table = new EnergyTable(dbMock, 'Sample-Table');
            var expectedQueryBase = {
                TableName: 'Sample-Table'
            };
            table.initQueryBase();
            expect(table.queryBase).to.deep.equal(expectedQueryBase);
        });

    });

    describe('#addQueryParam(key, value)', function() {

        var table;

        beforeEach(function(done) {
            table = new EnergyTable(dbMock, 'Sample-Table');
            table.init(function(err, table) {
                return done(err);
            });
        });

        it('should add a new query param to the queryBase object', function() {
            var queryBase = {
                TableName: 'Sample-Table',
                NewParam: 'new-value'
            };
            table.addQueryParam('NewParam', 'new-value');
            expect(table.queryBase).to.deep.equal(queryBase);
        });

    });

    describe('#addQueryParams(params)', function() {

        var table;

        beforeEach(function(done) {
            table = new EnergyTable(dbMock, 'Sample-Table');
            table.init(function(err, table) {
                return done(err);
            });
        });

        it('should add several params to the queryBase object', function() {
            var queryBase = {
                TableName: 'Sample-Table',
                NewParam1: 'new-value-1',
                NewParam2: 'new-value-2'
            };
            table.addQueryParams({
                NewParam1: 'new-value-1',
                NewParam2: 'new-value-2'
            });
            expect(table.queryBase).to.deep.equal(queryBase);
        });

    });

    describe('#returnConsumedCapacity()', function() {

        var table;

        beforeEach(function(done) {
            table = new EnergyTable(dbMock, 'Sample-Table');
            table.init(function(err, table) {
                return done(err);
            });
        });

        it('should add the API flag to return the consumed capacity', function() {
            var queryBase = {
                TableName: 'Sample-Table',
                ReturnConsumedCapacity: 'TOTAL'
            };
            table.returnConsumedCapacity();
            expect(table.queryBase).to.deep.equal(queryBase);
        });

    });

    describe('#returnOldValues()', function() {

        var table;

        beforeEach(function(done) {
            table = new EnergyTable(dbMock, 'Sample-Table');
            table.init(function(err, table) {
                return done(err);
            });
        });

        it('should add the API flag to return all the old values for after an update query', function() {
            var queryBase = {
                TableName: 'Sample-Table',
                ReturnValues: 'ALL_OLD'
            };
            table.returnOldValues();
            expect(table.queryBase).to.deep.equal(queryBase);
        });

    });

});
