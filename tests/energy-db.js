var chai = require('chai');
var expect = chai.expect;
var energyDB = require('../lib/energy-db');
var EnergyDB = energyDB.EnergyDB;
var DynamoDB = require('aws-sdk').DynamoDB;
var EnergyTable = require('../lib/energy-table').EnergyTable;

var sinon = require('sinon');
var sinonChai = require('sinon-chai');
chai.use(sinonChai);

var dynamoDBMock = {
    describeTable: sinon.spy(function(tableName, callback) {
        var tableInfo = {
            Table: {
                TableName: tableName,
                KeySchema: [
                    { AttributeName: 'some-hash-key', KeyType: 'HASH' },
                ],
                AttributeDefinitions: [
                    { AttributeName: 'some-hash-key', AttributeType: 'S' },
                ]
            }
        };
        return callback(null, tableInfo);
    })
};

describe('energy-db', function() {
    describe('#connect(settings, callback)', function() {

        it('should pass an EnergyDB instance to the callback', function(done) {
            energyDB.connect({}, function(err, db) {
                if (err) return done(err);
                expect(db).to.be.an.instanceOf(EnergyDB);
                done();
            });
        });

        it('should have an instance of DynamoDB available as a connector', function(done) {
            energyDB.connect({}, function(err, db) {
                if (err) return done(err);
                expect(db.connector).to.be.an.instanceOf(DynamoDB);
                done();
            });
        });

    });
});

describe('EnergyDB', function() {

    describe('#getConnector()', function() {
        it('should return the stored connector', function() {
            var db = new EnergyDB({}, dynamoDBMock);
            var connector = db.getConnector();
            expect(connector).to.equals(dynamoDBMock);
        });
    });

    describe('#describeTable(tableName, callback)', function() {

        afterEach(function() {
            dynamoDBMock.describeTable.reset();
        });

        it('should forward the request to DynamoDB with the expected query format', function(done) {

            var db = new EnergyDB({}, dynamoDBMock);
            var tableName = 'Table-Name';
            var expectedQuery = {TableName: tableName};

            db.describeTable(tableName, function thisCallback(err, tableInfo) {
                if (err) return done(err);
                expect(dynamoDBMock.describeTable).to.have.been.calledOnce;
                expect(dynamoDBMock.describeTable).to.have.been.calledWith(
                    sinon.match(expectedQuery),
                    thisCallback
                );
                done();
            });
        });
    });

    describe('#table(tableName, callback)', function() {

        it('should pass an EnergyTable instance to the callback', function(done) {
            var db = new EnergyDB({}, dynamoDBMock);
            db.table('Table-Name', function(err, table) {
                if (err) return done(err);
                expect(table).to.be.an.instanceOf(EnergyTable);
                done();
            });
        });

    });
});
