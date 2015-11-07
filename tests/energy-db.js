var expect = require('chai').expect;
var energyDB = require('../lib/energy-db');
var EnergyDB = energyDB.EnergyDB;
var DynamoDB = require('aws-sdk').DynamoDB;

var dynamoDBMock = {};

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
});
