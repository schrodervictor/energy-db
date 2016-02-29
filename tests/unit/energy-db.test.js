var chai = require('chai');
var expect = chai.expect;
var sinon = require('sinon');
var sinonChai = require('sinon-chai');
chai.use(sinonChai);

var energyDB = require('../../lib/energy-db');
var EnergyDB = energyDB.EnergyDB;
var DynamoDB = require('aws-sdk').DynamoDB;
var EnergyTable = require('../../lib/energy-table').EnergyTable;

var dynamoDBMock = require('../fixtures/table-mocks').connectorMock;

describe('energy-db', function() {

  describe('#connect(settings, callback)', function() {

    it('should pass an EnergyDB instance to the callback', function(done) {
      energyDB.connect({}, function(err, db) {
        if (err) return done(err);
        expect(db).to.be.an.instanceOf(EnergyDB);
        expect(db.connector).to.be.an.instanceOf(DynamoDB);
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

  var sandbox = sinon.sandbox.create();

  afterEach(function() {
    sandbox.restore();
  });

  describe('#getConnector()', function() {

    it('should return the stored connector', function() {
      var db = new EnergyDB({}, dynamoDBMock);
      var connector = db.getConnector();
      expect(connector).to.equals(dynamoDBMock);
    });

  });

  describe('#describeTable(tableName, callback)', function() {

    it('should forward the request to DynamoDB with the expected query format', function(done) {
      var db = new EnergyDB({}, dynamoDBMock);
      var tableName = 'Table-HashKey';
      var expectedQuery = {TableName: tableName};

      sandbox.spy(dynamoDBMock, 'describeTable');

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
      db.table('Table-HashKey', function(err, table) {
        if (err) return done(err);
        expect(table).to.be.an.instanceOf(EnergyTable);
        done();
      });
    });

  });

  describe('#collection(collectionName, callback)', function() {

    it('should work exactly like the table method (alias)', function(done) {
      var db = new EnergyDB({}, dynamoDBMock);
      db.collection('Table-HashKey', function(err, table) {
        if (err) return done(err);
        expect(table).to.be.an.instanceOf(EnergyTable);
        done();
      });
    });

  });

});
