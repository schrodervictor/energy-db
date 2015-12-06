var chai = require('chai');
var sinon = require('sinon');
var sinonChai = require('sinon-chai');
var expect = chai.expect;
chai.use(sinonChai);

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
            { AttributeName: 'some-other-attr', AttributeType: 'N' },
            { AttributeName: 'some-global-index-attr', AttributeType: 'S' },
            { AttributeName: 'some-local-index-attr', AttributeType: 'N' },
        ],
        GlobalSecondaryIndexes: [
            {
                IndexName: 'Global-Index-1',
                IndexStatus: 'ACTIVE',
                KeySchema: [
                    { AttributeName: 'some-global-index-attr', KeyType: 'HASH' },
                    { AttributeName: 'some-range-key', KeyType: 'RANGE' }
                ],
            }
        ],
        LocalSecondaryIndexes: [
            {
                IndexName: 'Local-Index-1',
                IndexStatus: 'ACTIVE',
                KeySchema: [
                    { AttributeName: 'some-hash-key', KeyType: 'HASH' },
                    { AttributeName: 'some-local-index-attr', KeyType: 'RANGE' }
                ],
            }
        ]

    }
};

var connectorMock = {
    putItem: function(item, callback) {
        return callback();
    },
    query: function(item, callback) {
        return callback();
    },
    scan: function(item, callback) {
        return callback();
    }
};

var dbMock = {
    describeTable: function(tableName, callback) {
        if (tableName === tableInfo.Table.TableName) {
            return callback(null, tableInfo);
        } else {
            return callback(new Error());
        }
    },
    getConnector: function() {
        return connectorMock;
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

    describe('#validateDynamoItem(dynamoItem, callback)', function() {

        var table;

        beforeEach(function(done) {
            table = new EnergyTable(dbMock, 'Sample-Table');
            table.init(function(err, table) {
                return done(err);
            });
        });

        it('should should return the same object when the hash and range keys are defined and have the correct type', function(done) {
            var dynamoItem = {
                'some-hash-key': { S: 'some-value' },
                'some-range-key': { S: 'some-range' }
            };

            table.validateDynamoItem(dynamoItem, function(err, validDynamoItem) {
                if (err) return done(err);
                expect(validDynamoItem).to.equals(dynamoItem);
                done();
            });
        });

        it('should reject items without the hash key', function(done) {
            var dynamoItem = {
                'some-invalid-key': { S: 'some-value' },
                'other-invalid-key': { N: '12345' }
            };

            table.validateDynamoItem(dynamoItem, function(err, validDynamoItem) {
                expect(err).to.be.an.instanceOf(Error);
                done();
            });
        });

        it('should reject items without the range key', function(done) {
            var dynamoItem = {
                'some-hash-key': { S: 'some-value' },
                'some-invalid-key': { N: '12345' }
            };

            table.validateDynamoItem(dynamoItem, function(err, validDynamoItem) {
                expect(err).to.be.an.instanceOf(Error);
                done();
            });
        });

        it('should reject items when the hash key has the wrong type', function(done) {
            var dynamoItem = {
                'some-hash-key': { N: '12345' },
                'some-range-key': { S: 'some-range' }
            };

            table.validateDynamoItem(dynamoItem, function(err, validDynamoItem) {
                expect(err).to.be.an.instanceOf(Error);
                done();
            });
        });

        it('should reject items when the range key has the wrong type', function(done) {
            var dynamoItem = {
                'some-hash-key': { S: 'some-value' },
                'some-range-key': { N: '12345' }
            };

            table.validateDynamoItem(dynamoItem, function(err, validDynamoItem) {
                expect(err).to.be.an.instanceOf(Error);
                done();
            });
        });

    });

    describe('#putItem(item, callback)', function() {

        var table;

        beforeEach(function(done) {
            table = new EnergyTable(dbMock, 'Sample-Table');
            table.init(function(err, table) {
                return done(err);
            });
        });

        it('should redirect the call to putDynamoItem, with a transformed item', function(done) {
            var item = {
                'some-hash-key': 'some-value',
                'some-range-key': 'some-range',
                'other-key': 12345
            };

            var expectedDynamoItem = {
                'some-hash-key': { S: 'some-value' },
                'some-range-key': { S: 'some-range' },
                'other-key': { N: '12345' }
            };

            var stub = sinon.stub(table, 'putDynamoItem', function(dynamoItem, callback) {
                expect(dynamoItem).to.deep.equals(expectedDynamoItem);
                callback();
            });

            table.putItem(item, function thisCallback(err, result) {
                if (err) return done(err);
                expect(stub).to.have.been.calledOnce;
                expect(stub).to.have.been.calledWith(
                    sinon.match(expectedDynamoItem),
                    thisCallback
                );
                done();
            });
        });

    });

    describe('#putDynamoItem(dynamoItem, callback)', function() {

        var table;

        beforeEach(function(done) {
            table = new EnergyTable(dbMock, 'Sample-Table');
            table.init(function(err, table) {
                return done(err);
            });
        });

        it('should call the DynamoDB API to put the item', function(done) {
            var dynamoItem = {
                'some-hash-key': { S: 'some-value' },
                'some-range-key': { S: 'some-range' },
                'other-key': { N: '12345' }
            };

            sinon.spy(connectorMock, 'putItem');

            table.putDynamoItem(dynamoItem, function thisCallback(err, result) {
                if (err) return done(err);
                expect(connectorMock.putItem).to.have.been.calledOnce;
                expect(connectorMock.putItem).to.have.been.calledWith(
                    sinon.match({ Item: dynamoItem }),
                    thisCallback
                );
                connectorMock.putItem.restore();
                done();
            });
        });

        it('should comply with the format expected by DynamoDB API', function(done) {

            table.returnConsumedCapacity().returnOldValues();

            var dynamoItem = {
                'some-hash-key': { S: 'some-value' },
                'some-range-key': { S: 'some-range' },
                'other-key': { N: '12345' }
            };

            var expectedRequest = {
                TableName: 'Sample-Table',
                Item: dynamoItem,
                ReturnConsumedCapacity: 'TOTAL',
                ReturnValues: 'ALL_OLD',
            };

            sinon.spy(connectorMock, 'putItem');

            table.putDynamoItem(dynamoItem, function thisCallback(err, result) {
                if (err) return done(err);
                expect(connectorMock.putItem).to.have.been.calledOnce;
                expect(connectorMock.putItem).to.have.been.calledWith(
                    sinon.match(expectedRequest),
                    thisCallback
                );
                connectorMock.putItem.restore();
                done();
            });
        });

    });

    describe('#query(doc, callback)', function() {

        var table;

        beforeEach(function(done) {
            table = new EnergyTable(dbMock, 'Sample-Table');
            table.init(function(err, table) {
                return done(err);
            });
        });

        it('should call the query method on the connector, when the hash key is specified', function(done) {
            var doc = {
                'some-hash-key': 'some-value',
                'other-key': 12345
            };

            var expectedQuery = {
                TableName: 'Sample-Table',
                ExpressionAttributeNames: {
                    '#k0': 'some-hash-key',
                    '#k1': 'other-key'
                },
                ExpressionAttributeValues: {
                    ':v0': {S: 'some-value'},
                    ':v1': {N: '12345'}
                },
                KeyConditionExpression: '#k0 = :v0 AND #k1 = :v1'
            };

            sinon.spy(connectorMock, 'query');

            table.query(doc, function thisCallback(err, result) {
                if (err) return done(err);
                expect(connectorMock.query).to.have.been.calledOnce;
                expect(connectorMock.query).to.have.been.calledWith(
                    sinon.match(expectedQuery),
                    sinon.match.func
                );
                connectorMock.query.restore();
                done();
            });
        });

        it('should call the scan method on the connector, when the hash key is not specified', function(done) {
            var doc = {
                'a-key': 'some-string',
                'other-key': 12345
            };

            var expectedQuery = {
                TableName: 'Sample-Table',
                ExpressionAttributeNames: {
                    '#k0': 'a-key',
                    '#k1': 'other-key'
                },
                ExpressionAttributeValues: {
                    ':v0': {S: 'some-string'},
                    ':v1': {N: '12345'}
                },
                FilterExpression: '#k0 = :v0 AND #k1 = :v1'
            };

            sinon.spy(connectorMock, 'scan');

            table.query(doc, function thisCallback(err, result) {
                if (err) return done(err);
                expect(connectorMock.scan).to.have.been.calledOnce;
                expect(connectorMock.scan).to.have.been.calledWith(
                    sinon.match(expectedQuery),
                    sinon.match.func
                );
                connectorMock.scan.restore();
                done();
            });
        });

        it('should return the query results in the form of plain js objects (not dynamo)', function(done) {
            var doc = {
                'some-hash-key': 'some-value',
                'other-key': 12345
            };

            var queryResults = {
                Count: 1,
                ScannedCount: 1,
                Items: [
                    {
                        'some-hash-key': {S: 'some-value'},
                        'other-key': {N: '12345'},
                        'still-other-key': {M: {
                            'key1': {N: '999'},
                            'key2': {S: '444'}
                        }}
                    }
                ]
            };

            var expectedResults = [
                {
                    'some-hash-key': 'some-value',
                    'other-key': 12345,
                    'still-other-key': {
                        'key1': 999,
                        'key2': '444'
                    }
                }
            ];

            sinon.stub(connectorMock, 'query').callsArgWith(1, null, queryResults);

            table.query(doc, function thisCallback(err, result) {
                if (err) return done(err);
                expect(result).to.deep.equals(expectedResults);
                connectorMock.query.restore();
                done();
            });
        });

        it('should return the scan results in the form of plain js objects (not dynamo)', function(done) {
            var doc = {
                'a-key': 'some-string',
                'other-key': 12345
            };

            var queryResults = {
                Count: 2,
                ScannedCount: 50,
                Items: [
                    {
                        'some-hash-key': {S: 'some-value'},
                        'other-key': {N: '12345'},
                        'a-key': {S: 'some-string'},
                        'still-other-key': {M: {
                            'key1': {N: '999'},
                            'key2': {S: '444'}
                        }}
                    },
                    {
                        'some-hash-key': {S: 'some-value-2'},
                        'other-key': {N: '12345'},
                        'a-key': {S: 'some-string'},
                        'still-other-key': {M: {
                            'key1': {N: '333'},
                            'key2': {S: '111'}
                        }}
                    }
                ]
            };

            var expectedResults = [
                {
                    'some-hash-key': 'some-value',
                    'other-key': 12345,
                    'a-key': 'some-string',
                    'still-other-key': {
                        'key1': 999,
                        'key2': '444'
                    }
                },
                {
                    'some-hash-key': 'some-value-2',
                    'other-key': 12345,
                    'a-key': 'some-string',
                    'still-other-key': {
                        'key1': 333,
                        'key2': '111'
                    }
                }
            ];

            sinon.stub(connectorMock, 'scan').callsArgWith(1, null, queryResults);

            table.query(doc, function thisCallback(err, result) {
                if (err) return done(err);
                expect(result).to.deep.equals(expectedResults);
                connectorMock.scan.restore();
                done();
            });
        });

    });

});
