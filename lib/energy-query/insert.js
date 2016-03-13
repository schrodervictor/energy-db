'use strict';

var BaseQueryBuilder = require('./base').BaseQueryBuilder;
var QueryHelper = require('./helper').QueryHelper;
var dynamoDoc = require('dynamo-doc');
var util = require('util');

function getBuilder() {
  return new InsertQueryBuilder();
}

function InsertQueryBuilder() {}
util.inherits(InsertQueryBuilder, BaseQueryBuilder);

InsertQueryBuilder.prototype.queryType = 'insert';

InsertQueryBuilder.prototype.allowedParams = [
  'Item',
  'TableName',
  'ConditionExpression',
  'ExpressionAttributeNames',
  'ExpressionAttributeValues',
  'ReturnConsumedCapacity',
  'ReturnItemCollectionMetrics',
  'ReturnValues',
];

InsertQueryBuilder.prototype.build = function build(doc, callback) {

  var query = this.getQueryBase();
  var self = this;

  return dynamoDoc.jsToDynamo(doc, function(err, dynamoItem) {
    if (err) return callback(err);

    query['Item'] = dynamoItem;
    query = self.finalValidation(query);

    return callback(null, query);
  });

}

module.exports = {
  getBuilder: getBuilder
}
