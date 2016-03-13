'use strict';

var BaseQueryBuilder = require('./base').BaseQueryBuilder;
var QueryHelper = require('./helper').QueryHelper;
var dynamoDoc = require('dynamo-doc');
var util = require('util');

function getBuilder() {
  return new ReplaceQueryBuilder();
}

function ReplaceQueryBuilder() {}
util.inherits(ReplaceQueryBuilder, BaseQueryBuilder);

ReplaceQueryBuilder.prototype.queryType = 'delete';

ReplaceQueryBuilder.prototype.allowedParams = [
  'Item',
  'TableName',
  'ConditionExpression',
  'ExpressionAttributeNames',
  'ExpressionAttributeValues',
  'ReturnConsumedCapacity',
  'ReturnItemCollectionMetrics',
  'ReturnValues',
];

ReplaceQueryBuilder.prototype.build = function build(doc, update, callback) {

  var query = this.getQueryBase();
  var helper = new QueryHelper();

  for (var key in doc) {
    this.mapExpression(helper, key, doc[key]);
  }

  var expression = helper.expressions.join(' AND ');

  query['ConditionExpression'] = expression;
  query['ExpressionAttributeNames'] = helper.attributes;
  query['ExpressionAttributeValues'] = helper.values;

  var self = this;

  return dynamoDoc.jsToDynamo(update, function(err, dynamoItem) {
    if (err) return callback(err);
    query['Item'] = dynamoItem;
    query = self.finalValidation(query);
    return callback(null, query);
  });

}

module.exports = {
  getBuilder: getBuilder
}
