'use strict';

var BaseQueryBuilder = require('./base').BaseQueryBuilder;
var QueryHelper = require('./helper').QueryHelper;
var dynamoDoc = require('dynamo-doc');
var util = require('util');

function getBuilder() {
  return new DeleteQueryBuilder();
}

function DeleteQueryBuilder() {}
util.inherits(DeleteQueryBuilder, BaseQueryBuilder);

DeleteQueryBuilder.prototype.queryType = 'delete';

DeleteQueryBuilder.prototype.allowedParams = [
  'Key',
  'TableName',
  'ConditionExpression',
  'ExpressionAttributeNames',
  'ExpressionAttributeValues',
  'ReturnConsumedCapacity',
  'ReturnItemCollectionMetrics',
  'ReturnValues',
];

DeleteQueryBuilder.prototype.build = function build(doc, callback) {

  var query = this.getQueryBase();
  var helper = new QueryHelper();

  for (var key in doc) {
    if (key === this.hashKey || key === this.rangeKey) continue;
    this.mapExpression(helper, key, doc[key]);
  }

  var expression = helper.expressions.join(' AND ');

  if (expression) query['ConditionExpression'] = expression;

  query['Key'] = {};
  query['Key'][this.hashKey] =
    dynamoDoc.dynamoValue(doc[this.hashKey]);

  if (this.rangeKey) {
    query['Key'][this.rangeKey] =
      dynamoDoc.dynamoValue(doc[this.rangeKey]);
  }

  if (Object.keys(helper.attributes).length > 0) {
    query['ExpressionAttributeNames'] = helper.attributes;
  }
  if (Object.keys(helper.values).length > 0) {
    query['ExpressionAttributeValues'] = helper.values;
  }

  query = this.finalValidation(query);

  return callback(null, query);
}

module.exports = {
  getBuilder: getBuilder
}
