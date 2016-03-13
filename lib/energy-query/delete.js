'use strict';

var BaseQueryBuilder = require('./base').BaseQueryBuilder;
var QueryHelper = require('./helper').QueryHelper;
var dynamoDoc = require('dynamo-doc');
var simpleClone = require('../utils/clone').simpleClone;
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

  var query = simpleClone(this.base);
  var helper = new QueryHelper();

  for (var key in doc) {
    this.mapExpression(helper, key, doc[key]);
  }

  var expression = helper.expressions.join(' AND ');

  query['ConditionExpression'] = expression;

  query['Key'] = {};
  query['Key'][this.hashKey] =
    dynamoDoc.dynamoValue(doc[this.hashKey]);

  if (this.rangeKey) {
    query['Key'][this.rangeKey] =
      dynamoDoc.dynamoValue(doc[this.rangeKey]);
  }

  query['ExpressionAttributeNames'] = helper.attributes;
  query['ExpressionAttributeValues'] = helper.values;

  query = this.finalValidation(query);

  return callback(null, query);
}

module.exports = {
  getBuilder: getBuilder
}
