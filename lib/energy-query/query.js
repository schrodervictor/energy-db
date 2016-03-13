'use strict';

var BaseQueryBuilder = require('./base').BaseQueryBuilder;
var QueryHelper = require('./helper').QueryHelper;
var util = require('util');

function getBuilder() {
  return new QueryQueryBuilder();
}

function QueryQueryBuilder() {}
util.inherits(QueryQueryBuilder, BaseQueryBuilder);

QueryQueryBuilder.prototype.queryType = 'query';

QueryQueryBuilder.prototype.allowedParams = [
  'TableName',
  'ConsistentRead',
  'ExclusiveStartKey',
  'ExpressionAttributeNames',
  'ExpressionAttributeValues',
  'FilterExpression',
  'IndexName',
  'KeyConditionExpression',
  'Limit',
  'ProjectionExpression',
  'ReturnConsumedCapacity',
  'ScanIndexForward',
  'Select',
];

QueryQueryBuilder.prototype.build = function build(doc, callback) {

  var query = this.getQueryBase();
  var helper = new QueryHelper();

  for (var key in doc) {
    this.mapExpression(helper, key, doc[key]);
  }

  var expression = helper.expressions.join(' AND ');

  query['KeyConditionExpression'] = expression;
  query['ExpressionAttributeNames'] = helper.attributes;
  query['ExpressionAttributeValues'] = helper.values;

  query = this.finalValidation(query);

  callback(null, query);
}

module.exports = {
  getBuilder: getBuilder
}
