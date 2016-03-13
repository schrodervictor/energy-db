'use strict';

var BaseQueryBuilder = require('./base').BaseQueryBuilder;
var QueryHelper = require('./helper').QueryHelper;
var util = require('util');

function getBuilder() {
  return new ScanQueryBuilder();
}

function ScanQueryBuilder() {}
util.inherits(ScanQueryBuilder, BaseQueryBuilder);

ScanQueryBuilder.prototype.queryType = 'scan';

ScanQueryBuilder.prototype.allowedParams = [
  'TableName',
  'ConsistentRead',
  'ExclusiveStartKey',
  'ExpressionAttributeNames',
  'ExpressionAttributeValues',
  'FilterExpression',
  'IndexName',
  'Limit',
  'ProjectionExpression',
  'Segment',
  'TotalSegments',
  'ReturnConsumedCapacity',
  'Select',
];

ScanQueryBuilder.prototype.build = function build(doc, callback) {

  var query = this.getQueryBase();

  if (Object.keys(doc).length > 0) {
    var helper = new QueryHelper();

    for (var key in doc) {
      this.mapExpression(helper, key, doc[key]);
    }

    var expression = helper.expressions.join(' AND ');

    query['FilterExpression'] = expression;
    query['ExpressionAttributeNames'] = helper.attributes;
    query['ExpressionAttributeValues'] = helper.values;
  }

  query = this.finalValidation(query);

  return callback(null, query);
}

module.exports = {
  getBuilder: getBuilder
}
