'use strict';

var BaseQueryBuilder = require('./base').BaseQueryBuilder;
var QueryHelper = require('./helper').QueryHelper;
var dynamoDoc = require('dynamo-doc');
var util = require('util');

function getBuilder() {
  return new UpdateQueryBuilder();
}

function UpdateQueryBuilder() {}
util.inherits(UpdateQueryBuilder, BaseQueryBuilder);

UpdateQueryBuilder.prototype.queryType = 'insert';

UpdateQueryBuilder.prototype.allowedParams = [
  'Key',
  'TableName',
  'ConditionExpression',
  'ExpressionAttributeNames',
  'ExpressionAttributeValues',
  'ReturnConsumedCapacity',
  'ReturnItemCollectionMetrics',
  'ReturnValues',
  'UpdateExpression',
];

var updateOperations = {
  $set: function(context, doc) {
    var helper = context.getCollector();

    for (var key in doc) {
      UpdateQueryBuilder.prototype.mapExpression(helper, key, doc[key], ' = ');
    }

    var expression = 'SET ' + helper.expressions.join(', ');

    return expression;
  },
  $inc: function(context, doc) {
    var helper = context.getCollector();

    for (var key in doc) {
      UpdateQueryBuilder.prototype.mapExpression(helper, key, doc[key], ' ');
    }

    var expression = 'ADD ' + helper.expressions.join(', ');

    return expression;
  },
  $unset: function(context, doc) {
    var helper = context.getCollector();

    for (var key in doc) {
      UpdateQueryBuilder.prototype.mapExpression(helper, key);
    }

    var expression = 'REMOVE ' + helper.expressions.join(', ');

    return expression;
  },
};


UpdateQueryBuilder.prototype.build = function build(doc, update, callback) {

    var query = this.getQueryBase();
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

    var updateExpressions = [];

    for (var key in update) {
      // The update operations will append stuff on helper.attributes
      // and helper.values
      var expression = updateOperations[key](helper, update[key]);
      updateExpressions.push(expression);
    }

    var updateExpression = updateExpressions.join(' ');

    query['UpdateExpression'] = updateExpression;
    query['ExpressionAttributeNames'] = helper.attributes;
    query['ExpressionAttributeValues'] = helper.values;

    query = this.finalValidation(query);

    return callback(null, query);
}

module.exports = {
  getBuilder: getBuilder
}
