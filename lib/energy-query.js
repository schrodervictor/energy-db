'use strict';

var dynamoDoc = require('dynamo-doc');
var simpleClone = require('./utils/clone').simpleClone;

var operators = {
  '$eq': '=',
  '$gt': '>',
  '$gte': '>=',
  '$lt': '<',
  '$lte': '<='
};


var EnergyQuery = function(
  method,
  queryBase,
  queryDoc,
  tableHashKey,
  tableRangeKey,
  indexes
) {
  this.method = method;
  this.queryBase = simpleClone(queryBase);
  this.queryDoc = queryDoc;
  this.tableHashKey = tableHashKey;
  this.tableRangeKey = tableRangeKey;
  this.indexes = indexes;
};

function getOperator(value) {
  if (!isOperator(value)) {
    return '=';
  }

  var key = Object.keys(value)[0];
  return operators[key];
}

function isOperator(value) {

  if (
    !('object' === typeof value)
    ||
    !(1 === Object.keys(value).length)
  ) {
    return false;
  }

  var key = Object.keys(value)[0];

  if (!(key in operators)) {
    return false;
  }

  return true;
}

function getExpressionFragment(expressionName, expressionValue, value) {
  var operator = getOperator(value);
  return expressionName + ' ' + operator + ' ' + expressionValue;
}

function getExpressionValue(value) {
  if (!isOperator(value)) {
    return dynamoDoc.dynamoValue(value);
  }

  var key = Object.keys(value)[0];

  return dynamoDoc.dynamoValue(value[key]);
}

EnergyQuery.prototype.getQuery = function getQuery() {

  var query = simpleClone(this.queryBase);

  query['ExpressionAttributeNames'] = {};
  query['ExpressionAttributeValues'] = {};

  var expressions = [];

  var i = 0;
  for (var key in this.queryDoc) {
    var value = this.queryDoc[key];
    var expressionName = '#k' + i;
    var expressionValue = ':v' + i;
    expressions.push(
      getExpressionFragment(expressionName, expressionValue, value)
    );
    query['ExpressionAttributeNames'][expressionName] = key;
    query['ExpressionAttributeValues'][expressionValue] = getExpressionValue(value);
    i++;
  }

  var expression = expressions.join(' AND ');

  if ('query' === this.method) {
    query['KeyConditionExpression'] = expression;
  } else if ('scan' === this.method) {
    query['FilterExpression'] = expression;
  } else if ('delete' === this.method) {
    query['ConditionExpression'] = expression;

    query['Key'] = {};
    query['Key'][this.tableHashKey] =
      dynamoDoc.dynamoValue(this.queryDoc[this.tableHashKey]);

    if (this.tableRangeKey) {
      query['Key'][this.tableRangeKey] =
        dynamoDoc.dynamoValue(this.queryDoc[this.tableRangeKey]);
    }
  }

  return query;
};

module.exports = {
  EnergyQuery: EnergyQuery,
  getExpressionFragment: getExpressionFragment,
  getExpressionValue: getExpressionValue
}
