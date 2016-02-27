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
  // Some important initial values
  this.i = 0;
  this.attributes = {};
  this.values = {};

  // Values injected in this constructor
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

  var expressions = [];

  for (var key in this.queryDoc) {
    var value = this.queryDoc[key];
    var expressionName = '#k' + this.i;
    var expressionValue = ':v' + this.i;
    expressions.push(
      getExpressionFragment(expressionName, expressionValue, value)
    );
    this.attributes[expressionName] = key;
    this.values[expressionValue] = getExpressionValue(value);
    this.i++;
  }

  var expression = expressions.join(' AND ');

  if ('query' === this.method) {
    query['KeyConditionExpression'] = expression;
  } else if ('scan' === this.method) {
    query['FilterExpression'] = expression;
  } else if ('update' === this.method || 'delete' === this.method) {
    query['ConditionExpression'] = expression;

    query['Key'] = {};
    query['Key'][this.tableHashKey] =
      dynamoDoc.dynamoValue(this.queryDoc[this.tableHashKey]);

    if (this.tableRangeKey) {
      query['Key'][this.tableRangeKey] =
        dynamoDoc.dynamoValue(this.queryDoc[this.tableRangeKey]);
    }
  }

  query['ExpressionAttributeNames'] = this.attributes;
  query['ExpressionAttributeValues'] = this.values;

  return query;
};

var updateOperations = {
  $set: function(context, doc) {
    var expressions = [];

    for (var key in doc) {
      var value = doc[key];
      var expressionName = '#k' + context.i;
      var expressionValue = ':v' + context.i;
      expressions.push(expressionName + ' = ' + expressionValue);
      context.attributes[expressionName] = key;
      context.values[expressionValue] = getExpressionValue(value);
      context.i++;
    }

    var expression = 'SET ' + expressions.join(', ');

    return expression;
  },
};

EnergyQuery.prototype.getUpdateQuery = function getUpdateQuery(updateDoc) {

  var query = this.getQuery();

  if ('update' !== this.method) {
    return query;
  }

  var updateExpressions = [];

  for (var key in updateDoc) {
    // The update operations will append stuff on this.attributes
    // and this.values
    var expression = updateOperations[key](this, updateDoc[key]);
    updateExpressions.push(expression);
  }

  var updateExpression = updateExpressions.join(' ');

  query['UpdateExpression'] = updateExpression;
  query['ExpressionAttributeNames'] = this.attributes;
  query['ExpressionAttributeValues'] = this.values;

  return query;

};

module.exports = {
  EnergyQuery: EnergyQuery,
  getExpressionFragment: getExpressionFragment,
  getExpressionValue: getExpressionValue
}
