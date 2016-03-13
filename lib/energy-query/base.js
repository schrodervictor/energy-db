'use strict';

var dynamoDoc = require('dynamo-doc');

function BaseQueryBuilder() {}

BaseQueryBuilder.prototype.configure = function configure(config) {

  if (!config.hashKey) {
    throw new Error('hashKey is mandatory to build queries');
  }

  this.base = config.queryBase || {};
  this.options = config.options || {};
  this.hashKey = config.hashKey;
  this.rangeKey = config.rangeKey;

  return this;
};

BaseQueryBuilder.prototype.mapExpression =
  function mapExpression(context, key, value, glue) {
    var expressionName;
    var expressionValue;

    expressionName = '#k' + context.counter.i;
    context.attributes[expressionName] = key;

    if ('undefined' !== typeof value) {
      expressionValue = ':v' + context.counter.i;
      context.values[expressionValue] = getExpressionValue(value);
    }

    context.expressions.push(
      getExpressionFragment(expressionName, expressionValue, value, glue)
    );

    context.inc();
  };

BaseQueryBuilder.prototype.queryType = 'abstract';
BaseQueryBuilder.prototype.allowedParams = [];

BaseQueryBuilder.prototype.finalValidation = function finalValidation(query) {
  for (var key in query) {
    if (this.allowedParams.indexOf(key) === -1) {
      delete query[key];
    }
  }
  return query;
};

function getExpressionFragment(expressionName, expressionValue, value, glue) {
  if ('undefined' === typeof expressionValue) {
    return expressionName;
  }

  if ('undefined' === typeof glue) {
    glue = ' ' + getOperator(value) + ' ';
  }

  return expressionName + glue + expressionValue;
}

function getExpressionValue(value) {
  if (!isOperator(value)) {
    return dynamoDoc.dynamoValue(value);
  }

  var key = Object.keys(value)[0];

  return dynamoDoc.dynamoValue(value[key]);
}


var operators = {
  '$eq': '=',
  '$gt': '>',
  '$gte': '>=',
  '$lt': '<',
  '$lte': '<='
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

module.exports = {
  BaseQueryBuilder: BaseQueryBuilder,
  getExpressionFragment: getExpressionFragment,
  getExpressionValue: getExpressionValue
};
