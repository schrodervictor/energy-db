'use strict';

var dynamoDoc = require('dynamo-doc');
var simpleClone = require('../utils/clone').simpleClone;

function BaseQueryBuilder() {}

// queryType and allowedParams must be overriden by child classes
BaseQueryBuilder.prototype.queryType = 'abstract';
BaseQueryBuilder.prototype.allowedParams = [];

BaseQueryBuilder.prototype.configure = function configure(config) {

  if (!config.hashKey) {
    throw new Error('hashKey is mandatory to build queries');
  }

  this.base = config.base || {};
  this.options = config.options || {};
  this.hashKey = config.hashKey;
  this.rangeKey = config.rangeKey;

  return this;
};

BaseQueryBuilder.prototype.getQueryBase = function getQueryBase() {
  var query = simpleClone(this.base);
  if (this.options && 'object' === typeof this.options) {
    for (var key in this.options) {
      query[key] = this.options[key];
    }
  }
  return query;
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
