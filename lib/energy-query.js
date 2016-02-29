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

var EnergyQueryFactory = function(table) {
  this.table = table;
  this.queryBase = table.queryBase;
  this.tableHashKey = table.hashKey;
  this.tableRangeKey = table.rangeKey;
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

function QueryHelper() {
  this.counter =  {i: 0};
  this.attributes = {};
  this.values = {};
  this.expressions = [];

  this.inc = function() {
    this.counter.i++;
  };

  this.getCollector = function() {
    return {
      counter: this.counter,
      attributes: this.attributes,
      values: this.values,
      expressions: [],
      inc: this.inc
    }
  };
}

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
}

EnergyQueryFactory.prototype.getQuery = function getQuery(doc, callback) {

  var query = simpleClone(this.queryBase);
  var helper = new QueryHelper();

  for (var key in doc) {
    mapExpression(helper, key, doc[key]);
  }

  var expression = helper.expressions.join(' AND ');

  query['KeyConditionExpression'] = expression;
  query['ExpressionAttributeNames'] = helper.attributes;
  query['ExpressionAttributeValues'] = helper.values;

  return callback(null, query);
};

EnergyQueryFactory.prototype.getInsertQuery = function getInsertQuery(doc, callback) {

  var query = simpleClone(this.queryBase);

  return dynamoDoc.jsToDynamo(doc, function(err, dynamoItem) {
    if (err) return callback(err);
    query['Item'] = dynamoItem;
    return callback(null, query);
  });

};

EnergyQueryFactory.prototype.getScanQuery = function getQuery(doc, callback) {

  var query = simpleClone(this.queryBase);
  var helper = new QueryHelper();

  for (var key in doc) {
    mapExpression(helper, key, doc[key]);
  }

  var expression = helper.expressions.join(' AND ');

  query['FilterExpression'] = expression;
  query['ExpressionAttributeNames'] = helper.attributes;
  query['ExpressionAttributeValues'] = helper.values;

  return callback(null, query);
};

EnergyQueryFactory.prototype.getDeleteQuery = function getQuery(doc, callback) {

  var query = simpleClone(this.queryBase);
  var helper = new QueryHelper();

  for (var key in doc) {
    mapExpression(helper, key, doc[key]);
  }

  var expression = helper.expressions.join(' AND ');

  query['ConditionExpression'] = expression;

  query['Key'] = {};
  query['Key'][this.tableHashKey] =
    dynamoDoc.dynamoValue(doc[this.tableHashKey]);

  if (this.tableRangeKey) {
    query['Key'][this.tableRangeKey] =
      dynamoDoc.dynamoValue(doc[this.tableRangeKey]);
  }

  query['ExpressionAttributeNames'] = helper.attributes;
  query['ExpressionAttributeValues'] = helper.values;

  return callback(null, query);
};

var updateOperations = {
  $set: function(context, doc) {
    var helper = context.getCollector();

    for (var key in doc) {
      mapExpression(helper, key, doc[key], ' = ');
    }

    var expression = 'SET ' + helper.expressions.join(', ');

    return expression;
  },
  $inc: function(context, doc) {
    var helper = context.getCollector();

    for (var key in doc) {
      mapExpression(helper, key, doc[key], ' ');
    }

    var expression = 'ADD ' + helper.expressions.join(', ');

    return expression;
  },
  $unset: function(context, doc) {
    var helper = context.getCollector();

    for (var key in doc) {
      mapExpression(helper, key);
    }

    var expression = 'REMOVE ' + helper.expressions.join(', ');

    return expression;
  },
};

EnergyQueryFactory.prototype.getUpdateQuery =
  function getUpdateQuery(doc, update, callback) {

    var query = simpleClone(this.queryBase);
    var helper = new QueryHelper();

    for (var key in doc) {
      mapExpression(helper, key, doc[key]);
    }

    var expression = helper.expressions.join(' AND ');

    query['ConditionExpression'] = expression;

    query['Key'] = {};
    query['Key'][this.tableHashKey] =
      dynamoDoc.dynamoValue(doc[this.tableHashKey]);

    if (this.tableRangeKey) {
      query['Key'][this.tableRangeKey] =
        dynamoDoc.dynamoValue(doc[this.tableRangeKey]);
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

    return callback(null, query);
  };

EnergyQueryFactory.prototype.getReplaceQuery =
  function getReplaceQuery(doc, update, callback) {

    var query = simpleClone(this.queryBase);
    var helper = new QueryHelper();

    for (var key in doc) {
      mapExpression(helper, key, doc[key]);
    }

    var expression = helper.expressions.join(' AND ');

    query['ConditionExpression'] = expression;
    query['ExpressionAttributeNames'] = helper.attributes;
    query['ExpressionAttributeValues'] = helper.values;

    return dynamoDoc.jsToDynamo(update, function(err, dynamoItem) {
      if (err) return callback(err);
      query['Item'] = dynamoItem;
      return callback(null, query);
    });

  };

module.exports = {
  EnergyQueryFactory: EnergyQueryFactory,
  getExpressionFragment: getExpressionFragment,
  getExpressionValue: getExpressionValue
}
