'use strict';

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

module.exports = {
  QueryHelper: QueryHelper
};
