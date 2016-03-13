'use strict';

var EnergyQueryFactory = function(table) {
  this.queryBase = table.queryBase;
  this.tableHashKey = table.hashKey;
  this.tableRangeKey = table.rangeKey;
};

EnergyQueryFactory.prototype.getQuery = function getQuery(doc, options, callback) {
  require('./energy-query/query')
    .getBuilder()
    .configure({
      base: this.queryBase,
      options: options,
      hashKey: this.tableHashKey,
      rangeKey: this.tableRangeKey,
    })
    .build(doc, callback);
};

EnergyQueryFactory.prototype.getInsertQuery = function getInsertQuery(doc, options, callback) {
  require('./energy-query/insert')
    .getBuilder()
    .configure({
      base: this.queryBase,
      options: options,
      hashKey: this.tableHashKey,
      rangeKey: this.tableRangeKey,
    })
    .build(doc, callback);
};

EnergyQueryFactory.prototype.getScanQuery = function getScanQuery(doc, options, callback) {
  require('./energy-query/scan')
    .getBuilder()
    .configure({
      base: this.queryBase,
      options: options,
      hashKey: this.tableHashKey,
      rangeKey: this.tableRangeKey,
    })
    .build(doc, callback);
};

EnergyQueryFactory.prototype.getDeleteQuery = function getQuery(doc, options, callback) {
  require('./energy-query/delete')
    .getBuilder()
    .configure({
      base: this.queryBase,
      options: options,
      hashKey: this.tableHashKey,
      rangeKey: this.tableRangeKey,
    })
    .build(doc, callback);
};

EnergyQueryFactory.prototype.getUpdateQuery =
  function getUpdateQuery(doc, update, options, callback) {
    require('./energy-query/update')
      .getBuilder()
      .configure({
        base: this.queryBase,
        options: options,
        hashKey: this.tableHashKey,
        rangeKey: this.tableRangeKey,
      })
      .build(doc, update, callback);
  };

EnergyQueryFactory.prototype.getReplaceQuery =
  function getReplaceQuery(doc, update, options, callback) {
    require('./energy-query/replace')
      .getBuilder()
      .configure({
        base: this.queryBase,
        options: options,
        hashKey: this.tableHashKey,
        rangeKey: this.tableRangeKey,
      })
      .build(doc, update, callback);
  };

module.exports = {
  EnergyQueryFactory: EnergyQueryFactory
}
