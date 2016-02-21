'use strict';

module.exports.simpleClone = function simpleClone(obj) {
  if (obj === null) return null;
  if (typeof obj !== 'object') return null;
  var newObj = {};
  for (var prop in obj) {
    newObj[prop] = obj[prop];
  }
  return newObj;
}
