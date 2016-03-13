[![Build Status](https://travis-ci.org/schrodervictor/energy-db.svg?branch=master)](https://travis-ci.org/schrodervictor/energy-db)

energy-db
=========

What is a dynamo without energy???

AWS DynamoDB is great, but it can be very annoying to work with bare hands.
Our EnergyDB is a wrapper around DynamoDB, aiming to make it easier to use
this managed cloud database, specially for those used to the way MongoDB works.

This still an ALPHA version! Lots of things need to be improved.

## Instalation

If you are reading you know how to install a npm module. Just put it in your
dependencies list or install it globally if you are one of those people.

## How to use it

The basics. EnergyDB needs to use DynamoDB. You have two options: you may pass
a DynamoDB instance to EnergyDB or simply provide the credentials and let us
instantiate it for you:

```javascript
var energyDB = require('energy-db');

var settings = {
  region: 'eu-central-1',
  accessKeyId: 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA',
  secretAccessKey: 'vvvvvvvvvvvvvvvvvvvvvvvvvvvvvv'
};

energyDB.connect(settings, function(err, db) {
  // energy-db is ready to use!!
});
```

Once you have the EnergyDb instance, you can get access to tables and perform
operations:

```javascript
db.table('Your-Table-Name', function(err, table) {

  var doc = {
    'key-0': 'value-0',
    'key-1': 12345
  };

  table.putItem(doc[, options], callback);
  // or
  table.query(doc[, options], callback);
  // or
  table.delete(doc[, options], callback);
  // or
  table.update(doc, update[, options], callback);
});

```

You don't have to bother about converting the object to the DynamoDB
strong-typed format. EnergyDB uses DynamoDoc internally to make this
convertion for you.

The EnergyTable also supports a number of additional settings, that you can
define before performing the queries. For example, you may want to have the
information about the consumed read/write capacity or have the old values of
a document you are updating. In this case, you can do something like this:

```javascript
db.table('Your-Table-Name', function(err, table) {

  // The following settings will affect all queries done with this
  // EnergyTable instance.
  table.returnConsumedCapacity().returnOldValues();

  // perform your queries...

  // Another option is to pass an additional 'options' object
  // to the method. This will affect only the query in question.
  table.query(doc, {ConsistentRead: true}, callback);

});

```

As usual, I suggest you to take a look on the code and the tests, if you want
to do something more complex.

## Unit tests

This module was TDD'ed and we have good test coverage using mocha+chai. We
believe all core functionality is covered, but some error cases are not. We
are working to handle these scenarios.

## Contributing

Fork the repo, create a branch, do awesome additions and submit a
pull-request. Only PR's with tests will be considered.

## Releases

* 0.0.6:

  * Improves code organization
  * Adds the 'options' as an optional argument when performing queries
  * Adds support to return values as plain js object to the update method
  * Some clean up


* 0.0.5:

  * adds final validation to the query factory to prevent invalid syntax and
    the use of deprecated operators


* 0.0.4:

  * adds full-scan functionality


* 0.0.3:

  * adds support to update and replace operations
  * new structure for unit tests folder
  * better code organization
  * EnergyQuery is now EnergyQueryFactory
  * insert queries now uses EnergyQueryFactory as well


* 0.0.2:

  * bugfix with new dynamo-doc version


* 0.0.1

  * Initial alpha release
  * support to query, insert and delete operations
