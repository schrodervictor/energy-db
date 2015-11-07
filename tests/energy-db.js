var expect = require('chai').expect;
var energyDB = require('../lib/energy-db');
var EnergyDB = energyDB.EnergyDB;

describe('energy-db', function() {
    describe('#connect(settings, callback)', function() {
        it('should pass an EnergyDB instance to the callback', function(done) {
            energyDB.connect({}, function(err, db) {
                if (err) return done(err);
                expect(db).to.be.an.instanceOf(EnergyDB);
                done();
            });
        });
    });
});
