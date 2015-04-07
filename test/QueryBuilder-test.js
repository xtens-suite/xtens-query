var expect = require('chai').expect;
var sinon = require('sinon');
var QueryBuilder = require('../lib/QueryBuilder');
var PostgresJSONQueryStrategy = require('../lib/PostgresJSONQueryStrategy.js');
var PostgresJSONBQueryStrategy = require('../lib/PostgresJSONQueryStrategy.js');

describe('#QueryBuilder', function() {
    
    describe('#constructor', function() {
        
        it('should have a strategy property with a compose method', function() {
            var builder = new QueryBuilder();
            expect(builder).to.have.property('strategy');
            expect(builder.strategy).to.be.an.instanceof(PostgresJSONBQueryStrategy);
        });

    });

    describe('#compose', function() {
        
        beforeEach(function() {
            this.builder = new QueryBuilder();
            sinon.spy(this.builder.strategy, "compose");
        });

        it('should call the compose method of the strategy object', function() {
            var params = { pivotDataType: {id: 1, name:'Test'}};
            this.builder.compose(params);
            expect(this.builder.strategy.compose.calledOnce);
        });

    });

});
