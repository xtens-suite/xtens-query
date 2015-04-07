/**
 * @author Massimiliano Izzo
 * @description this builder works as a context for the query/search strategy
 */
var PostgresJSONQueryStrategy = require('./PostgresJSONQueryStrategy');
var PostgresJSONBQueryStrategy = require('./PostgresJSONBQueryStrategy');

function QueryBuilder(strategy) {
    if (!strategy) {
        strategy = new PostgresJSONBQueryStrategy();
    }
    this.setStrategy(strategy);
}

QueryBuilder.prototype = {
    
    setStrategy: function(strategy) {
        this.strategy = strategy;
    },

    compose: function(queryParams) {
        return this.strategy.compose(queryParams);
    }

};

module.exports = QueryBuilder;
