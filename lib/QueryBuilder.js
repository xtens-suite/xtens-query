/**
 * @author Massimiliano Izzo
 * @description this builder works as a context for the query/search strategy
 */
var PostgresJSONQueryStrategy = require('./PostgresJSONQueryStrategy');

function QueryBuilder(strategy) {
    if (!strategy) {
        strategy = new PostgresJSONQueryStrategy();
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
