/**
 * @author Massimiliano Izzo
 * @description unit tests
 */
var expect = require('chai').expect;
var _ = require('lodash');
var PostgresJSONBQueryStrategy = require('../lib/PostgresJSONBQueryStrategy.js');

var criteriaObj = {
    "pivotDataType": 1,
    "content": [
        {
        "fieldName": "constellation",
        "fieldType": "text",
        "comparator": "=",
        "fieldValue": "cepheus",
        "isList": false
    },
    {
        "fieldName": "type", // the stellar type
        "fieldType": "text",
        "comparator": "IN",
        "fieldValue": ["hypergiant","supergiant","main-sequence star"],
        "isList": true
    },
    {
        "fieldName": "mass",
        "fieldType": "float",
        "comparator": ">=",
        "fieldValue": "1.5",
        "fieldUnit": "M☉"
    },
    {
        "comparator": ">",
        "fieldName": "distance",
        "fieldType": "integer",
        "fieldUnit": "pc",
        "fieldValue": "50"
    }
    ]
};

describe("QueryStrategy.PostgresJSONB", function() {

    before(function() {
        this.strategy = new PostgresJSONBQueryStrategy();
    });

    describe("#getSubqueryRow", function() {

        it("should return a clause with a containment operator", function() {
            var i = 1;
            var previousOutput = {lastPosition: i, parameters: []};
            var res = this.strategy.getSubqueryRow(criteriaObj.content[0], previousOutput, 'd.');
            expect(res.subquery).to.equal("d.metadata @> $"+ (i+1));
            expect(res.previousOutput).to.have.property("parameters");
            expect(res.previousOutput.parameters).to.eql(['{\"constellation\":{\"value\":\"cepheus\"}}']);
        });
        
    });

    describe("#composeSingle", function() {
        
        it("compose a query from criteria with positive matching and range conditions on nonrecursive fields", function() {
            var parameteredQuery = this.strategy.composeSingle(criteriaObj);
            var selectStatement = "SELECT * FROM data d";
            var whereClause = "WHERE d.type = $1 AND (" +
                "(d.metadata @> $2) AND (d.metadata @> $3 OR d.metadata @> $4 OR d.metadata @> $5) AND " +
                "((d.metadata->$6->>'value')::float >= $7 AND " + "d.metadata @> $8) AND " +
                "((d.metadata->$9->>'value')::integer > $10 AND " + "d.metadata @> $11))";
            var parameters = [ criteriaObj.pivotDataType,
                '{\"constellation\":{\"value\":\"cepheus\"}}', '{\"type\":{\"value\":\"hypergiant\"}}',
                '{\"type\":{\"value\":\"supergiant\"}}', '{\"type\":{\"value\":\"main-sequence star\"}}',
                criteriaObj.content[2].fieldName, criteriaObj.content[2].fieldValue, '{\"mass\":{\"unit\":\"M☉\"}}',
                criteriaObj.content[3].fieldName, criteriaObj.content[3].fieldValue, '{\"distance\":{\"unit\":\"pc\"}}'
            ];
            
            expect(parameteredQuery).to.have.property('select');
            expect(parameteredQuery).to.have.property('where');
            expect(parameteredQuery).to.have.property('previousOutput');
            expect(parameteredQuery.select).to.equal(selectStatement);
            expect(parameteredQuery.where).to.equal(whereClause);
            expect(parameteredQuery.previousOutput.parameters).to.eql(parameters);
        
        });

        it("compose a query from criteria with exclusion matching and range conditions on nonrecursive fields", function() {
            
            criteriaObj.content[0].comparator = "<>";
            criteriaObj.content[1].comparator = "NOT IN";
            var selectStatement = "SELECT * FROM data d";
            var whereClause = "WHERE d.type = $1 AND (" +
                "(NOT d.metadata @> $2) AND (NOT d.metadata @> $3 OR NOT d.metadata @> $4 OR NOT d.metadata @> $5) AND " +
                "((d.metadata->$6->>'value')::float >= $7 AND " + "d.metadata @> $8) AND " +
                "((d.metadata->$9->>'value')::integer > $10 AND " + "d.metadata @> $11))";
            var parameters = [ criteriaObj.pivotDataType,
                '{\"constellation\":{\"value\":\"cepheus\"}}', '{\"type\":{\"value\":\"hypergiant\"}}',
                '{\"type\":{\"value\":\"supergiant\"}}', '{\"type\":{\"value\":\"main-sequence star\"}}',
                criteriaObj.content[2].fieldName, criteriaObj.content[2].fieldValue, '{\"mass\":{\"unit\":\"M☉\"}}',
                criteriaObj.content[3].fieldName, criteriaObj.content[3].fieldValue, '{\"distance\":{\"unit\":\"pc\"}}'
            ];
            var parameteredQuery = this.strategy.composeSingle(criteriaObj);
            expect(parameteredQuery).to.have.property('select');
            expect(parameteredQuery).to.have.property('where');
            expect(parameteredQuery).to.have.property('previousOutput');
            expect(parameteredQuery.select).to.equal(selectStatement);
            expect(parameteredQuery.where).to.equal(whereClause);
            expect(parameteredQuery.previousOutput.parameters).to.eql(parameters);

        });

    });


});
