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

var booleanCriteriaObj = {
    "pivotDataType": 1,
    "content": [
    {
        "comparator": "=",
        "fieldName": "is_neutron_star",
        "fieldType": "boolean",
        "fieldValue": true,
        "isList": false
    },
    {
        "comparator": "=",
        "fieldName": "is_black_hole",
        "fieldType": "boolean",
        "fieldValue": false
    }
    ]
};

var booleanStringCriteriaObj = {
    "pivotDataType": 1,
    "content": [
    {
        "comparator": "=",
        "fieldName": "is_neutron_star",
        "fieldType": "boolean",
        "fieldValue": "true",
        "isList": false
    },
    {
        "comparator": "=",
        "fieldName": "is_black_hole",
        "fieldType": "boolean",
        "fieldValue": "false"
    }
    ]
};

var loopCriteriaObj = {
    "pivotDataType": 7,
    "content": [
        {
            "comparator": "=",
            "fieldName": "gene_name",
            "fieldType": "text",
            "fieldValue": "MYCN",
            "isInLoop": true
        }
    ]
};

var loopListCriteriaObj = {
    "pivotDataType": 7,
    "content": [
        {
            "comparator": "?&",
            "fieldName": "gene_name",
            "fieldType": "text",
            "fieldValue": ["MYCN","ALK","CD44","SOX4"],
            "isList": true,
            "isInLoop": true
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

    describe("#getSubqueryRowAttribute", function() {

        it("should return a clause with a containment operator", function() {
            var i = 1;
            var previousOutput = {lastPosition: i, parameters: []};
            var res = this.strategy.getSubqueryRowAttribute(criteriaObj.content[0], previousOutput, 'd.');
            expect(res.subquery).to.equal("d.metadata @> $"+ (i+1));
            expect(res.previousOutput).to.have.property("parameters");
            expect(res.previousOutput.parameters).to.eql(['{\"constellation\":{\"value\":\"cepheus\"}}']);
        });
        
    });

    describe("#getSubqueryRowLoop", function() {
        
        it("should return a clause with the element exists [?] jsonb operator", function() {
            var i = 1;
            var previousOutput = {lastPosition: i, parameters: []};
            var res = this.strategy.getSubqueryRowLoop(loopCriteriaObj.content[0], previousOutput, 'd.');
            expect(res.subquery).to.equal("(d.metadata->$"+(++i)+"->'values' ? $"+(++i)+")");
            expect(res.previousOutput).to.have.property("parameters");
            expect(res.previousOutput.parameters).to.eql([loopCriteriaObj.content[0].fieldName, loopCriteriaObj.content[0].fieldValue]);
        });

        it("should return a clause with the element exists [?] jsonb operator with NOT condition", function() {
            var i = 1;
            var previousOutput = {lastPosition: i, parameters: []};
            loopCriteriaObj.content[0].comparator = '<>';
            var res = this.strategy.getSubqueryRowLoop(loopCriteriaObj.content[0], previousOutput, 'd.');
            expect(res.subquery).to.equal("(NOT d.metadata->$"+(++i)+"->'values' ? $"+(++i)+")");
            expect(res.previousOutput).to.have.property("parameters");
            expect(res.previousOutput.parameters).to.eql([loopCriteriaObj.content[0].fieldName, loopCriteriaObj.content[0].fieldValue]);
        });
        
        it("should return a clause with the element exists [?] jsonb operator", function() {
            var i = 1;
            var previousOutput = {lastPosition: i, parameters: []};
            var res = this.strategy.getSubqueryRowLoop(loopListCriteriaObj.content[0], previousOutput, 'd.');
            expect(res.subquery).to.equal("(d.metadata->$"+(++i)+"->'values' ?& $"+(++i)+")");
            expect(res.previousOutput).to.have.property("parameters");
            expect(res.previousOutput.parameters).to.eql([loopListCriteriaObj.content[0].fieldName, loopListCriteriaObj.content[0].fieldValue]);
        });

        it("should return a clause with the element exists [?] jsonb operator", function() {
            var i = 1;
            var previousOutput = {lastPosition: i, parameters: []};
            loopListCriteriaObj.content[0].comparator = '?|';
            var res = this.strategy.getSubqueryRowLoop(loopListCriteriaObj.content[0], previousOutput, 'd.');
            expect(res.subquery).to.equal("(d.metadata->$"+(++i)+"->'values' ?| $"+(++i)+")");
            expect(res.previousOutput).to.have.property("parameters");
            expect(res.previousOutput.parameters).to.eql([loopListCriteriaObj.content[0].fieldName, loopListCriteriaObj.content[0].fieldValue]);
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

        it("compose a query with two boolean fields (from string)", function() {
            
            var selectStatement = "SELECT * FROM data d";
            var whereClause = "WHERE d.type = $1 AND ((d.metadata @> $2) AND (d.metadata @> $3))";
            var parameters = [booleanStringCriteriaObj.pivotDataType,
            '{\"is_neutron_star\":{\"value\":true}}', '{\"is_black_hole\":{\"value\":false}}'];
            var parameteredQuery = this.strategy.composeSingle(booleanStringCriteriaObj);
            expect(parameteredQuery).to.have.property('select');
            expect(parameteredQuery).to.have.property('where');
            expect(parameteredQuery).to.have.property('previousOutput');
            expect(parameteredQuery.select).to.equal(selectStatement);
            expect(parameteredQuery.where).to.equal(whereClause);
            expect(parameteredQuery.previousOutput.parameters).to.eql(parameters);

        });

        it("compose a query with two boolean fields (from boolean)", function() {
            
            var selectStatement = "SELECT * FROM data d";
            var whereClause = "WHERE d.type = $1 AND ((d.metadata @> $2) AND (d.metadata @> $3))";
            var parameters = [booleanCriteriaObj.pivotDataType,
            '{\"is_neutron_star\":{\"value\":true}}', '{\"is_black_hole\":{\"value\":false}}'];
            var parameteredQuery = this.strategy.composeSingle(booleanCriteriaObj);
            expect(parameteredQuery).to.have.property('select');
            expect(parameteredQuery).to.have.property('where');
            expect(parameteredQuery).to.have.property('previousOutput');
            expect(parameteredQuery.select).to.equal(selectStatement);
            expect(parameteredQuery.where).to.equal(whereClause);
            expect(parameteredQuery.previousOutput.parameters).to.eql(parameters);

        });

    });

});
