/**
 * @author Massimiliano Izzo
 * @description unit tests
 */
/* jshint esnext: true */
var expect = require('chai').expect;
var _ = require('lodash');
var PostgresJSONBQueryStrategy = require('../lib/PostgresJSONBQueryStrategy.js');

var criteriaObj = {
    "dataType": 1,
    "model": "Data",
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

var caseInsensitiveCriteriaObj = {
    "dataType": 1,
    "model": "Data",
    "content": [{
        "comparator": "=",
        "fieldName": "name",
        "fieldType": "text",
        "fieldValue": "Aldebaran",
        "isList": false,
        "caseInsensitive": true
    }]
};

var booleanCriteriaObj = {
    "dataType": 1,
    "model": "Data",
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
    "dataType": 1,
    "model": "Data",
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
    "dataType": 7,
    "content": [
        {
        "comparator": "=",
        "fieldName": "gene_name",
        "fieldType": "text",
        "fieldValue": "Corf44",
        "isInLoop": true
    }
    ]
};

var loopListCriteriaObj = {
    "dataType": 7,
    "content": [
        {
        "comparator": "?&",
        "fieldName": "gene_name",
        "fieldType": "text",
        "fieldValue": ["MYCN","ALK","CD44","SOX4", "Corf44"],
        "isList": true,
        "isInLoop": true
    }
    ]
};

var sampleParamsObj = {"dataType":4,"model":"Sample","wantsSubject":true,"wantsPersonalInfo":true,"content":[{"specializedQuery":"Sample","biobank":1, "biobankComparator":"="},{"fieldName":"quantity","fieldType":"float","isList":false,"comparator":">=","fieldValue":"1.0","fieldUnit":"μg"},{"dataType":6,"model":"Data","content":[{"fieldName":"platform","fieldType":"text","isList":true,"comparator":"IN","fieldValue":["Agilent"]},{"fieldName":"array","fieldType":"text","isList":true,"comparator":"IN","fieldValue":["4x180K"]},{"dataType":7,"model":"Data","content":[{"fieldName":"genome","fieldType":"text","isList":true,"comparator":"IN","fieldValue":["hg19"]},{"dataType":8,"model":"Data","content":[{"fieldName":"chr","fieldType":"text","isList":true,"comparator":"IN","fieldValue":["chr11","chr17"]},{"fieldName":"is_amplification","fieldType":"boolean","isList":false,"comparator":"=","fieldValue":"true"}]}]}]}]};

var emptySampleObj = {
    "dataType": 2,
    "model": "Sample",
    "content": [
        {"specializedQuery": "Sample"},
        {}
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

        it("should return a containment (@>) clause with uppercase metadata value (case insensitive search)", function() {
            var i = 1;
            var previousOutput = {lastPosition: i, parameters: []};
            var res = this.strategy.getSubqueryRowAttribute(caseInsensitiveCriteriaObj.content[0], previousOutput, 'd.');
            expect(res.subquery).to.equal("d.metadata @> $"+ (i+1));
            expect(res.previousOutput).to.have.property("parameters");
            expect(res.previousOutput.parameters).to.eql(['{\"name\":{\"value\":\"ALDEBARAN\"}}']);
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

        it("should return a clause with the element exists [?] jsonb operator with case insensitive values", function() {
            var i = 1;
            var previousOutput = {lastPosition: i, parameters: []};
            var caseInsensitiveLoopRow = _.extend(_.clone(loopCriteriaObj.content[0]), {caseInsensitive: true});
            var res = this.strategy.getSubqueryRowLoop(caseInsensitiveLoopRow, previousOutput, 'd.');
            expect(res.subquery).to.equal("(d.metadata->$"+(++i)+"->'values' ? $"+(++i)+")");
            expect(res.previousOutput).to.have.property("parameters");
            expect(res.previousOutput.parameters).to.eql([loopCriteriaObj.content[0].fieldName, loopCriteriaObj.content[0].fieldValue.toUpperCase()]);
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

        it("should return a clause with the element exists all [?&] jsonb operator", function() {
            var i = 1;
            var previousOutput = {lastPosition: i, parameters: []};
            var res = this.strategy.getSubqueryRowLoop(loopListCriteriaObj.content[0], previousOutput, 'd.');
            expect(res.subquery).to.equal("(d.metadata->$"+(++i)+"->'values' ?& $"+(++i)+")");
            expect(res.previousOutput).to.have.property("parameters");
            expect(res.previousOutput.parameters).to.eql([loopListCriteriaObj.content[0].fieldName, loopListCriteriaObj.content[0].fieldValue]);
        });

        it("should return a clause with the element exists all [?&] jsonb operator (case insensitive)", function() {
            var i = 1;
            var previousOutput = {lastPosition: i, parameters: []};
            var caseInsensitiveLoopListRow = _.extend(_.cloneDeep(loopListCriteriaObj.content[0]), {caseInsensitive: true});
            var values = _.map(loopListCriteriaObj.content[0].fieldValue, el => el.toUpperCase());
            var res = this.strategy.getSubqueryRowLoop(caseInsensitiveLoopListRow, previousOutput, 'd.');
            expect(res.subquery).to.equal("(d.metadata->$"+(++i)+"->'values' ?& $"+(++i)+")");
            expect(res.previousOutput).to.have.property("parameters");
            expect(res.previousOutput.parameters).to.eql([loopListCriteriaObj.content[0].fieldName, values]);
        });

        it("should return a clause with the element exists any [?|] jsonb operator", function() {
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
            var selectStatement = "SELECT id, parent_subject, parent_sample, parent_data FROM data d";
            var whereClause = "WHERE d.type = $1 AND (" +
                "(d.metadata @> $2) AND (d.metadata @> $3 OR d.metadata @> $4 OR d.metadata @> $5) AND " +
                "((d.metadata->$6->>'value')::float >= $7 AND " + "d.metadata @> $8) AND " +
                "((d.metadata->$9->>'value')::integer > $10 AND " + "d.metadata @> $11))";
            var parameters = [ criteriaObj.dataType,
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
            var selectStatement = "SELECT id, parent_subject, parent_sample, parent_data FROM data d";
            var whereClause = "WHERE d.type = $1 AND (" +
                "(NOT d.metadata @> $2) AND (NOT d.metadata @> $3 OR NOT d.metadata @> $4 OR NOT d.metadata @> $5) AND " +
                "((d.metadata->$6->>'value')::float >= $7 AND " + "d.metadata @> $8) AND " +
                "((d.metadata->$9->>'value')::integer > $10 AND " + "d.metadata @> $11))";
            var parameters = [ criteriaObj.dataType,
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

            var selectStatement = "SELECT id, parent_subject, parent_sample, parent_data FROM data d";
            var whereClause = "WHERE d.type = $1 AND ((d.metadata @> $2) AND (d.metadata @> $3))";
            var parameters = [booleanStringCriteriaObj.dataType,
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

            var selectStatement = "SELECT id, parent_subject, parent_sample, parent_data FROM data d";
            var whereClause = "WHERE d.type = $1 AND ((d.metadata @> $2) AND (d.metadata @> $3))";
            var parameters = [booleanCriteriaObj.dataType,
                '{\"is_neutron_star\":{\"value\":true}}', '{\"is_black_hole\":{\"value\":false}}'];
                var parameteredQuery = this.strategy.composeSingle(booleanCriteriaObj);
                expect(parameteredQuery).to.have.property('select');
                expect(parameteredQuery).to.have.property('where');
                expect(parameteredQuery).to.have.property('previousOutput');
                expect(parameteredQuery.select).to.equal(selectStatement);
                expect(parameteredQuery.where).to.equal(whereClause);
                expect(parameteredQuery.previousOutput.parameters).to.eql(parameters);

        });

        it("compose a query with a loop array condition", function() {

            var selectStatement = "SELECT id, parent_subject, parent_sample, parent_data FROM data d";
            var whereClause = "WHERE d.type = $1 AND (((d.metadata->$2->'values' " + loopListCriteriaObj.content[0].comparator + " $3)))";
            var parameters = [loopListCriteriaObj.dataType, loopListCriteriaObj.content[0].fieldName, loopListCriteriaObj.content[0].fieldValue];
            var parameteredQuery = this.strategy.composeSingle(loopListCriteriaObj);
            expect(parameteredQuery).to.have.property('select');
            expect(parameteredQuery).to.have.property('where');
            expect(parameteredQuery).to.have.property('previousOutput');
            expect(parameteredQuery.select).to.equal(selectStatement);
            expect(parameteredQuery.where).to.equal(whereClause);
            expect(parameteredQuery.previousOutput.parameters).to.eql(parameters);

        });
        
        /*
        it("compose a query with a loop array condition", function() {

            var selectStatement = "SELECT id, parent_subject, parent_sample, parent_data FROM data d";
            var whereClause = "WHERE d.type = $1 AND (((d.metadata->$2->'values' " + loopListCriteriaObj.content[0].comparator + " $3)))";
            var parameters = [loopListCriteriaObj.dataType, loopListCriteriaObj.content[0].fieldName, 
                _.map(loopListCriteriaObj.content[0].fieldValue, elem => elem.toUpperCase())];
            loopCriteriaObj.content[0].caseInsensitive = true;
            var parameteredQuery = this.strategy.composeSingle(loopListCriteriaObj);
            expect(parameteredQuery).to.have.property('select');
            expect(parameteredQuery).to.have.property('where');
            expect(parameteredQuery).to.have.property('previousOutput');
            expect(parameteredQuery.select).to.equal(selectStatement);
            expect(parameteredQuery.where).to.equal(whereClause);
            expect(parameteredQuery.previousOutput.parameters).to.eql(parameters);

        }); */

    });

    describe("#compose", function() {

        it("composes a query from a nested sample criteria object", function() {
            var query = this.strategy.compose(sampleParamsObj);
            var commonTableExpr = [
                "WITH s AS (SELECT id, code, sex, personal_info FROM subject), ",
                "pd AS (SELECT id, given_name, surname, birth_date FROM personal_details), ",
                "bb AS (SELECT id, biobank_id, acronym, name FROM biobank), ",
                "nested_1 AS (SELECT id, parent_subject, parent_sample, parent_data FROM data ",
                "WHERE type = $6 AND ((metadata @> $7) AND (metadata @> $8))), ",
                "nested_2 AS (SELECT id, parent_subject, parent_sample, parent_data FROM data WHERE type = $9 AND ((metadata @> $10))), ",
                "nested_3 AS (SELECT id, parent_subject, parent_sample, parent_data FROM data ",
                "WHERE type = $11 AND ((metadata @> $12 OR metadata @> $13) AND (metadata @> $14)))"
            ].join("");
            var mainQuery = [
                "SELECT DISTINCT d.id, d.biobank, d.biobank_code, s.code, s.sex, pd.given_name, pd.surname, pd.birth_date, bb.acronym AS biobank_acronym, d.metadata FROM sample d ",
                "LEFT JOIN s ON s.id = d.parent_subject ",
                "LEFT JOIN pd ON pd.id = s.personal_info ",
                "LEFT JOIN bb ON bb.id = d.biobank ",
                "INNER JOIN nested_1 ON nested_1.parent_sample = d.id ",
                "INNER JOIN nested_2 ON nested_2.parent_data = nested_1.id ",
                "INNER JOIN nested_3 ON nested_3.parent_data = nested_2.id ",
                "WHERE d.type = $1 AND ((d.biobank = $2) AND ((d.metadata->$3->>'value')::float >= $4 AND d.metadata @> $5));"
            ].join("");
            expect(query).to.have.property('statement');
            expect(query).to.have.property('parameters');
            expect(query.statement).to.equal(commonTableExpr + " " + mainQuery);
            console.log(query.parameters);
            expect(query.parameters).to.have.length(14);
        });

        it("composes a query from an empty sample criteria (containing an empty specialized criteria)", function() {
            var query = this.strategy.compose(emptySampleObj);
            var expectedStatement = ["WITH bb AS (SELECT id, biobank_id, acronym, name FROM biobank) ",
                "SELECT DISTINCT d.id, d.biobank, d.biobank_code, bb.acronym AS biobank_acronym, d.metadata FROM sample d ",
                "LEFT JOIN bb ON bb.id = d.biobank ", 
                "WHERE d.type = $1;"].join("");
            expect(query.statement).to.equal(expectedStatement);
            expect(query.parameters).to.eql([emptySampleObj.dataType]);
        });

    });

});
