/**
 * @author Massimiliano Izzo
 * @description unit test 
 */
var expect = require('chai').expect;
var sinon = require('sinon');
var _ = require("lodash");
var DataTypeClasses = require("../lib/Utils").DataTypeClasses;
var PostgresJSONQueryStrategy = require('../lib/PostgresJSONQueryStrategy');

describe("QueryStrategy.PostgresJSON", function() {

    var criteriaRowWithSQLInjection = {
        "fieldName":"Diagnosis",
        "fieldType":"text",
        "isList":true,
        "comparator":">= 0; DROP table data;",
        "fieldValue":["Neuroblastoma"]
    };

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

    var nestedParamsObj = {
        "dataType":1,
        "model": "Subject",
        "content":[{
            "fieldName":"Diagnosis Age",
            "fieldType":"integer",
            "isList":false,
            "comparator":"<=",
            "fieldValue":"365",
            "fieldUnit":"days"
        },{
            "fieldName":"Overall Status",
            "fieldType":"text",
            "isList":true,
            "comparator":"IN",
            "fieldValue":["Diseased"]
        },{
            "dataType":2,
            "model":"Sample",
            "content":[{
                "fieldName":"Diagnosis",
                "fieldType":"text",
                "isList":true,
                "comparator":"IN",
                "fieldValue":["Neuroblastoma"]
            },{
                "dataType":6,
                "model":"Sample",
                "content":[{
                    "fieldName":"quantity",
                    "fieldType":"float",
                    "isList":false,
                    "comparator":">=",
                    "fieldValue":"1.0",
                    "fieldUnit":"μl"
                },{
                    "dataType":3,
                    "model":"Data",
                    "content":[{
                        "fieldName":"Overall Result",
                        "fieldType":"text",
                        "isList":true,
                        "comparator":"IN",
                        "fieldValue":["SCA","NCA"]
                    }]
                }]
            },{
                "dataType":7,
                "model":"Sample",
                "content":[{
                    "fieldName":"quantity",
                    "fieldType":"float",
                    "isList":false,
                    "comparator":">=",
                    "fieldValue":"1.2",
                    "fieldUnit":"µg"
                },{
                    "dataType":8,
                    "model":"Data",
                    "content":[{
                        "fieldName":"hypoxia signature",
                        "fieldType":"text",
                        "isList":true,
                        "comparator":"IN",
                        "fieldValue":["high"]
                    }]
                }]
            }]
        }]
    };

    var subjectParamsObj = {
        "dataType":1,
        "model":"Subject",
        "content": [
            {
            "personalDetails":true,
            "surnameComparator":"LIKE",
            "surname":"Pizzi",
            "givenNameComparator":"NOT LIKE",
            "givenName":"Pippo",
            "birthDateComparator":"="
        },{
            "specializedQuery":"Subject",
            "codeComparator":"LIKE",
            "code":"PAT002"
        },{
            "specializedQuery":"Subject",
            "sexComparator":"IN",
            "sex":["F","M"]
        },{
            "fieldName":"overall_status",
            "fieldType":"text",
            "isList":true,
            "comparator":"IN",
            "fieldValue":["Diseased","Deceased"]
        },{
            "dataType":2,
            "model":"Sample",
            "content":[
                {
                "specializedQuery":"Sample",
                "biobankCodeComparator":"LIKE",
                "biobankCode":"SAMPOO1"
            },{
                "fieldName":"Diagnosis",
                "fieldType":"text",
                "isList":true,
                "comparator":"IN",
                "fieldValue":["Neuroblastoma"]
            }]
        }
        ]
    };

    before(function() {
        this.strategy = new PostgresJSONQueryStrategy();
    });

    describe("#getSubqueryRow", function() {
        it("#should throw an error if a comparator is not allowed (SQL injection)", function() {
            expect(this.strategy.getSubqueryRow.bind(this.strategy.getSubqueryRow, criteriaRowWithSQLInjection)).to.throw(
                "Operation not allowed. Trying to inject a forbidden comparator!!"
            );
        });
    });

    describe("#composeSpecializedPersonalDetailsQuery", function() {
        it("composes a query from a criteria object containing specialized fields on subject and personal details", function() {
            var pdProperties = subjectParamsObj.content[0];
            var parameteredQuery = this.strategy.composeSpecializedPersonalDetailsQuery(pdProperties);
            var selectStatement = "SELECT id, given_name, surname, birth_date FROM personal_details";
            var subquery = "pd.surname "+pdProperties.surnameComparator+" $1 AND pd.given_name "+pdProperties.givenNameComparator+" $2";
            var parameters = [pdProperties.surname.toUpperCase(), pdProperties.givenName.toUpperCase()];
            expect(parameteredQuery).to.have.property('select');
            expect(parameteredQuery).to.have.property('where');
            expect(parameteredQuery).to.have.property('previousOutput');
            expect(parameteredQuery.select).to.equal(selectStatement);
            expect(parameteredQuery.subquery).to.equal(subquery);
            expect(parameteredQuery.previousOutput.parameters).to.eql(parameters);
        });
    });

    describe("#composeSpecializedQuery", function() {
        it("composes a query from a criteria object containing specialized fields on subject (code)", function() {
            var subjProperties = subjectParamsObj.content[1];
            var parameteredQuery = this.strategy.composeSpecializedQuery(subjProperties, {}, "d.");
            var subquery = "d.code LIKE $1";
            expect(parameteredQuery).to.have.property('subquery');
            expect(parameteredQuery.subquery).to.equal(subquery);
            // TODO add parameters check into the array
        });
            
        it("composes a query from a criteria object containing specialized fields on subject (sex)", function() {
            var subjSex = subjectParamsObj.content[2];
            var parameteredQuery = this.strategy.composeSpecializedQuery(subjSex, { parameters: [subjectParamsObj.dataType]}, "d.");
            var subquery = "d.sex IN ($2,$3)";
            expect(parameteredQuery.parameters).to.eql(_.flatten([subjectParamsObj.dataType, subjectParamsObj.content[2].sex]));
        });

    });

    describe("#composeSingle", function() {

        it("composes a query from a criteria object containing only nonrecursive fields", function() {
            var parameteredQuery = this.strategy.composeSingle(criteriaObj);
            var selectStatement = "SELECT id, parent_subject, parent_sample, parent_data FROM data d";
            var whereClause = "WHERE d.type = $1 AND (" +
                "((d.metadata->$2->>'value')::text = $3) AND " +
                "((d.metadata->$4->>'value')::text IN ($5,$6,$7)) AND " +
                "((d.metadata->$8->>'value')::float >= $9 AND " + "(d.metadata->$8->>'unit')::text LIKE $10) AND " +
                "((d.metadata->$11->>'value')::integer > $12 AND " + "(d.metadata->$11->>'unit')::text LIKE $13))";
            var parameters = [ criteriaObj.dataType, 
                criteriaObj.content[0].fieldName, criteriaObj.content[0].fieldValue,
                criteriaObj.content[1].fieldName, criteriaObj.content[1].fieldValue[0], 
                criteriaObj.content[1].fieldValue[1], criteriaObj.content[1].fieldValue[2], 
                criteriaObj.content[2].fieldName, criteriaObj.content[2].fieldValue, criteriaObj.content[2].fieldUnit,
                criteriaObj.content[3].fieldName, criteriaObj.content[3].fieldValue, criteriaObj.content[3].fieldUnit 
            ];
            expect(parameteredQuery).to.have.property('select');
            expect(parameteredQuery).to.have.property('where');
            expect(parameteredQuery).to.have.property('previousOutput');
            expect(parameteredQuery.select).to.equal(selectStatement);
            expect(parameteredQuery.where).to.equal(whereClause);
            expect(parameteredQuery.previousOutput.parameters).to.eql(parameters);
            expect(parameteredQuery.previousOutput.lastPosition).to.equal(13);
        });

        it("composes a query from a criteria object containing specialized fields on subject and personal details", function() {
            var commonTableExpr = [
                "SELECT id, given_name, surname, birth_date FROM personal_details pd WHERE pd.surname NOT LIKE "
            ]; 
        });

        it("composes a set of queries from a nested criteria object", function() {
            var commonTableExpressions = [
                "SELECT id, parent_subject, parent_sample, parent_data FROM data ",
                "WHERE type = $14 AND (((metadata->$15->>'value')::text IN ($16,$17)))", //CGH

                "SELECT id, biobank_code, parent_subject, parent_sample FROM sample ",
                "WHERE type = $10 AND (((metadata->$11->>'value')::float >= $12 AND (metadata->$11->>'unit')::text LIKE $13))",
                
                "SELECT id, parent_subject, parent_sample, parent_data FROM data ",
                "WHERE type = $22 AND (((metadata->$23->>'value')::text IN ($24)))", // Microarray
                
                "SELECT id, biobank_code, parent_subject, parent_sample FROM sample ",
                "WHERE type = $18 AND (((metadata->$19->>'value')::float >= $20 AND (metadata->$19->>'unit')::text LIKE $21))",
                
                "SELECT id, biobank_code, parent_subject, parent_sample FROM sample WHERE type = $7 AND (((metadata->$8->>'value')::text IN ($9)))"
            ];

            var selectStatement = "SELECT id, code, sex FROM subject d"; 
            var whereClause = "WHERE d.type = $1 AND (((d.metadata->$2->>'value')::integer <= $3 "; 
            whereClause += "AND (d.metadata->$2->>'unit')::text LIKE $4) AND ((d.metadata->$5->>'value')::text IN ($6)))";
            var parameters = [ nestedParamsObj.dataType,
                nestedParamsObj.content[0].fieldName, nestedParamsObj.content[0].fieldValue, nestedParamsObj.content[0].fieldUnit, // Subject
                nestedParamsObj.content[1].fieldName, nestedParamsObj.content[1].fieldValue[0],
                nestedParamsObj.content[2].dataType, nestedParamsObj.content[2].content[0].fieldName, //Tissue
                nestedParamsObj.content[2].content[0].fieldValue[0], 
                nestedParamsObj.content[2].content[1].dataType, nestedParamsObj.content[2].content[1].content[0].fieldName, // DNA Sample
                nestedParamsObj.content[2].content[1].content[0].fieldValue, nestedParamsObj.content[2].content[1].content[0].fieldUnit,
                nestedParamsObj.content[2].content[1].content[1].dataType, // CGH
                nestedParamsObj.content[2].content[1].content[1].content[0].fieldName,
                nestedParamsObj.content[2].content[1].content[1].content[0].fieldValue[0], 
                nestedParamsObj.content[2].content[1].content[1].content[0].fieldValue[1],
                nestedParamsObj.content[2].content[2].dataType, nestedParamsObj.content[2].content[2].content[0].fieldName, // RNA Sample
                nestedParamsObj.content[2].content[2].content[0].fieldValue, nestedParamsObj.content[2].content[2].content[0].fieldUnit,
                nestedParamsObj.content[2].content[2].content[1].dataType, // Microarray
                nestedParamsObj.content[2].content[2].content[1].content[0].fieldName,
                nestedParamsObj.content[2].content[2].content[1].content[0].fieldValue[0] 
            ];
            console.log(parameters);
            console.log(parameters.length);
            var nestedParameteredQuery = this.strategy.composeSingle(nestedParamsObj);
            var res = this.strategy.composeCommonTableExpression(nestedParameteredQuery);
            console.log(nestedParameteredQuery.parameters);
            expect(nestedParameteredQuery.select).to.equal(selectStatement);
            expect(nestedParameteredQuery.where).to.equal(whereClause);
            // expect(_.pluck(nestedParameteredQuery.commonTableExpressions, 'statement')).to.eql(commonTableExpressions);
            expect(nestedParameteredQuery.previousOutput.parameters).to.eql(parameters);
            expect(nestedParameteredQuery.previousOutput.lastPosition).to.equal(parameters.length);
        });
    });

    describe("#compose", function() {
        it("composes a query from a nested criteria object (containing only nonrecursive fields)", function() {
            var query = this.strategy.compose(nestedParamsObj);

            var commonTableExpr = [
                "WITH nested_1 AS (SELECT id, biobank_code, parent_subject, parent_sample FROM sample ",
                "WHERE type = $7 AND (((metadata->$8->>'value')::text IN ($9)))), ",
                "nested_2 AS (SELECT id, biobank_code, parent_subject, parent_sample FROM sample WHERE type = $10 ",
                "AND (((metadata->$11->>'value')::float >= $12 AND (metadata->$11->>'unit')::text LIKE $13))), ",
                "nested_3 AS (SELECT id, parent_subject, parent_sample, parent_data FROM data ",
                "WHERE type = $14 AND (((metadata->$15->>'value')::text IN ($16,$17)))), ",
                "nested_4 AS (SELECT id, biobank_code, parent_subject, parent_sample FROM sample WHERE type = $18 ",
                "AND (((metadata->$19->>'value')::float >= $20 AND (metadata->$19->>'unit')::text LIKE $21))), ",
                "nested_5 AS (SELECT id, parent_subject, parent_sample, parent_data FROM data ",
                "WHERE type = $22 AND (((metadata->$23->>'value')::text IN ($24))))"
            ].join("");
            var mainQuery = [
                "SELECT DISTINCT d.id, d.code, d.sex, d.metadata FROM subject d ",
                "INNER JOIN nested_1 ON nested_1.parent_subject = d.id ",
                "INNER JOIN nested_2 ON nested_2.parent_sample = nested_1.id ",
                "INNER JOIN nested_3 ON nested_3.parent_sample = nested_2.id ",
                "INNER JOIN nested_4 ON nested_4.parent_sample = nested_1.id ",
                "INNER JOIN nested_5 ON nested_5.parent_sample = nested_4.id ",
                "WHERE d.type = $1 ",
                "AND (((d.metadata->$2->>'value')::integer <= $3 AND (d.metadata->$2->>'unit')::text LIKE $4) ",
                "AND ((d.metadata->$5->>'value')::text IN ($6)));"
            ].join("");
            expect(query).to.have.property('statement');
            expect(query).to.have.property('parameters');
            expect(query.statement).to.equal(commonTableExpr + " " + mainQuery);
        });

        it("composes a query from a nested subject criteria object (containing specialized fields only)", function() {
            var query = this.strategy.compose(subjectParamsObj);

            var commonTableExpr = [
                "WITH pd AS (SELECT id, given_name, surname, birth_date FROM personal_details), ",
                "nested_1 AS (SELECT id, biobank_code, parent_subject, parent_sample FROM sample ",
                "WHERE type = $10 AND ((biobank_code LIKE $11) AND ((metadata->$12->>'value')::text IN ($13))))"
            ].join("");
            var mainQuery = [
                "SELECT DISTINCT d.id, d.code, d.sex, d.metadata FROM subject d ",
                "LEFT JOIN pd ON pd.id = d.personal_info ",
                "INNER JOIN nested_1 ON nested_1.parent_subject = d.id ",
                "WHERE d.type = $1 ",
                "AND ((pd.surname LIKE $2 AND pd.given_name NOT LIKE $3) ",
                "AND (d.code LIKE $4) AND (d.sex IN ($5,$6)) AND ((d.metadata->$7->>'value')::text IN ($8,$9)));"
            ].join("");
            expect(query).to.have.property('statement');
            expect(query).to.have.property('parameters');
            expect(query.statement).to.equal(commonTableExpr + " " + mainQuery);
            console.log("Parameters for query with sex options: ");
            console.log(query.parameters);
            expect(query.parameters).to.have.length(13);
            // name and surname searches should be set to uppercase
            expect(query.parameters[1]).to.equal(subjectParamsObj.content[0].surname.toUpperCase());
            expect(query.parameters[2]).to.equal(subjectParamsObj.content[0].givenName.toUpperCase());
        });

        it("composes a query from a nested subject criteria object (containing specialized fields only)", function() {
        
            var query = this.strategy.compose(_.assign({wantsPersonalInfo: true}, _.cloneDeep(subjectParamsObj)));
            var commonTableExpr = [
                "WITH pd AS (SELECT id, given_name, surname, birth_date FROM personal_details), ",
                "nested_1 AS (SELECT id, biobank_code, parent_subject, parent_sample FROM sample ",
                "WHERE type = $10 AND ((biobank_code LIKE $11) AND ((metadata->$12->>'value')::text IN ($13))))"
            ].join("");
            var mainQuery = [
                "SELECT DISTINCT d.id, d.code, d.sex, pd.given_name, pd.surname, pd.birth_date, d.metadata FROM subject d ",
                "LEFT JOIN pd ON pd.id = d.personal_info ",
                "INNER JOIN nested_1 ON nested_1.parent_subject = d.id ",
                "WHERE d.type = $1 AND ((pd.surname LIKE $2 AND pd.given_name NOT LIKE $3) ",
                "AND (d.code LIKE $4) AND (d.sex IN ($5,$6)) AND ((d.metadata->$7->>'value')::text IN ($8,$9)));"
            ].join("");
            expect(query).to.have.property('statement');
            expect(query).to.have.property('parameters');
            expect(query.statement).to.equal(commonTableExpr + " " + mainQuery);
            console.log(query.parameters);
            expect(query.parameters).to.have.length(13);
        
        });

    });

});
