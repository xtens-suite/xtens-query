/**
 * @author Massimiliano Izzo
 * @description unit test 
 */
var expect = require('chai').expect;
var PostgresJSONQueryStrategy = require('../lib/PostgresJSONQueryStrategy');

describe("QueryStrategy.PostgresJSON", function() {

    var criteriaObj = {
        pivotDataType: 1,
        content: [
            {
            fieldName: "constellation",
            fieldType: "text",
            comparator: "=",
            fieldValue: "cepheus",
            isList: false
        },
        {
            fieldName: "type", // the stellar type
            fieldType: "text",
            comparator: "IN",
            fieldValue: ["hypergiant","supergiant","main-sequence star"],
            isList: true
        },
        {
            fieldName: "mass",
            fieldType: "float",
            comparator: ">=",
            fieldValue: "1.5",
            fieldUnit: "M☉"
        },
        {
            comparator: ">",
            fieldName: "distance",
            fieldType: "integer",
            fieldUnit: "pc",
            fieldValue: "50"
        }
        ]
    };


    var nestedParamsObj = {
        "pivotDataType":1,
        "classTemplate": "Subject",
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
            "pivotDataType":2,
            "classTemplate":"Sample",
            "content":[{
                "fieldName":"Diagnosis",
                "fieldType":"text",
                "isList":true,
                "comparator":"IN",
                "fieldValue":["Neuroblastoma"]
            },{
                "pivotDataType":6,
                "classTemplate":"Sample",
                "content":[{
                    "fieldName":"quantity",
                    "fieldType":"float",
                    "isList":false,
                    "comparator":">=",
                    "fieldValue":"1.0",
                    "fieldUnit":"μl"
                },{
                    "pivotDataType":3,
                    "classTemplate":"Generic",
                    "content":[{
                        "fieldName":"Overall Result",
                        "fieldType":"text",
                        "isList":true,
                        "comparator":"IN",
                        "fieldValue":["SCA","NCA"]
                    }]
                }]
            },{
                "pivotDataType":7,
                "classTemplate":"Sample",
                "content":[{
                    "fieldName":"quantity",
                    "fieldType":"float",
                    "isList":false,
                    "comparator":">=",
                    "fieldValue":"1.2",
                    "fieldUnit":"µg"
                },{
                    "pivotDataType":8,
                    "classTemplate":"Generic",
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

    before(function() {
        this.strategy = new PostgresJSONQueryStrategy();
    });


    describe("#composeSingle", function() {

        it("composes a query from a criteria object containing only nonrecursive fields", function() {
            var parameteredQuery = this.strategy.composeSingle(criteriaObj);
            var statement = "SELECT * FROM data d WHERE type = $1 AND " +
                "(metadata->$2->'value'->>0)::text = $3 AND " +
                "(metadata->$4->'value'->>0)::text IN ($5,$6,$7) AND " +
                "(metadata->$8->'value'->>0)::float >= $9 AND " + "(metadata->$8->'unit'->>0)::text LIKE $10 AND " +
                "(metadata->$11->'value'->>0)::integer > $12 AND " + "(metadata->$11->'unit'->>0)::text LIKE $13" +
                ";";
            var parameters = [ criteriaObj.pivotDataType, 
                criteriaObj.content[0].fieldName, criteriaObj.content[0].fieldValue,
                criteriaObj.content[1].fieldName, criteriaObj.content[1].fieldValue[0], 
                criteriaObj.content[1].fieldValue[1], criteriaObj.content[1].fieldValue[2], 
                criteriaObj.content[2].fieldName, criteriaObj.content[2].fieldValue, criteriaObj.content[2].fieldUnit,
                criteriaObj.content[3].fieldName, criteriaObj.content[3].fieldValue, criteriaObj.content[3].fieldUnit 
            ];
            expect(parameteredQuery).to.have.property('statement');
            expect(parameteredQuery).to.have.property('parameters');
            expect(parameteredQuery).to.have.property('lastPosition');
            expect(parameteredQuery.statement).to.equal(statement);
            expect(parameteredQuery.parameters).to.eql(parameters);
            expect(parameteredQuery.lastPosition).to.equal(13);
        });

        it("composes a set of queries from a nested criteria object", function() {
            var commonTableExpressions = [
                "SELECT * FROM sample WHERE type = $7 AND (metadata->$8->'value'->>0)::text IN ($9)",
                "SELECT * FROM sample WHERE type = $10 AND (metadata->$11->'value'->>0)::float >= $12 AND (metadata->$11->'unit'->>0)::text LIKE $13",
                "SELECT * FROM sample WHERE TYPE = $14 AND (metadata->$15->'value'->>0)::float >= $16 AND (metadata->$15->'unit'->>0)::text LIKE $17",
                "SELECT * FROM data WHERE type = $18 AND (metadata->$19->'value'->>0)::text IN ($20,$21)",
                "SELECT * FROM data WHERE type =$22 AND (metadata->$23->'value'->>0)::text IN ($24)"
            ];
            var statement = "SELECT * FROM subject d WHERE type = $1 AND (metadata->$2->'value'->>0)::integer <= $3 "; 
            statement += "AND (metadata->$2->'unit'->>0)::text LIKE $4 AND (metadata->$5->'value'->>0)::text IN ($6);";
            var parameters = [ nestedParamsObj.pivotDataType,
                nestedParamsObj.content[0].fieldName, nestedParamsObj.content[0].fieldValue, nestedParamsObj.content[0].fieldUnit, // Subject
                nestedParamsObj.content[1].fieldName, nestedParamsObj.content[1].fieldValue,
                nestedParamsObj.content[2].pivotDataType, nestedParamsObj.content[2].content[0].fieldName, //Tissue
                nestedParamsObj.content[2].content[0].fieldValue, 
                nestedParamsObj.content[2].content[1].pivotDataType, nestedParamsObj.content[2].content[1].content[0].fieldName, // DNA Sample
                nestedParamsObj.content[2].content[1].content[0].fieldValue, nestedParamsObj.content[2].content[1].content[0].fieldUnit,
                nestedParamsObj.content[2].content[2].pivotDataType, nestedParamsObj.content[2].content[2].content[0].fieldName, // RNA Sample
                nestedParamsObj.content[2].content[2].content[0].fieldValue, nestedParamsObj.content[2].content[2].content[0].fieldUnit,
                nestedParamsObj.content[2].content[1].content[1].pivotDataType, // CGH
                nestedParamsObj.content[2].content[1].content[1].content[0].fieldName,
                nestedParamsObj.content[2].content[1].content[1].content[0].fieldValue[0], 
                nestedParamsObj.content[2].content[1].content[1].content[0].fieldValue[1],
                nestedParamsObj.content[2].content[2].content[1].pivotDataType, // Microarray
                nestedParamsObj.content[2].content[2].content[1].content[0].fieldName,
                nestedParamsObj.content[2].content[2].content[1].content[0].fieldValue[0], 
                nestedParamsObj.content[2].content[2].content[1].content[0].fieldValue[1]
            ];
            console.log(parameters);
            console.log(parameters.length);
            debugger;
            var nestedParameteredQuery = this.strategy.composeSingle(nestedParamsObj);
            expect(nestedParameteredQuery.statement).to.equal(statement);
            expect(nestedParameteredQuery.commonTableExpressions).to.eql(commonTableExpressions);
            expect(nestedParameteredQuery.parameters).to.eql(parameters);
            expect(nestedParameteredQuery.lastPosition).to.equal(parameters.length);
        });
    });

    describe("#compose", function() {
        it("composes a query from a nested criteria object (containing only nonrecursive fields)", function() {
            var parameteredQuery = this.strategy.compose(nestedParamsObj);
            var commonTableExpr = [
                "WITH nested_1 AS (SELECT * FROM sample WHERE type = $7 AND (metadata->$8->'value'->>0)::text IN ($9)), ",
                "nested_2 AS (SELECT * FROM sample WHERE type = $10 ",
                "AND (metadata->$11->'value'->>0)::float >= $12 AND (metadata->$11->'unit'->>0)::text LIKE $13), ",
                "nested_3 AS (SELECT * FROM sample WHERE TYPE = $14 ",
                "AND (metadata->$15->'value'->>0)::float >= $16 AND (metadata->$15->'unit'->>0)::text LIKE $17), ",
                "nested_4 AS (SELECT * FROM data WHERE type = $18 AND  (metadata->$19->'value'->>0)::text IN ($20,$21)), ",
                "nested_5 AS (SELECT * FROM data WHERE type =$22 AND (metadata->$23->'value'->>0)::text IN ($24))"
            ].join("");
            var mainQuery = [
                "SELECT DISTINCT d.id, d.code FROM subject d ",
                "INNER JOIN nested_1 ON nested_1.parent_subject = d.id ",
                "INNER JOIN nested_2 ON nested_2.parent_sample = nested_1.id ",
                "INNER JOIN nested_3 ON nested_3.parent_sample = nested_1.id ",
                "INNER JOIN nested_4 ON nested_4.parent_sample = nested_2.id ",
                "INNER JOIN nested_5 ON nested_5.parent_sample = nested_3.id ",
                "WHERE d.type = $1 ",
                "AND (d.metadata->$2->'value'->>0)::integer <= $3 AND (d.metadata->$2->'unit'->>0)::text LIKE $4 ",
                "AND (d.metadata->$5->'value'->>0)::text IN ($6);"
            ].join();
            /* TODO
            console.log(commonTableExpr);
            console.log(mainQuery);
            console.log(parameteredQuery);
            expect(parameteredQuery).to.have.property('statement');
            expect(parameteredQuery).to.have.property('parameters');
            expect(parameteredQuery).to.have.property('lastPosition'); */
        });
    });

});
