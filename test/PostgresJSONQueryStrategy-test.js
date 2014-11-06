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
            "fieldValue":"Diseased"
        },{
            "pivotDataType":2,
            "classTemplate":"Sample",
            "content":[{
                "fieldName":"Diagnosis",
                "fieldType":"text",
                "isList":true,
                "comparator":"IN",
                "fieldValue":"Neuroblastoma"
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
                        "fieldValue":"SCA"
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
                        "fieldValue":"high"
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
                criteriaObj.content[3].fieldName, criteriaObj.content[3].fieldValue, criteriaObj.content[3].fieldUnit ];
                expect(parameteredQuery).to.have.property('statement');
                expect(parameteredQuery).to.have.property('parameters');
                expect(parameteredQuery).to.have.property('lastPosition');
                expect(parameteredQuery.statement).to.equal(statement);
                expect(parameteredQuery.parameters).to.eql(parameters);
                expect(parameteredQuery.lastPosition).to.equal(13);
        });

        it("composes a query from a criteria object containing only nonrecursive fields", function() {
            var parameteredQuery = this.strategy.composeSingle(nestedParamsObj);
            console.log(parameteredQuery);
            expect(parameteredQuery).to.have.property('statement');
            expect(parameteredQuery).to.have.property('parameters');
            expect(parameteredQuery).to.have.property('lastPosition');
        });

    
    });

    describe("#compose", function() {
    
    });
  
});
