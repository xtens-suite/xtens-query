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
            fieldValue: "hypergiant,supergiant",
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

    var loopParamsObj = {
        "loopName":"Companions",
        "content":[{
            "fieldName":"planet",
            "fieldType":"boolean",
            "isList":false
        },{
            "fieldName":"companion radius",
            "fieldType":"float",
            "isList":false,
            "comparator":">=",
            "fieldValue":"50000",
            "fieldUnit":"km"
        },{
            "fieldName":"companion distance",
            "fieldType":"float",
            "isList":false,
            "comparator":">",
            "fieldValue":"4.5",
            "fieldUnit":"AU"
        },{
            "fieldName":"companion name",
            "fieldType":"text",
            "isList":false,
            "comparator":"=",
            "fieldValue":"Saturn"
        }]
    };

    before(function() {
        this.strategy = new PostgresJSONQueryStrategy();
    });

    describe("#composeLoopStatement", function() {

        it("composes a common table expression subquery based on the loop parameters", function() {
            for (j=1; j<5; j++) {
                for (var i=1; i<20; i=i+2){
                    var loopQuery = this.strategy.composeLoopStatement(loopParamsObj, i, j);
                    var commonTableExpr = ["WITH loop_instances_"+j+" AS (",
                        "SELECT * FROM (", 
                        "SELECT id, row_number() OVER () AS rid, list_1.json_array_elements::text AS companion_radius FROM ",
                        "(SELECT id, json_array_elements(metadata->$"+(i+1)+"->'value') FROM data) AS list_1 ",
                        ") AS field_1 ", 
                        "LEFT JOIN (",
                        "SELECT row_number() OVER () AS rid, list_2.json_array_elements::text AS companion_distance FROM ",
                        "(SELECT id, json_array_elements(metadata->$"+(i+3)+"->'value') FROM data) AS list_2 ",
                        ") AS field_2 ",
                        "ON field_1.rid = field_2.rid ",
                        "LEFT JOIN (",
                        "SELECT row_number() OVER () AS rid, list_3.json_array_elements::text AS companion_name FROM ",
                        "(SELECT id, json_array_elements(metadata->$"+(i+5)+"->'value') FROM data) AS list_3 ",
                        ") AS field_3 ",
                        "ON field_1.rid = field_3.rid",
                        ")"].join("");
                        var mainQuery = ["SELECT DISTINCT data.id FROM data ",
                            "LEFT JOIN loop_instances_"+j+" ON loop_instances_"+j+".id = data.id ",
                            "WHERE companion_radius::float >= $"+(i+2)+" AND companion_distance::float > $"+(i+4)+" ", 
                            "AND companion_name::text = $"+(i+6)+";"].join("");
                            expect(loopQuery).to.have.property('commonTableExpr');
                            expect(loopQuery).to.have.property('mainQuery');
                            expect(loopQuery.commonTableExpr).to.equal(commonTableExpr);
                            expect(loopQuery.mainQuery).to.equal(mainQuery);
                        if (i===1 && j===1) {
                            console.log(commonTableExpr);
                            console.log(mainQuery);
                        }
                }
            }
        });

    });

    describe("#compose", function() {

        it("composes a query from a criteria object containing only nonrecursive fields", function() {
            var parameteredQuery = this.strategy.compose(criteriaObj);
            var statement = "SELECT * FROM data WHERE type = $1 AND (" +
                "(metadata->$2->'value'->>0)::text = $3 AND " +
                "(metadata->$4->'value'->>0)::text IN ($5) AND " +
                "(metadata->$6->'value'->>0)::float >= $7 AND " + "(metadata->$6->'unit'->>0)::text = $8 AND " +
                "(metadata->$9->'value'->>0)::integer > $10 AND " + "(metadata->$9->'unit'->>0)::text = $11" +
                ");";
            var parameters = [ criteriaObj.pivotDataType, 
                criteriaObj.content[0].fieldName, criteriaObj.content[0].fieldValue,
                criteriaObj.content[1].fieldName, criteriaObj.content[1].fieldValue, 
                criteriaObj.content[2].fieldName, criteriaObj.content[2].fieldValue, criteriaObj.content[2].fieldUnit,
                criteriaObj.content[3].fieldName, criteriaObj.content[3].fieldValue, criteriaObj.content[3].fieldUnit ];
                expect(parameteredQuery).to.have.property('statement');
                expect(parameteredQuery).to.have.property('parameters');
                expect(parameteredQuery.statement).to.equal(statement);
                expect(parameteredQuery.parameters).to.eql(parameters);
        });

    });

});
