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
            "comparator":"LIKE",
            "fieldValue":"Saturn"
        }]
    };

    var mixedParamsObj = {
        "pivotDataType":5,
        "content":[{
            "fieldName":"Type",
            "fieldType":"text",
            "isList":true,
            "comparator":"IN",
            "fieldValue":"hypergiant,supergiant"
        },{
            "fieldName":"Radius",
            "fieldType":"float",
            "isList":false,
            "comparator":">=",
            "fieldValue":"1000",
            "fieldUnit":"R☉"
        },{
            "loopName":"Other designantions",
            "content":[{
                "fieldName":"designation",
                "fieldType":"text",
                "isList":false,
                "comparator":"=",
                "fieldValue":"UY Sct"
            }]
        },{
            "loopName":"Companions",
            "content":[{
                "fieldName":"planet",
                "fieldType":"boolean",
                "isList":false
            },{
                "fieldName":"companion name",
                "fieldType":"text",
                "isList":false
            },{
                "fieldName":"companion radius",
                "fieldType":"float",
                "isList":false,
                "comparator":"<=",
                "fieldValue":"50000",
                "fieldUnit":"km"
            },{
                "fieldName":"companion distance",
                "fieldType":"float",
                "isList":false,
                "comparator":">",
                "fieldValue":"50",
                "fieldUnit":"AU"
            }]
        }]
    };

    before(function() {
        this.strategy = new PostgresJSONQueryStrategy();
    });

    describe("#composeLoopStatement", function() {

        it("composes a common table expression subquery based on the loop parameters", function() {
            for (j=1; j<5; j++) {
                for (var i=0; i<20; i=i+2){
                    var loopQuery = this.strategy.composeLoopStatement(loopParamsObj, i, j);
                    var commonTableExpr = ["loop_instances_"+j+" AS (",
                        "SELECT * FROM (", 
                        "SELECT id, row_number() OVER () AS rid, list_1.value::text AS attribute_1_value, list_1.unit::text AS attribute_1_unit FROM ",
                        "(SELECT id, json_array_elements(metadata->$"+(i+1)+"->'value') AS value, ",
                        "json_array_elements(metadata->$"+(i+1)+"->'unit') AS unit FROM data) AS list_1 ",
                        ") AS field_1 ", 
                        "LEFT JOIN (",
                        "SELECT row_number() OVER () AS rid, list_2.value::text AS attribute_2_value, list_2.unit::text AS attribute_2_unit FROM ",
                        "(SELECT id, json_array_elements(metadata->$"+(i+4)+"->'value') AS value, ",
                        "json_array_elements(metadata->$"+(i+4)+"->'unit') AS unit FROM data) AS list_2 ",
                        ") AS field_2 ",
                        "ON field_1.rid = field_2.rid ",
                        "LEFT JOIN (",
                        "SELECT row_number() OVER () AS rid, list_3.value::text AS attribute_3_value, list_3.unit::text AS attribute_3_unit FROM ",
                        "(SELECT id, json_array_elements(metadata->$"+(i+7)+"->'value') AS value, ",
                        "json_array_elements(metadata->$"+(i+7)+"->'unit') AS unit FROM data) AS list_3 ",
                        ") AS field_3 ",
                        "ON field_1.rid = field_3.rid",
                        ")"].join("");
                    if (j === 1) {
                        commonTableExpr = "WITH " + commonTableExpr;
                    }
                    else {
                        commonTableExpr = ", " + commonTableExpr;
                    }
                        var mainQuery = ["SELECT DISTINCT d_"+j+".id FROM data d_"+j,
                            " LEFT JOIN loop_instances_"+j+" ON loop_instances_"+j+".id = d_"+j+".id ",
                            "WHERE attribute_1_value::float >= $"+(i+2)+" AND attribute_1_unit LIKE $"+(i+3),
                            " AND attribute_2_value::float > $"+(i+5)+" AND attribute_2_unit LIKE $"+(i+6) + " ", 
                            "AND attribute_3_value::text LIKE $"+(i+8)+";"].join("");
                            expect(loopQuery).to.have.property('commonTableExpr');
                            expect(loopQuery).to.have.property('mainQuery');
                            expect(loopQuery.commonTableExpr).to.equal(commonTableExpr);
                            expect(loopQuery.mainQuery).to.equal(mainQuery);
                            if (i===0 && j===1) {
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

        it("composes a query from a criteria object containing both nonrecursive fields and loops", function() {
            var parameteredQuery = this.strategy.compose(mixedParamsObj);
            // TODO: substitute attribute underscored names in the expected query
            var statement = ["WITH loop_instances_1 AS (",
                "SELECT * FROM (",
                "SELECT id, row_number() OVER () AS rid, list_1.value::text AS designation, list_1.unit::text AS designation_unit FROM (",
                "SELECT id, json_array_elements(metadata->$7->'value') AS value, json_array_elements(metadata->$7->'unit') AS unit FROM data",
                ") AS list_1",
                ") AS field_1",
                "), loop_instances_2 AS (",
                "SELECT * FROM (",
                "SELECT id, row_number() OVER () AS rid, list_1.value::text AS companion_radius, list_1.unit::text AS companion_radius_unit FROM (",
                "SELECT id, json_array_elements(metadata->$9->'value') AS value, json_array_elements(metadata->$9->'unit') AS unit FROM data",
                " ) AS list_1",
                ") AS field_1 LEFT JOIN (",
                "SELECT row_number() OVER () AS rid, list_2.value::text AS companion_distance, list_2.unit::text AS companion_distance_unit FROM (",
                "SELECT id, json_array_elements(metadata->$12->'value') AS value, json_array_elements(metadata->$12->'unit') AS unit FROM data",
                " ) AS list_2 ) AS field_2 ON field_1.rid = field_2.rid",
                ")",
                "SELECT * FROM data d ",
                "INNER JOIN (",
                "SELECT DISTINCT d_1.id FROM data d_1 LEFT JOIN loop_instances_1 ON loop_instances_1.id = d_1.id  WHERE designation::text LIKE $8",
                ") AS loop_data_1 ON loop_data_1.id = d.id ",
                "INNER JOIN (",
                "SELECT DISTINCT d_2.id FROM data d_2 LEFT JOIN loop_instances_2 ON loop_instances_2.id = d_2.id ",
                "WHERE companion_radius::float <= $10 AND companion_radius_unit LIKE $11 AND companion_distance::float <= $13",
                " AND companion_distance_unit LIKE $14",
                ") AS loop_data_2 ON loop_data_2.id = d.id ",
                "WHERE type = $1 AND (metadata->$2->'value'->>0)::text IN ($3)",
                " AND (metadata->$4->'value'->>0)::float >= $5 AND (metadata->$4->'unit'->>0)::text LIKE $6;"    
            ].join("");

            expect(parameteredQuery).to.have.property('statement');
            expect(parameteredQuery).to.have.property('parameters');
            expect(parameteredQuery.statement).to.equal(statement);
            expect(parameteredQuery.parameters).to.eql(parameters);
        });

    });

});
