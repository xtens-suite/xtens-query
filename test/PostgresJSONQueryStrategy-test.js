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

    describe("#compose", function() {

        before(function() {
            this.strategy = new PostgresJSONQueryStrategy();
        });
        
        it("compose a query from a criteria object", function() {
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
