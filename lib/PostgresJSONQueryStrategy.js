/**
 * @author Massimiliano Izzo
 * @description a method to composed a query on JSON/JSONB metadata stored within 
 *              the XTENS repository (see https://github.com/biolab-unige/xtens-app)
 */
var _ = require("lodash");

function PostgresJSONQueryStrategy() {
    this.getSubqueryRow = function(element, lastParameterPosition) {
        var nameParam = '$'+(++lastParameterPosition), valueParam = '$'+(++lastParameterPosition);
        var fieldValueOpener = element.isList ? " (" : " ";
        var fieldValueCloser = element.isList ? ")" : "";
        var subquery = "(metadata->" + nameParam + "->'value'->>0)::" + element.fieldType.toLowerCase()  + 
            " " + element.comparator + fieldValueOpener + valueParam + fieldValueCloser;
        if (element.fieldUnit) {
            var unitParam = '$'+(++lastParameterPosition);
            subquery += " AND ";
            subquery += "(metadata->" + nameParam + "->'unit'->>0)::text LIKE " + unitParam;
        }
        return {subquery: subquery, lastParameterPosition: lastParameterPosition};
    };

    this.composeLoopStatement = function(element, lastParameterPosition, loopCount) {
        // TODO solve the double quote("") issue
        var subquery, subqueryHeader, subqueryFooter, subqueryInsertion, name, names = [], constraints=[];
        var nameParam, valueParam; 
        var commonTableExpr = (loopCount === 1) ? "WITH " : ", "; 
        commonTableExpr += "loop_instances_" + loopCount + " AS (";
        commonTableExpr += "SELECT * FROM (";
        var count = 0;
        for (var i=0; i<element.content.length; i++) {
            var field = element.content[i];
            if (!field.fieldValue) {
                continue;
            }
            nameParam = '$'+(++lastParameterPosition);
            valueParam = '$'+(++lastParameterPosition);
            subqueryHeader = count ? " LEFT JOIN (" : "";
            subqueryInsertion = count ? "" : "id, ";
            subquery = "SELECT "+subqueryInsertion+"row_number() OVER () AS rid, list_"+count+".value::text AS attribute_"+count+"_value";
            subquery += ", list_"+count+".unit::text AS attribute_"+count+"_unit FROM ";
            subquery += "(SELECT id, json_array_elements(metadata->"+nameParam+"->'value') AS value, "; 
            subquery += "json_array_elements(metadata->"+nameParam+"->'unit') AS unit FROM data) AS list_"+count;
            subquery += ") AS field_"+count;
            subqueryFooter = count ? " ON field_0.rid = field_"+count+".rid" : "";
            subquery = subqueryHeader + subquery + subqueryFooter;
            commonTableExpr += subquery;
            constraints.push("attribute_" + count + "_value::" + field.fieldType.toLowerCase() + " " + field.comparator + " " + valueParam);
            if (field.fieldUnit) {
                unitParam = '$'+(++lastParameterPosition);
                constraints.push("attribute_" + count + "_unit LIKE " + unitParam);
            }
            count++;
        }    
        commonTableExpr += ")";
        var mainQuery = "(SELECT DISTINCT d_"+loopCount+".id FROM data d_"+loopCount;
        mainQuery += " LEFT JOIN loop_instances_"+loopCount+" ON loop_instances_"+loopCount+".id = d_"+loopCount+".id ";
        mainQuery += "WHERE ";
        mainQuery += constraints.join(" AND ");
        mainQuery += ") AS loop_data_"+loopCount+" ON loop_data_"+loopCount+".id = d.id";
        return {commonTableExpr: commonTableExpr, mainQuery: mainQuery, lastParameterPosition: lastParameterPosition};
    };
}

PostgresJSONQueryStrategy.prototype.compose = function(serializedCriteria) {
    var query = "SELECT * FROM data d ";
    var whereClause = "WHERE type = $1";
    var parameters = [ serializedCriteria.pivotDataType ];
    var lastPosition = 1, loopCount = 1, loopQueries = [], fieldQueries = [], value;
    if (serializedCriteria.content) {
        for (var i=0; i<serializedCriteria.content.length; i++) {
            var element = serializedCriteria.content[i];
            if (element.loopName) {
                var loopOp = this.composeLoopStatement(element, lastPosition, loopCount);
                loopCount++;
                loopQueries.push(loopOp);
                lastPosition = loopOp.lastParameterPosition;
                for (var j=0; j<element.content.length; j++) {
                    if (element.content[j].fieldValue) {
                        value = (element.content[j].fieldType === 'text') ? "\"" + element.content[j].fieldValue + "\"" : element.content[j].fieldValue;
                        parameters.push(element.content[j].fieldName, value);
                        if (element.content[j].fieldUnit !== undefined) {
                            parameters.push("\"" + element.content[j].fieldUnit + "\"");
                        }
                    }
                } 
            }
            else {
                var op = this.getSubqueryRow(element, lastPosition);
                fieldQueries.push(op.subquery);
                lastPosition = op.lastParameterPosition;
                parameters.push(element.fieldName, element.fieldValue);
                if (element.fieldUnit !== undefined) {
                    parameters.push(element.fieldUnit);
                }
            }
        }
    }
    var prequery = _.map(loopQueries, function(loopQuery) {
        return loopQuery.commonTableExpr;
    }).join("");
    var joinClause = _.map(loopQueries, function(loopQuery) {
        return "INNER JOIN " + loopQuery.mainQuery + " ";
    }).join("");
    if (fieldQueries.length) {
        whereClause += " AND " + fieldQueries.join(" AND ") + ";";
    } 
    query = (prequery + " " + query + joinClause + whereClause).trim();
    return {statement: query, parameters: parameters};
};

module.exports = PostgresJSONQueryStrategy;
