/**
 * @author Massimiliano Izzo
 * @description a method to composed a query on JSON/JSONB metadata stored within 
 *              the XTENS repository (see https://github.com/biolab-unige/xtens-app)
 */

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
            subquery += "(metadata->" + nameParam + "->'unit'->>0)::text = " + unitParam;
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
        var notFirst = false;
        for (var i=0; i<element.content.length; i++) {
            var field = element.content[i];
            if (!field.fieldValue) {
                continue;
            }
            nameParam = '$'+(++lastParameterPosition);
            valueParam = '$'+(++lastParameterPosition);
            subqueryHeader = notFirst ? " LEFT JOIN (" : "";
            name = field.fieldName.toLowerCase().replace(" ", "_");
            names.push(name);
            subqueryInsertion = notFirst ? "" : "id, ";
            subquery = "SELECT "+subqueryInsertion+"row_number() OVER () AS rid, list_"+i+".value::text AS "+name;
            subquery += ", list_"+i+".unit::text AS "+name+"_unit FROM ";
            subquery += "(SELECT id, json_array_elements(metadata->"+nameParam+"->'value') AS value, "; 
            subquery += "json_array_elements(metadata->"+nameParam+"->'unit') AS unit FROM data) AS list_"+i+" ";
            subquery += ") AS field_"+i;
            subqueryFooter = notFirst ? " ON field_1.rid = field_"+i+".rid" : "";
            subquery = subqueryHeader + subquery + subqueryFooter;
            commonTableExpr += subquery;
            constraints.push(name + "::" + field.fieldType.toLowerCase() + " " + field.comparator + " " + valueParam);
            if (field.fieldUnit) {
                unitParam = '$'+(++lastParameterPosition);
                constraints.push(name + "_unit LIKE " + unitParam);
            }
            notFirst = true;
        }    
        commonTableExpr += ")";
        var mainQuery = "SELECT DISTINCT data.id FROM data ";
        mainQuery += "LEFT JOIN loop_instances_"+loopCount+" ON loop_instances_"+loopCount+".id = data.id ";
        mainQuery += "WHERE ";
        mainQuery += constraints.join(" AND ");
        mainQuery += ";";
        return {commonTableExpr: commonTableExpr, mainQuery: mainQuery, lastParameterPosition: lastParameterPosition};
    };
}

PostgresJSONQueryStrategy.prototype.compose = function(serializedCriteria) {
    var query = "SELECT * FROM data WHERE type = $1";
    var parameters = [ serializedCriteria.pivotDataType ];
    var lastPosition = 1, loopCount = 0, loopQueries = [];
    if (serializedCriteria.content) {
        query += " AND (";
        for (var i=0; i<serializedCriteria.content.length; i++) {
            var element = serializedCriteria.content[i];
            if (element.loopName) {
                var loopOp = this.composeLoopStatement(element, lastPosition, loopCount);
                loopCount++;
                loopQueries.push(loopOp);
                lastPosition = loopOp.lastParameterPosition;
                for (var j=0; j<element.content.length; j++) {
                    parameters.push(element.content[j].fieldName, element.content[j].fieldValue);
                    if (element.fieldUnit !== undefined) {
                        parameters.push(element.content[j].fieldUnit);
                    }
                } 
            }
            else {
                var op = this.getSubqueryRow(element, lastPosition);
                query += op.subquery;
                lastPosition = op.lastParameterPosition;
                parameters.push(element.fieldName, element.fieldValue);
                if (element.fieldUnit !== undefined) {
                    parameters.push(element.fieldUnit);
                }
                if (i < len-1) {
                    query += " AND ";
                }
            }
        }
        query += ")";
    }
    var prequery = _.map(loopQueries, function(loopQuery) {
        return loopQuery.commonTableExpr;
    }).join("");
    query += ";";
    console.log(query);
    return {statement: query, parameters: parameters};
};

module.exports = PostgresJSONQueryStrategy;
