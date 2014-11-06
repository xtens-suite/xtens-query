/**
 * @author Massimiliano Izzo
 * @description a method to composed a query on JSON/JSONB metadata stored within 
 *              the XTENS repository (see https://github.com/biolab-unige/xtens-app)
 */
var _ = require("lodash");
var DataTypeClasses = require("./Utils").DataTypeClasses;

function determineTableByClassTemplate(classTemplate) {
    switch (classTemplate) {
        case DataTypeClasses.SUBJECT:
            return "data";
        case DataTypeClasses.SAMPLE:
            return "sample";
        case DataTypeClasses.GENERIC:
            return "data";
        default:
            return "data";
    }
}

function PostgresJSONQueryStrategy() {
    this.getSubqueryRow = function(element, lastParameterPosition) {
        var nameParam = '$'+(++lastParameterPosition), valueParam, subquery;
        if (element.isList) {
            var values = [];
            for (var i=0; i<element.fieldValue.length; i++) {
                values.push('$'+(++lastParameterPosition));
            }
            console.log(values);
            valueParam = values.join();
            subquery = "(metadata->" + nameParam + "->'value'->>0)::" + element.fieldType.toLowerCase()  + 
                " " + element.comparator + " (" + valueParam  + ")";
        }
        else {
            valueParam = '$'+(++lastParameterPosition);
            subquery = "(metadata->" + nameParam + "->'value'->>0)::" + element.fieldType.toLowerCase()  + 
                " " + element.comparator + " " + valueParam;
        }
        if (element.fieldUnit) {
            var unitParam = '$'+(++lastParameterPosition);
            subquery += " AND ";
            subquery += "(metadata->" + nameParam + "->'unit'->>0)::text LIKE " + unitParam;
        }
        return {subquery: subquery, lastParameterPosition: lastParameterPosition};
    };

}

/**
 * @description composes a query based on a single DataType
 */
PostgresJSONQueryStrategy.prototype.composeSingle = function(serializedCriteria, lastPos) {
    var table = determineTableByClassTemplate(serializedCriteria.classTemplate); 
    var query = "SELECT * FROM " + table + " d ";
    var commonTableExpressions = [];
    var whereClause = "WHERE type = $1";
    var parameters = [ serializedCriteria.pivotDataType ];
    var lastPosition = lastPos || 1, fieldQueries = [], value;
    if (serializedCriteria.content) {
        for (var i=0; i<serializedCriteria.content.length; i++) {
            var element = serializedCriteria.content[i];
            if (element.pivotDataType) {
                var subquery = this.composeSingle(element, lastPos);
                commonTableExpressions.push(subquery.statement);
                parameters.push(subquery.parameters);
                lastPosition = lastPosition;
            }
            else {
                var op = this.getSubqueryRow(element, lastPosition);
                fieldQueries.push(op.subquery);
                lastPosition = op.lastParameterPosition;
                parameters.push(element.fieldName,element.fieldValue);
                if (element.fieldUnit !== undefined) {
                    parameters.push(element.fieldUnit);
                }
            }
        }
    } 
    if (fieldQueries.length) {
        whereClause += " AND " + fieldQueries.join(" AND ") + ";";
    } 
    query = query + whereClause;
    return {
        statement: query, 
        parameters: _.flatten(parameters), 
        lastPosition: lastPosition,
        commonTableExpressions: commonTableExpressions
    };
};

/**
 * @description composes a query based on a single DataType
 */
PostgresJSONQueryStrategy.prototype.compose = function(serializedCriteria) {

};
module.exports = PostgresJSONQueryStrategy;
