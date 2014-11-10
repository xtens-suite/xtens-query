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
            return "subject";
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

    this.composeInnerJoin = function(serializedCriteria) {
        // TODO how can it know whose child it is, to correctly compose the join?
        var joinStatement = "INNER JOIN nested_" + serializedCriteria.cteCount;
        joinStatement += " ON nested_" + serializedCriteria.cteCount + ".parent_" + " = " + ".id";
        return null;
    };

}

/**
 * @description composes a query based on a single DataType
 */
PostgresJSONQueryStrategy.prototype.composeSingle = function(serializedCriteria, previousOutput) { // should I pass the parent params??
    if (!previousOutput) { 
        previousOutput = {
            lastPosition: 0,
            cteCount: 0,
            commonTableExpressions: [],
            parameters: [],
            innerJoins: []
        }; 
    }
    else {
        previousOutput.innerJoins.push(this.composeInnerJoin(serializedCriteria));
    }
    var table = determineTableByClassTemplate(serializedCriteria.classTemplate); 
    var query = "SELECT * FROM " + table;
    var tableAlias = previousOutput.lastPosition ? " " : " d ";
    var whereClause = "WHERE type = $" + (++previousOutput.lastPosition);
    previousOutput.parameters.push(serializedCriteria.pivotDataType);
    var fieldQueries = [], value;
    if (serializedCriteria.content) {
        for (var i=0; i<serializedCriteria.content.length; i++) {
            var element = serializedCriteria.content[i];
            if (element.pivotDataType) {
                previousOutput.cteCount++;
                previousOutput = this.composeSingle(element, previousOutput);
                previousOutput.commonTableExpressions.push({
                    alias: "nested_"+previousOutput.cteCount,
                    statement: previousOutput.statement.replace(";","")
                }); 
                console.log(previousOutput);
            }
            else {
                var op = this.getSubqueryRow(element, previousOutput.lastPosition);
                fieldQueries.push(op.subquery);
                previousOutput.lastPosition = op.lastParameterPosition;
                previousOutput.parameters.push(element.fieldName, element.fieldValue);
                if (element.fieldUnit !== undefined) {
                    previousOutput.parameters.push(element.fieldUnit);
                }
            }
        }
    } 
    if (fieldQueries.length) {
        whereClause += " AND " + fieldQueries.join(" AND ") + ";";
    } 
    query = query + tableAlias + whereClause;
    return _.extend(previousOutput, {statement: query, parameters: _.flatten(previousOutput.parameters)});
};

/**
 * @description composes a query based on a single DataType
 */
PostgresJSONQueryStrategy.prototype.compose = function(serializedCriteria) {

};
module.exports = PostgresJSONQueryStrategy;
