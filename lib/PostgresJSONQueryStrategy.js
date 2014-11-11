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

    this.getSubqueryRow = function(element, lastParameterPosition, tablePrefix) {
        var nameParam = '$'+(++lastParameterPosition), valueParam, subquery;
        if (element.isList) {
            var values = [];
            for (var i=0; i<element.fieldValue.length; i++) {
                values.push('$'+(++lastParameterPosition));
            }
            console.log(values);
            valueParam = values.join();
            subquery = "(" + tablePrefix + "metadata->" + nameParam + "->'value'->>0)::" + element.fieldType.toLowerCase()  + 
                " " + element.comparator + " (" + valueParam  + ")";
        }
        else {
            valueParam = '$'+(++lastParameterPosition);
            subquery = "(" + tablePrefix + "metadata->" + nameParam + "->'value'->>0)::" + element.fieldType.toLowerCase()  + 
                " " + element.comparator + " " + valueParam;
        }
        if (element.fieldUnit) {
            var unitParam = '$'+(++lastParameterPosition);
            subquery += " AND ";
            subquery += "(" + tablePrefix + "metadata->" + nameParam + "->'unit'->>0)::text LIKE " + unitParam;
        }
        return {subquery: subquery, lastParameterPosition: lastParameterPosition};
    };

    this.composeCommonTableExpression = function(query, ctes, parentAlias, parentTable) {
        if (!ctes) {
            ctes = [];
        }
        else {
            var classTemplate = "data";
            var joinClause = "INNER JOIN " + query.alias + " ON " + query.alias + ".parent_" + parentTable + " = " + parentAlias + ".id";
            ctes.push({
                alias: query.alias, 
                commonTableExpression: query.alias + " AS (" + query.select + " " + query.where + ")", 
                joinClause: joinClause 
            });
        }
        var alias = query.alias || 'd';
        for (var i=0; i<query.subqueries.length; i++) {
            ctes = this.composeCommonTableExpression(query.subqueries[i], ctes, alias, query.table);
        }
        return ctes;
    };

}

/**
 * @description composes a query based on a single DataType
 */
PostgresJSONQueryStrategy.prototype.composeSingle = function(serializedCriteria, previousOutput, query) { // should I pass the parent params??
    if (!previousOutput) { 
        previousOutput = {
            lastPosition: 0,
            cteCount: 0,
            parameters: []
        }; 
    } 
    if (!query) { 
        query= {}; 
    }
    query.subqueries = [];
    query.table = determineTableByClassTemplate(serializedCriteria.classTemplate); 
    query.select= "SELECT * FROM " + query.table;
    var tableAlias = previousOutput.lastPosition ? "" : " d";
    var tablePrefix = previousOutput.lastPosition ? "" : "d.";
    query.where = "WHERE " + tablePrefix + "type = $" + (++previousOutput.lastPosition);
    previousOutput.parameters.push(serializedCriteria.pivotDataType);
    var fieldQueries = [], value;
    if (serializedCriteria.content) {
        for (var i=0; i<serializedCriteria.content.length; i++) {
            var element = serializedCriteria.content[i];
            if (element.pivotDataType) {
                var res = this.composeSingle(element, previousOutput, { alias: 'nested_'+(++previousOutput.cteCount)});
                previousOutput = res.previousOutput;
                query.subqueries.push(_.omit(res, 'previousOutput'));
            }
            else {
                var op = this.getSubqueryRow(element, previousOutput.lastPosition, tablePrefix);
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
        query.where += " AND " + fieldQueries.join(" AND ");
    } 
    query.select += tableAlias; 
    query.previousOutput =  _.extend(previousOutput, {parameters: _.flatten(previousOutput.parameters)});
    return query; // _.extend(previousOutput, {statement: query, parameters: _.flatten(previousOutput.parameters)});
};

/**
 * @description composes a query based on a single DataType
 */
PostgresJSONQueryStrategy.prototype.compose = function(serializedCriteria) {
    var query = this.composeSingle(serializedCriteria);
    var ctes = this.composeCommonTableExpression(query);
    commonTableExpressions = "";
    if (ctes.length) {
        query.select = "SELECT DISTINCT d.id FROM " + query.table + " d";
        commonTableExpressions = "WITH " + _.pluck(ctes, 'commonTableExpression').join(", ");
    }
    var mainStatement = query.select + " " + _.pluck(ctes, 'joinClause').join(" ") + " " + query.where;
    mainStatement = (commonTableExpressions + " " + mainStatement).trim() + ";";
    return { statement: mainStatement, parameters: query.previousOutput.parameters };
};
module.exports = PostgresJSONQueryStrategy;
