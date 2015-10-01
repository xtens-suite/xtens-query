/**
 * @author Massimiliano Izzo
 * @description a method to composed a query on JSON/JSONB metadata stored within 
 *              the XTENS repository (see https://github.com/biolab-unige/xtens-app)
 */

/*jshint node: true */
/*jshint esnext: true */

"use strict";

var _ = require("lodash");
var DataTypeClasses = require("./Utils").DataTypeClasses;
var determineTableByModel = require("./Utils.js").determineTableByModel;
var allowedComparators = require("./Utils.js").allowedComparators;
var specializedProperties = require("./Utils.js").specializedProperties;
var pdProperties = require("./Utils.js").pdProperties;

var queryOutput = {
    lastPosition: 0,
    cteCount: 0,
    parameters: []
};

var fieldsForMainQueryMap = new Map([
    [DataTypeClasses.SUBJECT, "d.code, d.sex, "],
    [DataTypeClasses.SAMPLE, "d.biobank_code, "],
    [DataTypeClasses.DATA, ""]
]);

var fieldsForSubqueriesMap = new Map([
    [DataTypeClasses.SUBJECT, "id, code, sex"],
    [DataTypeClasses.SAMPLE, "id, biobank_code, parent_subject, parent_sample"],
    [DataTypeClasses.DATA, "id, parent_subject, parent_sample, parent_data"],
    [undefined, "id, parent_subject, parent_sample, parent_data"]  // default to DATA
]);

String.prototype.toUnderscore = function(){
    return this.replace(/([A-Z])/g, function($1){return "_"+$1.toLowerCase();});
};


/**
 * @class PostgresJSONQueryStrategy
 * @description strategy method to compose a query against PostgreSQL JSON data format where metadata are stored 
 */
function PostgresJSONQueryStrategy() {

    this.getSubqueryRow = function(element, previousOutput, tablePrefix) {
        if (_.isEmpty(element)) {
            return null;
        }
        if (allowedComparators.indexOf(element.comparator) < 0) {
            console.log(element.comparator);
            throw new Error("Operation not allowed. Trying to inject a forbidden comparator!!");
        }
        var nameParam = '$'+(++previousOutput.lastPosition), valueParam, subquery;
        if (element.isList) {
            var values = [];
            for (var i=0; i<element.fieldValue.length; i++) {
                values.push('$'+(++previousOutput.lastPosition));
            }
            console.log(values);
            valueParam = values.join();
            subquery = "(" + tablePrefix + "metadata->" + nameParam + "->>'value')::" + element.fieldType.toLowerCase()  + 
                " " + element.comparator + " (" + valueParam  + ")";
        }
        else {
            valueParam = '$'+(++previousOutput.lastPosition);
            subquery = "(" + tablePrefix + "metadata->" + nameParam + "->>'value')::" + element.fieldType.toLowerCase()  + 
                " " + element.comparator + " " + valueParam;
        }
        previousOutput.parameters.push(element.fieldName, element.fieldValue);
        if (element.fieldUnit) {
            var unitParam = '$'+(++previousOutput.lastPosition);
            subquery += " AND ";
            subquery += "(" + tablePrefix + "metadata->" + nameParam + "->>'unit')::text LIKE " + unitParam;
            previousOutput.parameters.push(element.fieldUnit);
        }
        // flatten nested arrays in parameters
        previousOutput.parameters = _.flatten(previousOutput.parameters);
        return {subquery: subquery};
    };

}

/**
 * @method
 * @name composeSpecializedQuery
 * @description compose the part of the query relative to the specialized Model (Model here is intended in the sails.js sense)
 * @return {Object} - the query for the specialized parameters
 */
PostgresJSONQueryStrategy.prototype.composeSpecializedQuery = function(criteria, previousOutput, tablePrefix) {
    var lastParameterPosition = previousOutput.lastPosition || 0;
    previousOutput.parameters = previousOutput.parameters || [];
    var dataTypeClass = criteria.specializedQuery;
    tablePrefix = tablePrefix || ''; //'d.';
    var query = {}, clauses = [], comparator;
    specializedProperties[dataTypeClass].forEach(function(property) {
        if (criteria[property]) {
            if (_.isArray(criteria[property])) { // if it is a list of options (like in sex)
                comparator = allowedComparators.indexOf(criteria[property+"Comparator"]) >= 0 ? criteria[property+"Comparator"] : 'IN';
                let values = [];
                for (let i=0; i<criteria[property].length; i++) {
                    values.push('$'+(++lastParameterPosition));
                }
                clauses.push(tablePrefix + property.toUnderscore() + " " + comparator + " (" + values.join() + ")");
            }
            else {
                comparator = allowedComparators.indexOf(criteria[property+"Comparator"]) >= 0 ? criteria[property+"Comparator"] : '=';
                clauses.push(tablePrefix + property.toUnderscore() + " " + comparator + " $" + (++lastParameterPosition));
            }
            previousOutput.parameters.push(criteria[property]);
        }
    });
    if (clauses.length) {
        query.subquery = clauses.join(" AND "); // TODO add possibility to switch and/or
    }
    query.lastParameterPosition = lastParameterPosition;
    query.parameters = previousOutput.parameters;
    return query;
};

/**
 * @method
 * @name composeSpecializedPersonalDetailsQuery
 * @description 
 * @return {Object}
 */
PostgresJSONQueryStrategy.prototype.composeSpecializedPersonalDetailsQuery = function(pdCriteria, previousOutput) {
    if (!previousOutput) {
        previousOutput = {
            lastPosition: 0,
            cteCount: 0,
            parameters: []
        }; 
    }
    var query = { alias: 'pd'};
    query.select = "SELECT id, given_name, surname, birth_date FROM personal_details";
    query.where = "";
    var whereClauses = []; // comparator;

    pdProperties.forEach(function(property) {
        if (pdCriteria[property]) {
            let comparator = allowedComparators.indexOf(pdCriteria[property+"Comparator"]) >= 0 ? pdCriteria[property+"Comparator"] : '=';
            whereClauses.push(property.toUnderscore() + " " + comparator + " $" + (++previousOutput.lastPosition));
            previousOutput.parameters.push(pdCriteria[property]);
        }
    });
    if (whereClauses.length) {
        query.where = "WHERE " + whereClauses.join(" AND ");
    }
    query.previousOutput = previousOutput;
    return query;
};


/**
 * @method
 * @name composeSingle
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
    query.table = determineTableByModel(serializedCriteria.model);
    console.log("PostgresJSONQueryStrategy.prototype.composeSingle -  model: " + serializedCriteria.model);
    console.log("PostgresJSONQueryStrategy.prototype.composeSingle - mapped fields: " + fieldsForSubqueriesMap.get(serializedCriteria.model));
    query.select= "SELECT " + fieldsForSubqueriesMap.get(serializedCriteria.model) + " FROM " + query.table;
    var tableAlias = previousOutput.lastPosition ? "" : " d";
    var tablePrefix = previousOutput.lastPosition ? "" : "d.";
    query.where = "WHERE " + tablePrefix + "type = $" + (++previousOutput.lastPosition);
    previousOutput.parameters.push(serializedCriteria.pivotDataType);
    var fieldQueries = [], value;
    if (serializedCriteria.content) {
        for (let i=0; i<serializedCriteria.content.length; i++) {
            let res, op, element = serializedCriteria.content[i];
            if (element.pivotDataType) {
                res = this.composeSingle(element, previousOutput, { alias: 'nested_'+(++previousOutput.cteCount)});
                previousOutput = res.previousOutput;
                query.subqueries.push(_.omit(res, 'previousOutput'));
            }
            else if (element.personalDetails) {
                res = this.composeSpecializedPersonalDetailsQuery(element, previousOutput);
                previousOutput = res.previousOutput;
                query.subqueries.push(_.omit(res, 'previousOutput'));
            }
            else if (element.specializedQuery) {
                op = this.composeSpecializedQuery(element, previousOutput, tablePrefix);
                if (!op) {
                    continue;
                }
                fieldQueries.push(op.subquery);
                previousOutput.lastPosition = op.lastParameterPosition;
                previousOutput.parameters = op.parameters;
            } 
            else {
                op = this.getSubqueryRow(element, previousOutput, tablePrefix);
                if (!op) {
                    continue;
                }
                fieldQueries.push(op.subquery);
            }
        }
    }
    fieldQueries = _.compact(fieldQueries); 
    if (fieldQueries.length) {
        var junction = serializedCriteria.junction === 'OR' ? 'OR' : 'AND';
        query.where += " AND (" + fieldQueries.map(function(row) {return "(" + row + ")"; }).join(" " + junction + " ") + ")";
    } 
    query.select += tableAlias; 
    // query.previousOutput =  _.extend(previousOutput, {parameters: _.flatten(previousOutput.parameters)});
    query.previousOutput = previousOutput;
    return query; // _.extend(previousOutput, {statement: query, parameters: _.flatten(previousOutput.parameters)});
};

/**
 * @name composeCommonTableExpression
 * @description given a list of sub-queries, the procedure stores them in a WITH statement (a.k.a Common Table Expression)
 * @return {Object} - ctes: the complete WITH statement
 */
PostgresJSONQueryStrategy.prototype.composeCommonTableExpression = function(query, ctes, parentAlias, parentTable) {
    if (!ctes) {
        ctes = [];
    }
    else if (query.alias === "pd") {  // PERSONAL_DETAILS table
        // var joinClause = "INNER JOIN " + query.alias + " ON " + query.alias + ".id = " + parentAlias + ".personal_info";
        ctes.push({
            alias: query.alias,
            commonTableExpression: query.alias + " AS (" + query.select + " " + query.where + ")",
            joinClause: "LEFT JOIN " + query.alias + " ON " + query.alias + ".id = " + parentAlias + ".personal_info"
        });
        return ctes;
    }
    else {
        // var model = "data";
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

/**
 * @method
 * @name compose
 * @description composes a query based on a single DataType
 */
PostgresJSONQueryStrategy.prototype.compose = function(serializedCriteria) {
    var query = this.composeSingle(serializedCriteria);
    var ctes = [];
    var specificFields = fieldsForMainQueryMap.get(serializedCriteria.model);

    // No subject and personal details info are required if querying on subjects 
    if (serializedCriteria.model !== DataTypeClasses.SUBJECT && serializedCriteria.wantsSubject) {
        ctes.push({ alias: 's', commonTableExpression: 's AS (SELECT id, code, sex FROM subject)', joinClause: 'LEFT JOIN s ON s.id = d.parent_subject'});
        specificFields += "s.code, s.sex, ";

        if (serializedCriteria.wantsPersonalInfo) {
            ctes.push({ 
                alias: 'pd', 
                commonTableExpression: 'pd AS (SELECT id, given_name, surname, birth_date FROM personal_details)', 
                joinClause: 'LEFT JOIN pd ON pd.id = d.personal_info'
            });
            specificFields += "pd.given_name, pd.surname, pd.birth_date, ";
        }
    }
    /*
    var ctes = serializedCriteria.model === DataTypeClasses.SUBJECT || !serializedCriteria.getSubject ? [] : [ 
        { alias: 's', commonTableExpression: 's AS (SELECT id, code, sex FROM subject)', joinClause: 'LEFT JOIN s ON s.id = d.parent_subject'}, 
        { alias: 'pd', commonTableExpression: 'pd AS (SELECT id, given_name, surname, birth_date FROM personal_details)', 
            joinClause: 'LEFT JOIN pd ON pd.id = d.personal_info'}
    ]; */

    ctes = ctes.concat(this.composeCommonTableExpression(query));
    console.log(ctes);
    var commonTableExpressions = "", joins = " ";
    // var specificFields = fieldsForMainQueryMap.get(serializedCriteria.model);
    /*
       var specificFields = (serializedCriteria.model === DataTypeClasses.SUBJECT) ? 'd.code, d.sex, ' :
       (serializedCriteria.model === DataTypeClasses.SAMPLE) ? 'd.biobank_code, ' : ''; */
    query.select = "SELECT DISTINCT d.id, " + specificFields + "d.metadata FROM " + query.table + " d";

    if (ctes.length) {
        // query.select = "SELECT DISTINCT d.id, " + specificFields + "d.metadata FROM " + query.table + " d";
        commonTableExpressions = "WITH " + _.pluck(ctes, 'commonTableExpression').join(", ");
        joins = " " + _.pluck(ctes, 'joinClause').join(" ") + " ";
    }
    var mainStatement = query.select + joins + query.where;
    mainStatement = (commonTableExpressions + " " + mainStatement).trim() + ";";
    return { statement: mainStatement, parameters: query.previousOutput.parameters };
};

module.exports = PostgresJSONQueryStrategy;
