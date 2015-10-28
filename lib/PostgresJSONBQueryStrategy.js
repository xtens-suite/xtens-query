/**
 * @author Massimiliano Izzo
 * @description a method to composed a query on JSON/JSONB metadata stored within 
 *              the XTENS repository (see https://github.com/biolab-unige/xtens-app)
 */

/*jshint esnext: true */
/*jshint node: true */

"use strict";
const DataTypeClasses = require("./Utils").DataTypeClasses;
const FieldTypes = require("./Utils.js").FieldTypes;
var _ = require("lodash");
var extend = require("./Utils.js").extend;
var PostgresJSONQueryStrategy = require("./PostgresJSONQueryStrategy.js");
var determineTableByModel = require("./Utils.js").determineTableByModel;
var allowedComparators = require("./Utils.js").allowedComparators;
var specializedProperties = require("./Utils.js").specializedProperties;
var pdProperties = require("./Utils.js").pdProperties;

var queryOutput = {
    lastPosition: 0,
    cteCount: 0,
    parameters: []
};

/**
 * @class PostgresJSONBQueryStrategy
 * @extends PostgresJSONQueryStrategy
 * @description strategy method to compose a query against PostgreSQL JSONB data format where metadata are stored
 */
function PostgresJSONBQueryStrategy() {
    
    /**
     * @method
     * @name getSubqueryRow
     * @description compose a (sub)query fragment based a a single criterium (a single paeameter and a condition
     *              over the parameter)
     */     
    this.getSubqueryRow = function(element, previousOutput, tablePrefix) {
        
        if(_.isEmpty(element)) {
            return null;
        }

        if (allowedComparators.indexOf(element.comparator) < 0) {
            console.log(element.comparator);
            throw new Error("Operation not allowed. Trying to inject a forbidden comparator!!");
        }

        if (element.isInLoop) {
            console.log("PostgresJSONBQueryStrategy - executing loop composition algorithm - " + element.isInLoop);
            return this.getSubqueryRowLoop(element, previousOutput, tablePrefix);
        }

        return this.getSubqueryRowAttribute(element, previousOutput, tablePrefix);
    };
    

    /**
     * @method
     * @name getSubqueryRowAttribute
     * @description
     */
    this.getSubqueryRowAttribute = function(element, previousOutput, tablePrefix) {

        let boolValue, i, subquery = "", subqueries = [], param = {}, operatorPrefix;

        if (element.fieldType === "boolean") {
            boolValue = _.isBoolean(element.fieldValue) ? element.fieldValue : (element.fieldValue.toLowerCase() === 'true');
            // param = "{\"" + element.fieldName + "\":{\"value\":" + boolValue + "}}";
            // param = {}; 
            param[element.fieldName] = {value: boolValue}; 
            subquery = tablePrefix + "metadata @> $" + (++previousOutput.lastPosition);
            previousOutput.parameters.push(JSON.stringify(param));
        }

        else if (element.isList) {
            // if the comparator has a not condition add it a prefix
            operatorPrefix = element.comparator === 'NOT IN' ? 'NOT ' : '';

            subqueries = [];
            for (i=0; i<element.fieldValue.length; i++) {
                param = "{\"" + element.fieldName + "\":{\"value\":\"" + element.fieldValue[i] + "\"}}";
                subqueries.push(operatorPrefix + tablePrefix + "metadata @> $" + (++previousOutput.lastPosition));
                previousOutput.parameters.push(param);
            }
            // join all conditions from a list with or
            subquery = subqueries.join(" OR ");
        }
        
        // if it is an equality matching use JSONB containment (@>) operator 
        else if (element.comparator === '=' || element.comparator === '<>') { // NOTE: "!=" operator is not allowed since it is not standard SQL

            // if the comparator has an inequality condition add a NOT a prefix
            operatorPrefix = element.comparator === '<>' ? 'NOT ' : '';
            
            let value = element.fieldType === FieldTypes.INTEGER ? _.parseInt(element.fieldValue) :
               element.fieldType === FieldTypes.FLOAT ? Number(element.fieldValue) : 
               element.caseInsensitive ? element.fieldValue.toUpperCase() : element.fieldValue;

            // param = {};
            param[element.fieldName] = {value: value};
            subquery = operatorPrefix + tablePrefix + "metadata @> $" + (++previousOutput.lastPosition);
            previousOutput.parameters.push(JSON.stringify(param));
        }

        // otherwise use the standard JSON/JSONB accessor (->/->>) operator
        else {
            subquery = "(" + tablePrefix + "metadata->$" + (++previousOutput.lastPosition) + "->>'value')::" + element.fieldType.toLowerCase()  + 
                " " + element.comparator + " $" + (++previousOutput.lastPosition);
            previousOutput.parameters.push(element.fieldName);
            previousOutput.parameters.push(element.fieldValue);
        }
        
        // add condition on unit if present
        if (element.fieldUnit) {
            param = "{\"" + element.fieldName + "\":{\"unit\":\"" + element.fieldUnit + "\"}}";
            subquery += " AND ";
            subquery +=  tablePrefix + "metadata @> $" + (++previousOutput.lastPosition);
            previousOutput.parameters.push(param);
        }
        // flatten nested arrays (if any)
        console.log(previousOutput.parameters);
        previousOutput.parameters = _.flatten(previousOutput.parameters);
        return {subquery: subquery, previousOutput: previousOutput};

    };
    
    /*
    this.getSubqueryRowLoop = function(element, previousOutput, tablePrefix) {
        
        let subquery = "", operatorPrefix, value;

        if (element.comparator === '=' || element.comparator === '<>') {
            operatorPrefix = element.comparator === '<>' ? 'NOT ' : '';
            subquery = "(" + operatorPrefix + tablePrefix + "metadata->$" + (++previousOutput.lastPosition) + "->'values' ? $" + (++previousOutput.lastPosition) + ")"; 
            // if case-insensitive turn the value to uppercase
            value = element.caseInsensitive ? element.fieldValue.toUpperCase() : element.fieldValue;
        }

        else if (element.comparator === '?&' || element.comparator === '?|') {
            operatorPrefix = ""; // TODO: so far no operator prefix
            subquery = "(" + operatorPrefix + tablePrefix + "metadata->$" + (++previousOutput.lastPosition) + "->'values' " + element.comparator + " $" + (++previousOutput.lastPosition) + ")";
            // if case-insensitive turn all values to uppercase
            value = element.caseInsensitive ? _.map(element.fieldValue, el => el.toUpperCase()) : element.fieldValue;
        }
        
        
        previousOutput.parameters.push(element.fieldName);
        previousOutput.parameters.push(value);

        return {subquery: subquery, previousOutput: previousOutput};

    }; */

    this.getSubqueryRowLoop = function(element, previousOutput, tablePrefix) {
        
        let subquery = "", operatorPrefix, jsonbValue = {};

        if (element.comparator === '=' || element.comparator === '<>') {
            operatorPrefix = element.comparator === '<>' ? 'NOT ' : '';
            subquery = operatorPrefix + tablePrefix + "metadata @> $" + (++previousOutput.lastPosition);
            // if case-insensitive turn the value to uppercase
            let val = element.fieldType === FieldTypes.INTEGER ? _.parseInt(element.fieldValue) :
               element.fieldType === FieldTypes.FLOAT ? Number(element.fieldValue) : 
               element.caseInsensitive ? element.fieldValue.toUpperCase() : element.fieldValue;
            jsonbValue[element.fieldName] = {values: [val]};
        }
        
        // ALL VALUES operator
        else if (element.comparator === '?&') {
            operatorPrefix = element.comparator !== '?&' ? 'NOT ' : '';     // TODO so far no negative query implemented for this one
            subquery = operatorPrefix + tablePrefix + "metadata @> $" + (++previousOutput.lastPosition);
            let val = element.fieldType === FieldTypes.INTEGER ? _.map(element.fieldValue, el => _.parseInt(el)) :
                element.fieldType === FieldTypes.FLOAT ? _.map(element.fieldValue, el => Number(el)) :
                element.caseInsensitive ? _.map(element.fieldValue, el => el.toUpperCase()) : element.fieldValue;
            jsonbValue[element.fieldName] = {values: val};
        }
        
        // ANY OF THE VALUES operator
        else if (element.comparator === '?|') {
            operatorPrefix = ""; // TODO: so far no operator prefix
            subquery = "(" + operatorPrefix + tablePrefix + "metadata->$" + (++previousOutput.lastPosition) + "->'values' " + element.comparator + " $" + (++previousOutput.lastPosition) + ")";
            // if case-insensitive turn all values to uppercase
            jsonbValue = element.caseInsensitive ? _.map(element.fieldValue, el => el.toUpperCase()) : element.fieldValue;
            previousOutput.parameters.push(element.fieldName);
        }
        
        previousOutput.parameters.push(_.isArray(jsonbValue) ? jsonbValue : JSON.stringify(jsonbValue));

        return {subquery: subquery, previousOutput: previousOutput};

    };


}

// The JSONB query stategy extends the basic JSON strategy
extend(PostgresJSONBQueryStrategy,PostgresJSONQueryStrategy);

module.exports = PostgresJSONBQueryStrategy;
