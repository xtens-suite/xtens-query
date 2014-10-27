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
}

PostgresJSONQueryStrategy.prototype.compose = function(serializedCriteria) {
    var query = "SELECT * FROM data WHERE type = $1";
    var parameters = [ serializedCriteria.pivotDataType ];
    var lastPosition = 1;
    if (serializedCriteria.content) {
        query += " AND (";
        var len = serializedCriteria.content.length;
        for (var i=0; i<len; i++) {
            var element = serializedCriteria.content[i];
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
        query += ")";
    }
    query += ";";
    console.log(query);
    return {statement: query, parameters: parameters};
};

module.exports = PostgresJSONQueryStrategy;
