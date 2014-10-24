/**
 * @author Massimiliano Izzo
 * @description a method to composed a query on JSON/JSONB metadata stored within 
 *              the XTENS repository (see https://github.com/biolab-unige/xtens-app)
 */

function PostgresJSONQueryStrategy() {
    this.getSubqueryRow = function(element, index) {
        var nameParam = '$'+(index*3+2), valueParam = '$'+(index*3+3), unitParam = '$'+(index*3+4);
        var fieldValueOpener = element.isList ? " (" : " ";
        var fieldValueCloser = element.isList ? ")" : "";
        var subquery = "(metadata->" + nameParam + "->'value'->>0)::" + element.fieldType.toLowerCase()  + 
            " " + element.comparator + fieldValueOpener + valueParam + fieldValueCloser;
        if (element.fieldUnit) {
            subquery += " AND ";
            subquery += "(metadata->" + nameParam + "->'unit'->>0)::text = " + unitParam;
        }
        return subquery;
    };
}

PostgresJSONQueryStrategy.prototype.compose = function(serializedCriteria) {
    var query = "SELECT * FROM data WHERE type = $1";
    var parameters = [ serializedCriteria.pivotDataType.id ];
    if (serializedCriteria.content) {
        query += " AND (";
        var len = serializedCriteria.content.length;
        for (var i=0; i<len; i++) {
            var element = serializedCriteria.content[i];
            query += this.getSubqueryRow(element, i);
            // var fieldUnit = element.fieldUnit || '';
            parameters.push(element.fieldName, element.fieldValue, element.fieldUnit || '');
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
