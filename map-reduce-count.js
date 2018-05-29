db = db.getSiblingDB('VOTES_IN_PARLIAMENT')

db.createCollection("ld_vote_results")
db.createCollection("lab_vote_results")
db.createCollection("con_vote_results")


var mapFunction = function() {
    for (k in this.votes) {
        keyname = this.votes[k]; 
        val = {}; 
        val[keyname] = 1;
        emit ( k, val );}};

 
var reduceFunction = function(k, values) {
    result = { }; 
    values.forEach ( function(v) {
        for (k in v) {
                if (result[k] > 0) { 
                    result[k] += v[k];
                } else {
                    result[k] = v[k];}
        }} );
    return result;
};
 
db.runCommand( {
    mapReduce: "mp_lib_dem",
    map: mapFunction, 
    reduce: reduceFunction, 
    out: {replace: "ld_vote_results"}   

})

db.runCommand( {
    mapReduce: "mp_lab",
    map: mapFunction, 
    reduce: reduceFunction, 
    out: {replace: "lab_vote_results"}   

})

db.runCommand( {
    mapReduce: "mp_con",
    map: mapFunction, 
    reduce: reduceFunction, 
    out: {replace: "con_vote_results"}   

})