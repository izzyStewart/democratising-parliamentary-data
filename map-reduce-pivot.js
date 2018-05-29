db = db.getSiblingDB('VOTES_IN_PARLIAMENT')

var mapFunction = function() {
        var obj = {};
        obj[this._id] = this.score;        
        emit(this._id, obj);};

var reduceFunction = function(key, values) {
        var obj = {};
        values.forEach(function(value) {
            Object.keys(value).forEach(function(key) {                
                obj[key] = value[key];
            });
        });
        return obj;};


 
db.runCommand( {
    mapReduce: "lab_score",
    map: mapFunction, 
    reduce: reduceFunction, 
    out: {replace: "lab_score"}   

})

db.runCommand( {
    mapReduce: "ld_score",
    map: mapFunction, 
    reduce: reduceFunction, 
    out: {replace: "ld_score"}   

})

db.runCommand( {
    mapReduce: "con_score",
    map: mapFunction, 
    reduce: reduceFunction, 
    out: {replace: "con_score"}   

})




db.runCommand( {
    mapReduce: "lab_maj_score",
    map: mapFunction, 
    reduce: reduceFunction, 
    out: {replace: "lab_maj_score"}   

})

db.runCommand( {
    mapReduce: "ld_maj_score",
    map: mapFunction, 
    reduce: reduceFunction, 
    out: {replace: "ld_maj_score"}   

})

db.runCommand( {
    mapReduce: "con_maj_score",
    map: mapFunction, 
    reduce: reduceFunction, 
    out: {replace: "con_maj_score"}   

})




