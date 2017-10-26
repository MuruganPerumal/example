var destDB = db.getSiblingDB('fzDB_stg')

db.getCollectionNames().forEach(function(x){
    var index = db[x].getIndexes();
        index.forEach(function(y){
           // print(y.key)
            destDB[x].createIndex(y.key)
        })
})


// Single collection
db.getCollectionNames().forEach(function(x){
    var index = db[x].getIndexes();
	if(x == "JUP_DB_Market_Share_old"){
		index.forEach(function(y){
           // print(y.key)
            db['JUP_DB_Market_Share'].createIndex(y.key)
        })
	}
})

