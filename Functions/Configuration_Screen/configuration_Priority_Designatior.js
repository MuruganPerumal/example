


db.getCollection('JUP_DB_Competitor_Config').find({}).forEach(function(x){

    var getFullDoc = db.getCollection('JUP_DB_Competitor_Priority').find({
            "POS":x.pos.level,
            "Origin" : x.origin.level,
            "Destination" : x.destination.level,
            "Class":x.compartment.level
        }).sort({"Final_Priority" : -1}).limit(1).forEach(function(y){
                //print(y)
                
                db.JUP_DB_Competitor_Config.update({
                    _id:x._id
                    },{
                            $set:{
                                  priority:y.Final_Priority      
                            }
                        })
            
            })
     
    })