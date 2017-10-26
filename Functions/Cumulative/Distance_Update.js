var dist = db.JUP_DB_Leg_Distance.find();

var _objDist = {};

dist.forEach(function(x){
    _objDist[x.LEG] = Number((x["Leg Distance"]+"").replace('.','')) 
})

db.JUP_DB_OD_Master.find({}).forEach(function(x){
    if (x.OD.includes('DXB') || x.OD.includes('DWC') ){
            
        //print("LEG   "+x.OD+"   "+_objDist[x.OD]);

        db.JUP_DB_OD_Distance_Master.insert({'od':x.OD,'distance':_objDist[x.OD]})
    }
    else{
        // Leg 1
        var dist = _objDist[x.OD.substring(0,3)+"DXB"];
        
        // Leg 2
        dist += _objDist["DXB"+x.OD.substring(3,6)];
        //print("OD   "+x.OD.substring(0,3)+"   "+x.OD.substring(3,6)+"   "+dist);
        db.JUP_DB_OD_Distance_Master.insert({'od':x.OD,'distance':dist})
    }
    
})
