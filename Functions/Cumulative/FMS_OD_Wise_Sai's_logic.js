
db.JUP_DB_Host_OD_Capacity.aggregate([
 {$match:{
     'od':{$eq:'DXBDOH'}
     }},
    {$group:{
        _id:{
                origin:'$origin',
                destination:'$destination',
                od:'$od',
                month:'$month',
                year:'$year'
            },
        j_cap:{$sum:'$j_cap'},
        y_cap:{$sum:'$y_cap'},
        od_capacity:{$sum:'$od_capacity'},
    }},
    {"$addFields":{"compartment":['Y','J']}},
    {$unwind:{path: "$compartment", preserveNullAndEmptyArrays: true }},
    {$project:{
            origin:'$_id.origin',
            destination:'$_id.destination',
            od:'$_id.od',
            month:'$_id.month',
            year:'$_id.year',
            j_cap:'$j_cap',
            y_cap:'$y_cap',
            od_capacity:'$od_capacity',
            compartment:'$compartment',
            hostCapacity:{$cond:[{$eq:['$compartment','Y']},'$y_cap','$j_cap']}
        }},
      {"$addFields":{"pos":['$origin','$destination']}},
      {$unwind:{path: "$pos", preserveNullAndEmptyArrays: true }},
     {$lookup:{
       from: 'JUP_DB_OD_Capacity',
       localField: 'od',
       foreignField: 'od',
       as: 'compCapa'
    }},   
    
    {$addFields:{
        compCapa:{ $filter: {
             input: "$compCapa",
             as: "config",
             cond: { $and: [
                { $eq: [ "$$config.month", "$month" ] },
                { $eq: [ "$$config.year", "$year" ] },
                { $eq: [ "$$config.compartment", "$compartment" ] },
                ] }
        }}
    }},
    {$unwind:{path: "$compCapa", preserveNullAndEmptyArrays: true }},
    {$group:{
        _id:{
            pos:'$pos',
            origin:'$_id.origin',
            destination:'$_id.destination',
            od:'$_id.od',
            month:'$_id.month',
            year:'$_id.year',
            compartment:'$compartment',
            hostCapacity:'$hostCapacity'
            },
            competitorCapacity:{$push:{
                airline:'$compCapa.airline',
                capacity:'$compCapa.od_capacity'
                }}
        }},

    {$lookup:{
       from: 'JUP_DB_Competitor_Ratings',
       localField: '_id.od',
       foreignField: 'od',
       as: 'compRating'
    }},   
   
    {$unwind:{path: "$competitorCapacity", preserveNullAndEmptyArrays: true }},
    
    {$unwind:{path: "$compRating", preserveNullAndEmptyArrays: true }},
    
     {$project:{
            pos:'$_id.pos',
            origin:'$_id.origin',
            destination:'$_id.destination',
            od:'$_id.od',
            month:'$_id.month',
            year:'$_id.year',
            compartment:'$_id.compartment',
            hostCapacity:'$_id.hostCapacity',
            competitorCapacity:'$competitorCapacity',
            compRating:'$compRating.rating_for_DB',
            competitorCapacity:{
                airline:'$competitorCapacity.airline',
                capacity:{$cond:[{$eq:["$competitorCapacity.airline",'FZ']},'$_id.hostCapacity','$competitorCapacity.capacity']}
            },
            HostRating:{$cond:[{$eq:[{$filter: {
            input: '$compRating.rating_for_DB',
            as: 'shape',
            cond: {$eq: ['$$shape.airline', 'FZ']}}},null]},5,{$filter: {
            input: '$compRating.rating_for_DB',
            as: 'shape',
            cond: {$eq: ['$$shape.airline', 'FZ']}}}]} ,
        
        
    }},
     
    {$unwind:{path: "$compRating", preserveNullAndEmptyArrays: true }},
    
    {$project:{
            pos:'$pos',
            origin:'$origin',
            destination:'$destination',
            od:'$od',
            month:'$month',
            year:'$year',
            compartment:'$compartment',
            hostCapacity:'$hostCapacity',
            competitorCapacity:'$competitorCapacity',
            compRating:'$compRating',
            competitorCapacity:'$competitorCapacity',
            HostRating:'$HostRating'
        
    }},
    
    {$unwind:{path: "$HostRating", preserveNullAndEmptyArrays: true }},

    {$group:{
            _id:{
                    pos:'$pos',
                    origin:'$origin',
                    destination:'$destination',
                    od:'$od',
                    month:'$month',
                    year:'$year',
                    compartment:'$compartment',
                    hostCapacity:'$hostCapacity',
                    HostRating:'$HostRating'
                },
                _CompVSrating :{$sum:{
                    $cond:[{$eq:["$compRating.airline", '$competitorCapacity.airline' ]},
                    {$multiply:['$competitorCapacity.capacity',{$cond:[{$eq:["$compRating.rating",null]},5,'$compRating.rating']}]}, // If no rating for any airline give 5 as default as per Sai
                    5]
                    }},
               // HostRating:{$avg:'$HostRating'}
                
    }},

    {$project:{
            pos:'$_id.pos',
            origin:'$_id.origin',
            destination:'$_id.destination',
            od:'$_id.od',
            month:'$_id.month',
            year:'$_id.year',
            compartment:'$_id.compartment',
            hostCapacity:'$_id.hostCapacity',
             _CompVSrating:'$_CompVSrating',
            HostRating:'$_id.HostRating',
            HostCapaVsRating:{$multiply:["$_id.hostCapacity","$_id.HostRating"]},
            FMS:{$cond:[{$ne:['$_CompVSrating',0]},{$divide:[
                {$multiply:[{$multiply:["$_id.hostCapacity","$_id.HostRating"]},100]},
                '$_CompVSrating'
                ]}
                ,{$divide:[
                {$multiply:[{$multiply:["$_id.hostCapacity","$_id.HostRating"]},100]},
                {$multiply:["$_id.hostCapacity","$_id.HostRating"]}
                ]}]
            }
            
        
    }},

   // {$out:"JUP_DB_FMS_OD_Level"}

    ], {
  allowDiskUse:true,
  cursor:{}
 })