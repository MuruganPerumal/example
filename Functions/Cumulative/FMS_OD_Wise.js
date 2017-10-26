db.JUP_DB_Host_OD_Capacity.aggregate([

    // Take matched od from host capacitty
    {$match:{
            'od':{$in:[
    "DMMHYD",
    "GYDDXB"]}
    }},

    // group the OD capacity based on particular market, month and year
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

    // add compartment wise rows which we have to generate because Host od capacity collection has all compartments data in single row
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
    
    // add pos in manually with the combination of origin and destination
      {"$addFields":{"pos":['$origin','$destination']}},
      {$unwind:{path: "$pos", preserveNullAndEmptyArrays: true }},
    
    // get competitor capacity from OD_capacity collection 
    {$lookup:{
       from: 'JUP_DB_OD_Capacity',
       localField: 'od',
       foreignField: 'od',
       as: 'compCapa'
    }},   
    
    // put the condition for matching of month, year and comp
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
   
    
   // make a proper columns 
    {$project:{
            _id:0,
            pos:'$pos',
            origin:'$origin',
            destination:'$destination',
            od:'$od',
            month:'$month',
            year:'$year',
            compartment:'$compartment',
            hostCapacity:'$hostCapacity',
            compCapa:'$compCapa',
            compCapa1:{$cond:[{$gt:[{$size:{$filter: {
               input: "$compCapa",
               as: "item",
               cond: { $eq: [ "$$item.airline", 'FZ' ] }
            }}},0]},
            []
            ,
             { 
              //  $slice: [ "$compCapa", 1 ]
                 $arrayElemAt: [ "$compCapa", 0 ]
                 
             }
            ]}
            
    }},
    
     
   // make a proper columns 
    {$project:{
            _id:0,
            pos:'$pos',
            origin:'$origin',
            destination:'$destination',
            od:'$od',
            month:'$month',
            year:'$year',
            compartment:'$compartment',
            hostCapacity:'$hostCapacity',
          //  compCapa:'$compCapa',
            compCapa1:'$compCapa1',
//             compArray:{ airline:{$concat:['FZ']}
//                 capacity:'$compCapa1.capacity'
//                 },
            compProj:{$cond:[{$ne:['$compCapa1',[]]},
                {$ifNull : ["$links" , [{airline:{$concat:['FZ']},
                capacity:'$compCapa1.capacity'}]]}    
                ,
                []
                ]},
        
                
            compCapa: { $concatArrays: [ "$compCapa", {$cond:[{$ne:['$compCapa1',[]]},
                {$ifNull : ["$links" , [{airline:{$concat:['FZ']},
                capacity:'$compCapa1.capacity'}]]}    
                ,
                []
                ]} ] } 
            
            
    }},
    
     // unwind compcapa for airlinewise data and rows 
  {$unwind:{path: "$compCapa", preserveNullAndEmptyArrays: true }},
   
    // make a proper columns 
{$project:{
            _id:0,
            pos:'$pos',
            origin:'$origin',
            destination:'$destination',
            od:'$od',
            month:'$month',
            year:'$year',
            compartment:'$compartment',
            hostCapacity:'$hostCapacity',
            // Taking capacity for all the airline which present in competitor capacity.. In case if no data for competitor 
            // this row will take host data also one of the market based on that we can compute FMS
            compAirline:{$ifNull:['$compCapa.airline','FZ']},
            compCapacity:{$ifNull:[{$cond:[{$eq:['$compCapa.airline','FZ']},
                                    '$hostCapacity',
                                    '$compCapa.capacity'
                                    ]},'$hostCapacity']},
                                    
    }},
    
//group based on pos and OD and compartment because one competitor may have two three rows of data also so sum it up
    {$group:{
        _id:{
                pos:'$pos',
                origin:'$origin',
                destination:'$destination',
                od:'$od',
                month:'$month',
                year:'$year',
                compAirline:'$compAirline',
                compartment:'$compartment'
            },
            hostCapacity:{$first:'$hostCapacity'},
            compCapacity:{$sum:'$compCapacity'},
        }},

    
    // group based on pos and OD. Anymore we don't need compartment so group it accordingly
    {$group:{
        _id:{
                pos:'$_id.pos',
                origin:'$_id.origin',
                destination:'$_id.destination',
                od:'$_id.od',
                month:'$_id.month',
                year:'$_id.year',
                compAirline:'$_id.compAirline'
               
            },
            hostCapacity:{$sum:'$hostCapacity'},
            compCapacity:{$sum:'$compCapacity'},
        }},
    
     // adding rating column for all the airline   
    {$lookup:{
       from: 'JUP_DB_Competitor_Ratings',
       localField: '_id.od',
       foreignField: 'od',
       as: 'compRating'
    }},   
   
    {$unwind:{path: "$compRating", preserveNullAndEmptyArrays: true }},       
    
{$project:{
            pos:'$_id.pos',
            origin:'$_id.origin',
            destination:'$_id.destination',
            od:'$_id.od',
            month:'$_id.month',
            year:'$_id.year',
            hostCapacity:'$hostCapacity',
            compRating:'$compRating',
            // Taking host rating saperatly which is for compute the calculation
            hostRating_set:{$filter: {
                input: '$compRating.rating_for_DB',
                as: 'shape',
                cond: {$eq: ['$$shape.airline', 'FZ']}
            }},
            compRating_sep: {$filter: {
                input: '$compRating.rating_for_DB',
                as: 'shape',
                cond: {$eq: ['$$shape.airline', '$_id.compAirline']}
            }},
            compAirline:'$_id.compAirline',
            compCapacity:'$compCapacity',
                                    
    }},
    
    // unwind array into documents 
    {$unwind:{path: "$compRating_sep", preserveNullAndEmptyArrays: true }},
    {$unwind:{path: "$hostRating_set", preserveNullAndEmptyArrays: true }},
       
{$project:{
            pos:'$pos',
            origin:'$origin',
            destination:'$destination',
            od:'$od',
            month:'$month',
            year:'$year',
            hostCapacity:'$hostCapacity',
            compRating:'$compRating',
            // In host or competitor if we don't have rating for the airline by default we can take 1 as rating... said Ashok
            compRating_sep: {$ifNull:[{$cond:[{$ne:['$compRating_sep.airline',null]},
                                    '$compRating_sep.rating',
                                    1
                                    ]},1]},
            hostRating_set: {$ifNull:[{$cond:[{$ne:['$hostRating_set.airline',null]},
                                    '$hostRating_set.rating',
                                    1
                                    ]},1]},                        
            compAirline:'$compAirline',
            compCapacity:'$compCapacity',
                                    
    }},
    
{$project:{
            pos:'$pos',
            origin:'$origin',
            destination:'$destination',
            od:'$od',
            month:'$month',
            year:'$year',
            hostCapacity:'$hostCapacity',
            compRating:'$compRating',    
            compRating_sep: '$compRating_sep',
            hostRating_set: '$hostRating_set',                        
            compAirline:'$compAirline',
            compCapacity:'$compCapacity',
            // getting competitor capacity * rating for each airline
            comp_ratVScap:{$multiply:["$compCapacity","$compRating_sep"]},
                                    
    }},
     
    {$group:{
        _id:{
            pos:'$pos',
            origin:'$origin',
            destination:'$destination',
            od:'$od',
            month:'$month',
            year:'$year',
            hostRating_set:'$hostRating_set',
            hostCapacity:'$hostCapacity'
            },
         // summing it up all the airline capacity into one value then only we can apply FMS formula
         comp_ratVScap:{$sum:'$comp_ratVScap'},
         compCapacity:{$sum:'$compCapacity'},
         compRating:{$sum:'$compRating_sep'},
        }},
    
{$project:{
            pos:'$_id.pos',
            origin:'$_id.origin',
            destination:'$_id.destination',
            od:'$_id.od',
            month:'$_id.month',
            year:'$_id.year',
            hostCapacity:'$_id.hostCapacity',
            hostRating_set: '$_id.hostRating_set',                        
            comp_ratVScap:'$comp_ratVScap',
            host_ratVScap:{$multiply:['$_id.hostRating_set','$_id.hostCapacity']},
            compCapacity:'$compCapacity',
            compRating_sep:'$compRating',
            // Taking OD Wise FMS
            FMS:{$cond:[{$ne:['$comp_ratVScap',0]},
                    {$divide:[
                    {$multiply:[{$multiply:['$_id.hostRating_set','$_id.hostCapacity']},100]},
                    '$comp_ratVScap'
                    ]}
                    ,
                        0
                    ]
                }

    }},
    // grouping for only pos, month, year combination 
//     {$group:{
//         _id:{
//             pos:'$pos',
//             month:'$month',
//             year:'$year',
//             },
//             hostCapacity:{$sum:'$hostCapacity'},
//             hostRating_set:{$sum:'$hostRating_set'} ,                        
//             comp_ratVScap:{$sum:'$comp_ratVScap'},
//             host_ratVScap:{$sum:'$host_ratVScap'},
//             compCapacity:{$sum:'$compCapacity'},
//             compRating_sep:{$sum:'$compRating_sep'},
//             FMS:{$avg:'$FMS'}
//         }},
//     {$project:{
//             pos:'$_id.pos',
//             month:'$_id.month',
//             year:'$_id.year',
//             hostCapacity:'$hostCapacity',
//             hostRating_set:'$hostRating_set',                        
//             comp_ratVScap:'$comp_ratVScap',
//             host_ratVScap:'$host_ratVScap',
//             compCapacity:'$compCapacity',
//             compRating_sep:'$compRating_sep',
//             FMS:'$FMS'
//         }},
//     
    {$out:"JUP_DB_FMS_OD_Level"}
    ], {
  allowDiskUse:true,
  cursor:{}
 })
 
 