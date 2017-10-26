// Sales
// db.JUP_DB_Sales.find()
db.JUP_DB_Sales.aggregate([
 {$group:{
    _id:{
        "Network_Dest" : '$Network_Dest',
        "Region_Dest" : '$Region_Dest',
        "Country_Dest" : '$Country_Dest',
        "Cluster_Dest" : '$Cluster_Dest',
        "Network_Origin" : '$Network_Origin',
        "Cluster_Origin" : '$Cluster_Origin',
        "Country_Origin" : '$Country_Origin',
        "Region_Origin" : '$Region_Origin',
        "Cluster" : '$Cluster',
        "region" : '$region',
        "country" : '$country',
        "pos" : '$pos',
        "od" : '$od',
        "origin" : '$origin',
        "destination" : '$destination',
        'compartment': '$compartment',
        'dep_date': '$dep_date',
       // 'sale_date': '$book_date',
        'snap_date': '$snap_date',
        'year':'$year', 
        'month':'$month',
        'fare_basis':'$fare_basis',
        'RBD':'$RBD',
        'fareId':'$fare_basis',
        'combine_col':{$concat:['$dep_date','$od']}
        },
        'pax':{$sum:'$pax'},
        'pax_1':{$sum:'$pax_1'},
        'rev':{$sum:'$revenue'},
        'rev_1':{$sum:'$revenue_1'},
        
    }},
    {
        $addFields:{
                    paxWgt:60
            }
    },{
        $addFields:{
                    revWgt:40
            }
    },
    {
      $lookup:
         {
            from: "JUP_DB_Host_OD_Capacity",
            localField: "_id.combine_col",
            foreignField: "combine_column",
            as: "capacity"
        }
   },

{$unwind:{ path: "$capacity", preserveNullAndEmptyArrays: true }},   
  { $addFields:{
                    capa:{$cond:[{
                            $eq:['$_id.compartment','J']
                        },
                        { $ifNull: [ '$capacity.j_cap', 1 ] }
                        ,
                        { $ifNull: [ '$capacity.y_cap', 1 ] }
                        ]}
            }
    },
    { $addFields:{
                    capa_1:{$cond:[{
                            $eq:['$_id.compartment','J']
                        },
                        '$capacity.j_cap_1'
                        ,
                        '$capacity.y_cap_1'
                        ]}
            }
    },
    { $addFields:{
                    booking:{$cond:[{
                            $eq:['$_id.compartment','J']
                        },
                        { $ifNull: [ '$capacity.leg1_j_bookings', 1 ] }
                        ,
                        { $ifNull: [ '$capacity.leg1_y_bookings', 0 ] }
                        ]}
            }
    },
    { $addFields:{
                    booking_1:{$cond:[{
                            $eq:['$_id.compartment','J']
                        },
                        '$capacity.leg1_j_bookings_1'
                        ,
                        '$capacity.leg1_y_bookings_1'
                        ]}
            }
    },

// project few fields for computing vlyr of pax and revenue    
{$project:{
    'pax':'$pax',
    'pax_1':'$pax_1',
    'rev':'$rev',
    'rev_1':'$rev_1',
    'revWgt':'$revWgt',
    'paxWgt':'$paxWgt',
    'capacity':'$capa',
    'booking':'$booking',
    'booking_1':'$booking_1',
    'capacity_1':{$cond:[{
                            $eq:['$capa_1',undefined]
                        },
                        '$capa'
                        ,
                        '$capa_1'
                        ]},
    'pax_vlyr':{$multiply:[{
                      $cond:[
                      {$ne:['$pax_1',0]},
                      {$divide:[
                          {$subtract:[
                              {$multiply:[
                                  '$pax',
                  {$cond:[
                    {$ne:['$capa',0]},
                    {$divide:[
                    {$cond:[
                      {$eq:['$capa_1',undefined]},
                      '$capa',
                      '$capa_1']},
                    '$capa']},
                    0 ]}
                                  ]},
                              '$pax_1']},
                           '$pax_1']},         
                       1 
                        ]
                    },
                    100
                    ]},
        'rev_vlyr':{$multiply:[{
                      $cond:[
                      {$ne:['$rev_1',0]},
                      {$divide:[
                          {$subtract:[
                              {$multiply:[
                                  '$rev',
                  {$cond:[
                    {$ne:['$capa',0]},
                    {$divide:[
                    {$cond:[
                      {$eq:['$capa_1',undefined]},
                      '$capa',
                      '$capa_1']},
                    '$capa']},
                    0 ]}
                                  ]},
                              '$rev_1']},
                           '$rev_1']},         
                       1 
                        ]
                    },
                    100
                    ]},    }},    
    
{$project:{
    'pax':'$pax',
    'pax_1':'$pax_1',
    'rev':'$rev',
    'rev_1':'$rev_1',
    'revWgt':'$revWgt',
    'paxWgt':'$paxWgt',
    'pax_vlyr':'$pax_vlyr',
    'rev_vlyr':'$rev_vlyr',
    'capacity':'$capacity',
    'capacity_1':'$capacity_1',
    'booking':'$booking',
    'booking_1':'$booking_1',
    'effectivity_calc':{$cond:[
                        {$ne:[{$add:['$paxWgt','$revWgt']},0]},
                                
                        {$divide:[
                                    {$add:[
                                          {$multiply:['$pax_vlyr','$paxWgt']},
                                          {$multiply:['$rev_vlyr','$revWgt']}
                                          ]},
                                    {$add:['$paxWgt','$revWgt']}
                                  ]},
                          0
                        ]}
        
        ,
    'effectivity_flag':{$cond:[{$gt:[{$cond:[
                        {$ne:[{$add:['$paxWgt','$revWgt']},0]},
                                
                        {$divide:[
                                    {$add:[
                                          {$multiply:['$pax_vlyr','$paxWgt']},
                                          {$multiply:['$rev_vlyr','$revWgt']}
                                          ]},
                                    {$add:['$paxWgt','$revWgt']}
                                  ]},
                          0
                        ]},0]},true,false]}
    }},    
    
    {
      $lookup:
         {
            from: "JUP_DB_Booking_Class",
            localField: "_id.RBD",
            foreignField: "Code",
            as: "type"
        }
   },
   {$unwind:{ path: "$type", preserveNullAndEmptyArrays: true }},
    // group all farebasis level of pax and revenue 
{$group:{
    _id:{
        "Network_Dest" : '$_id.Network_Dest',
        "Region_Dest" : '$_id.Region_Dest',
        "Country_Dest" : '$_id.Country_Dest',
        "Cluster_Dest" : '$_id.Cluster_Dest',
        "Network_Origin" : '$_id.Network_Origin',
        "Cluster_Origin" : '$_id.Cluster_Origin',
        "Country_Origin" : '$_id.Country_Origin',
        "Region_Origin" : '$_id.Region_Origin',
        "Cluster" : '$_id.Cluster',
        "region" : '$_id.region',
        "country" : '$_id.country',
        "pos" : '$_id.pos',
        "od" : '$_id.od',
        "origin" : '$_id.origin',
        "destination" : '$_id.destination',
        'compartment': '$_id.compartment',
        'dep_date': '$_id.dep_date',
    //    'sale_date': '$_id.sale_date',
        'snap_date': '$_id.snap_date',
        'year':'$_id.year', 
        'month':'$_id.month',
                
                
            },
          sale_pax_fb:{$push:{
                fare_basis:'$_id.fare_basis',
                type:'$type.type',
                fareId:{ $concat: [ '$_id.fareId',, "null", "null" ] } ,
                pax:'$pax',
                pax_1:'$pax_1',
                rev:'$rev',
                rev_1:'$rev_1',
                pax_vlyr:'$pax_vlyr',
                rev_vlyr:'$rev_vlyr',
                effectivity_flag:'$effectivity_flag',
                effectivity_calc:'$effectivity_calc',
                capacity:'$capacity',
                capacity_1:'$capacity_1',
                booking:'$booking',
                booking_1:'$booking_1',
                }},
                capacity:{$max:'$capacity'},
                capacity_1:{$max:'$capacity_1'},

                booking:{$max:'$booking'},
                booking_1:{$max:'$booking_1'},

    }},    
    {$out:'temp_sale_farebssis'}

],{allowDiskUse:true})



// update Manual trigger table from sale farebasis collection
var num = 1;
var bulk = db.JUP_DB_Manual_Triggers_Module.initializeOrderedBulkOp();
db.temp_sale_farebssis.find().forEach(function(x){
  var od = x._id.origin+""+ x._id.destination;
  var pos ={};
  pos['Network'] = 'Network';
  pos['Cluster'] = x._id.Cluster;
  pos['Region'] = x._id.region;
  pos['Country'] = x._id.country;
  pos['City'] = x._id.pos;
  var origin = {};
  origin['Network'] = x._id.Network_Origin;
  origin['Cluster'] = x._id.Cluster_Origin;
  origin['Region'] = x._id.Region_Origin;
  origin['Country'] = x._id.Country_Origin;
  origin['City'] = x._id.origin;
  var destination = {};
  destination['Network'] = x._id.Network_Dest;
  destination['Cluster'] = x._id.Cluster_Dest;
  destination['Region'] = x._id.Region_Dest;
  destination['Country'] = x._id.Country_Dest;
  destination['City'] = x._id.destination;
  var compartment = {};
  compartment['compartment'] = x._id.compartment ;
  compartment ['all'] = 'all';
  
  var snap_month =Number( x._id.snap_date.substring(5,7));
  var snap_year = Number(x._id.snap_date.substring(0,4));
  var dep_month = Number(x._id.dep_date.substring(5,7));
  var dep_year = Number(x._id.dep_date.substring(0,4));
  

  bulk.find({
  	'pos.City':pos.City,
  	'origin.City':origin.City,
  	'destination.City':destination.City,
  	'compartment.compartment':compartment.compartment,
  	//snap_date:x._id.snap_date,
  	trx_date:x._id.snap_date,
  	dep_date:x._id.dep_date,

  	}).upsert().update(
            {
              $set:{
              	od:od,
      			  	pos:pos,
      			  	origin:origin,
      			  	destination:destination,
      			  	compartment:compartment,
      			  	//snap_date:x._id.snap_date,
      			  	trx_date:x._id.snap_date,
      			  	dep_date:x._id.dep_date,
        				trx_month:snap_month,
        				trx_year:snap_year,
        				dep_month:dep_month,
        				dep_year:dep_year,
      			  	sale_farebasis:x.sale_pax_fb,
                capacity:x.capacity,
                capacity_1:x.capacity_1,
                inventory:{
                  booking:x.booking,
                  booking_1:x.booking_1,
                }
              }
             }
        );

 if ( num % 1000 == 0 ){
        	bulk.execute();
        	bulk = db.JUP_DB_Manual_Triggers_Module.initializeOrderedBulkOp();
        }
        num++;

      })      
bulk.execute();



// Flown
db.JUP_DB_Sales_Flown.aggregate([
 {$group:{
    _id:{
        "Network_Dest" : '$Network_Dest',
        "Region_Dest" : '$Region_Dest',
        "Country_Dest" : '$Country_Dest',
        "Cluster_Dest" : '$Cluster_Dest',
        "Network_Origin" : '$Network_Origin',
        "Cluster_Origin" : '$Cluster_Origin',
        "Country_Origin" : '$Country_Origin',
        "Region_Origin" : '$Region_Origin',
        "Cluster" : '$Cluster',
        "region" : '$region',
        "country" : '$country',
        "pos" : '$pos',
        "od" : '$od',
        "origin" : '$origin',
        "destination" : '$destination',
        'compartment': '$compartment',
        'dep_date': '$dep_date',
       // 'sale_date': '$book_date',
        'snap_date': '$snap_date',
        'year':'$year', 
        'month':'$month',
        'fare_basis':'$fare_basis',
        'RBD':'$RBD',
        'fareId':'$fare_basis',
        'combine_col':{$concat:['$dep_date','$od']}
        },
        'pax':{$sum:'$pax'},
        'pax_1':{$sum:'$pax_1'},
        'rev':{$sum:'$revenue'},
        'rev_1':{$sum:'$revenue_1'},
        
    }},
    {
        $addFields:{
                    paxWgt:60
            }
    },{
        $addFields:{
                    revWgt:40
            }
    },
    {
      $lookup:
         {
            from: "JUP_DB_Host_OD_Capacity",
            localField: "_id.combine_col",
            foreignField: "combine_column",
            as: "capacity"
        }
   },

{$unwind:{ path: "$capacity", preserveNullAndEmptyArrays: true }},   
  { $addFields:{
                    capa:{$cond:[{
                            $eq:['$_id.compartment','J']
                        },
                        { $ifNull: [ '$capacity.j_cap', 1 ] }
                        ,
                        { $ifNull: [ '$capacity.y_cap', 1 ] }
                        ]}
            }
    },
    { $addFields:{
                    capa_1:{$cond:[{
                            $eq:['$_id.compartment','J']
                        },
                        '$capacity.j_cap_1'
                        ,
                        '$capacity.y_cap_1'
                        ]}
            }
    },
    { $addFields:{
                    booking:{$cond:[{
                            $eq:['$_id.compartment','J']
                        },
                        { $ifNull: [ '$capacity.leg1_j_bookings', 1 ] }
                        ,
                        { $ifNull: [ '$capacity.leg1_y_bookings', 0 ] }
                        ]}
            }
    },
    { $addFields:{
                    booking_1:{$cond:[{
                            $eq:['$_id.compartment','J']
                        },
                        '$capacity.leg1_j_bookings_1'
                        ,
                        '$capacity.leg1_y_bookings_1'
                        ]}
            }
    },

// project few fields for computing vlyr of pax and revenue    
{$project:{
    'pax':'$pax',
    'pax_1':'$pax_1',
    'rev':'$rev',
    'rev_1':'$rev_1',
    'revWgt':'$revWgt',
    'paxWgt':'$paxWgt',
    'capacity':'$capa',
    'booking':'$booking',
    'booking_1':'$booking_1',
    'capacity_1':{$cond:[{
                            $eq:['$capa_1',undefined]
                        },
                        '$capa'
                        ,
                        '$capa_1'
                        ]},
    'pax_vlyr':{$multiply:[{
                      $cond:[
                      {$ne:['$pax_1',0]},
                      {$divide:[
                          {$subtract:[
                              {$multiply:[
                                  '$pax',
                  {$cond:[
                    {$ne:['$capa',0]},
                    {$divide:[
                    {$cond:[
                      {$eq:['$capa_1',undefined]},
                      '$capa',
                      '$capa_1']},
                    '$capa']},
                    0 ]}
                                  ]},
                              '$pax_1']},
                           '$pax_1']},         
                       1 
                        ]
                    },
                    100
                    ]},
        'rev_vlyr':{$multiply:[{
                      $cond:[
                      {$ne:['$rev_1',0]},
                      {$divide:[
                          {$subtract:[
                              {$multiply:[
                                  '$rev',
                  {$cond:[
                    {$ne:['$capa',0]},
                    {$divide:[
                    {$cond:[
                      {$eq:['$capa_1',undefined]},
                      '$capa',
                      '$capa_1']},
                    '$capa']},
                    0 ]}
                                  ]},
                              '$rev_1']},
                           '$rev_1']},         
                       1 
                        ]
                    },
                    100
                    ]},    }},    
    
{$project:{
    'pax':'$pax',
    'pax_1':'$pax_1',
    'rev':'$rev',
    'rev_1':'$rev_1',
    'revWgt':'$revWgt',
    'paxWgt':'$paxWgt',
    'pax_vlyr':'$pax_vlyr',
    'rev_vlyr':'$rev_vlyr',
    'capacity':'$capacity',
    'capacity_1':'$capacity_1',
    'booking':'$booking',
    'booking_1':'$booking_1',
    'effectivity_calc':{$cond:[
                        {$ne:[{$add:['$paxWgt','$revWgt']},0]},
                                
                        {$divide:[
                                    {$add:[
                                          {$multiply:['$pax_vlyr','$paxWgt']},
                                          {$multiply:['$rev_vlyr','$revWgt']}
                                          ]},
                                    {$add:['$paxWgt','$revWgt']}
                                  ]},
                          0
                        ]}
        
        ,
    'effectivity_flag':{$cond:[{$gt:[{$cond:[
                        {$ne:[{$add:['$paxWgt','$revWgt']},0]},
                                
                        {$divide:[
                                    {$add:[
                                          {$multiply:['$pax_vlyr','$paxWgt']},
                                          {$multiply:['$rev_vlyr','$revWgt']}
                                          ]},
                                    {$add:['$paxWgt','$revWgt']}
                                  ]},
                          0
                        ]},0]},true,false]}
    }},    
    
    {
      $lookup:
         {
            from: "JUP_DB_Booking_Class",
            localField: "_id.RBD",
            foreignField: "Code",
            as: "type"
        }
   },
   {$unwind:{ path: "$type", preserveNullAndEmptyArrays: true }},
    // group all farebasis level of pax and revenue 
{$group:{
    _id:{
        "Network_Dest" : '$_id.Network_Dest',
        "Region_Dest" : '$_id.Region_Dest',
        "Country_Dest" : '$_id.Country_Dest',
        "Cluster_Dest" : '$_id.Cluster_Dest',
        "Network_Origin" : '$_id.Network_Origin',
        "Cluster_Origin" : '$_id.Cluster_Origin',
        "Country_Origin" : '$_id.Country_Origin',
        "Region_Origin" : '$_id.Region_Origin',
        "Cluster" : '$_id.Cluster',
        "region" : '$_id.region',
        "country" : '$_id.country',
        "pos" : '$_id.pos',
        "od" : '$_id.od',
        "origin" : '$_id.origin',
        "destination" : '$_id.destination',
        'compartment': '$_id.compartment',
        'dep_date': '$_id.dep_date',
    //    'sale_date': '$_id.sale_date',
        'snap_date': '$_id.snap_date',
        'year':'$_id.year', 
        'month':'$_id.month',
                
                
            },
          sale_pax_fb:{$push:{
                fare_basis:'$_id.fare_basis',
                type:'$type.type',
                fareId:{ $concat: [ '$_id.fareId',, "null", "null" ] } ,
                pax:'$pax',
                pax_1:'$pax_1',
                rev:'$rev',
                rev_1:'$rev_1',
                pax_vlyr:'$pax_vlyr',
                rev_vlyr:'$rev_vlyr',
                effectivity_flag:'$effectivity_flag',
                effectivity_calc:'$effectivity_calc',
                capacity:'$capacity',
                capacity_1:'$capacity_1',
                }},
                capacity:{$max:'$capacity'},
                capacity_1:{$max:'$capacity_1'},
                booking:{$max:'$booking'},
                booking_1:{$max:'$booking_1'},

    }},    
    {$out:'temp_sale_Flown_farebssis'}

],{allowDiskUse:true})




//update Manual tringger in flown part of farebasis data
var num = 1;
var bulk = db.JUP_DB_Manual_Triggers_Module.initializeOrderedBulkOp();
db.temp_sale_Flown_farebssis.find().forEach(function(x){
  var od = x._id.origin+""+ x._id.destination;
  var pos ={};
  pos['Network'] = 'Network';
  pos['Cluster'] = x._id.Cluster;
  pos['Region'] = x._id.region;
  pos['Country'] = x._id.country;
  pos['City'] = x._id.pos;
  var origin = {};
  origin['Network'] = x._id.Network_Origin;
  origin['Cluster'] = x._id.Cluster_Origin;
  origin['Region'] = x._id.Region_Origin;
  origin['Country'] = x._id.Country_Origin;
  origin['City'] = x._id.origin;
  var destination = {};
  destination['Network'] = x._id.Network_Dest;
  destination['Cluster'] = x._id.Cluster_Dest;
  destination['Region'] = x._id.Region_Dest;
  destination['Country'] = x._id.Country_Dest;
  destination['City'] = x._id.destination;
  var compartment = {};
  compartment['compartment'] = x._id.compartment ;
  compartment ['all'] = 'all';
  
  var snap_month =Number( x._id.snap_date.substring(5,7));
  var snap_year = Number(x._id.snap_date.substring(0,4));
  var dep_month = Number(x._id.dep_date.substring(5,7));
  var dep_year = Number(x._id.dep_date.substring(0,4));
  

  
  	bulk.find({
  	'pos.City':pos.City,
  	'origin.City':origin.City,
  	'destination.City':destination.City,
  	'compartment.compartment':compartment.compartment,
  	//snap_date:x._id.snap_date,
  	trx_date:x._id.snap_date,
  	dep_date:x._id.dep_date,

  	}).upsert().update(
            {
              $set:{
              	od:od,
      			  	pos:pos,
      			  	origin:origin,
      			  	destination:destination,
      			  	compartment:compartment,
      			  	//snap_date:x._id.snap_date,
      			  	trx_date:x._id.snap_date,
      			  	dep_date:x._id.dep_date,
        				trx_month:snap_month,
        				trx_year:snap_year,
        				dep_month:dep_month,
        				dep_year:dep_year,
      			  	flown_farebasis:x.sale_pax_fb,
                capacity:x.capacity,
                capacity_1:x.capacity_1,
                inventory:{
                  booking:x.booking,
                  booking_1:x.booking_1,
                }
              }
             }
        );

   if ( num % 1000 == 0 ){
        	bulk.execute();
        	bulk = db.JUP_DB_Manual_Triggers_Module.initializeOrderedBulkOp();
        }
        num++;
      })      
bulk.execute();