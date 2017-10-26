//db.getCollection('manual_out').find({}).count()
// FOr updating Landing page based on user name
db.JUP_DB_User.find({'name':'Dummy'}).forEach(function(x){
    var _posList = x.list_of_pos ;
    var userName = x.name ;
    db.JUP_DB_Manual_Triggers_Module.aggregate([
    {
         $match:{
                'pos.City':{$in:_posList}
             }
    },
    {$unwind:'$dep_date'},
  
  
  // group it based on compartment wise.. It's needed for monthly data like target and farecast 
  {
         $group:{
                _id:{
                      dep_date:"$dep_date",
                      dep_month:'$dep_month',
                      dep_year:'$dep_year',
                      pos:'$pos.City',
                      pos_country:'$pos.Country',
                      od:'$od',
            compartment:'$compartment.compartment'
            },
            
                      book_pax:{$sum:"$book_pax.value"},
                      book_ticket:{$sum:"$book_ticket.value"},
                      book_pax_1:{$sum:"$book_pax.value_1"},
                      book_ticket_1:{$sum:"$book_ticket.value_1"},
                      sale_pax:{$sum:"$sale_pax.value"},
                      sale_revenue:{$sum:"$sale_revenue.value"},
                      sale_pax_1:{$sum:"$sale_pax.value_1"},
                      sale_revenue_1:{$sum:"$sale_revenue.value_1"},
                      flown_pax:{$sum:"$flown_pax.value"},
                      flown_revenue:{$sum:"$flown_revenue.value"},
                      flown_pax_1:{$sum:"$flown_pax.value_1"},
                      flown_revenue_1:{$sum:"$flown_revenue.value_1"},
                      target_pax:{$max:'$target.pax'},
                      target_revenue:{$max:'$target.revenue'},
                      market_pax:{$max:'$MS_pax.value'},
                      market_size:{$max:'$MS_pax.market_size'},
                      target_pax_1:{$max:'$target.pax_1'},
                      target_revenue_1:{$max:'$target.revenue_1'},
                      market_pax_1:{$max:'$MS_pax.value_1'},
                      market_size_1:{$max:'$MS_pax.market_size_1'},
                      distance:{$sum:'$distance'},
                      forecast_pax:{$max:'$forecast.pax'},
                      forecast_avgFare:{$max:'$forecast.avgFare'},
                      forecast_revenue:{$max:'$forecast.revenue'},
             }   
    },

    {
        $project:{
            dep_month:'$_id.dep_month',
            dep_date:'$_id.dep_date',
            dep_year:'$_id.dep_year',
            pos:'$_id.pos',
            pos_country:"$_id.pos_country",
            od:'$_id.od',
            book_pax:'$book_pax',
            book_ticket:'$book_ticket',
            book_pax_1:'$book_pax_1',
            book_ticket_1:'$book_ticket_1',
            sale_pax:'$sale_pax',
            sale_revenue:'$sale_revenue',
            sale_pax_1:'$sale_pax_1',
            sale_revenue_1:'$sale_revenue_1',
            flown_pax:'$flown_pax',
            flown_revenue:'$flown_revenue',
            flown_pax_1:'$flown_pax_1',
            flown_revenue_1:'$flown_revenue_1',
            target_pax:'$target_pax',
            market_pax:'$market_pax',
            market_size:'$market_size',
            target_pax_1:'$target_pax_1',
            target_revenue_1:'$target_revenue_1',
            market_pax_1:'$market_pax_1',
            market_size_1:'$market_size_1',
            forecast_pax:'$forecast_pax',
            forecast_avgFare:'$forecast_avgFare',
            forecast_revenue:'$forecast_revenue',
            target_revenue:'$target_revenue',
            distance:'$distance',
            }
    },
  
  
  
    {
         $group:{
                _id:{
                      dep_date:"$dep_date",
                      dep_month:'$dep_month',
                      dep_year:'$dep_year',
                      pos:'$pos.City',
                      pos_country:'$pos_country',
                      od:'$od'
            },
                      book_pax:{$sum:"$book_pax"},
                      book_ticket:{$sum:"$book_ticket"},
                      book_pax_1:{$sum:"$book_pax_1"},
                      book_ticket_1:{$sum:"$book_ticket_1"},
                      sale_pax:{$sum:"$sale_pax"},
                      sale_revenue:{$sum:"$sale_revenue"},
                      sale_pax_1:{$sum:"$sale_pax_1"},
                      sale_revenue_1:{$sum:"$sale_revenue_1"},
                      flown_pax:{$sum:"$flown_pax"},
                      flown_revenue:{$sum:"$flown_revenue"},
                      flown_pax_1:{$sum:"$flown_pax_1"},
                      flown_revenue_1:{$sum:"$flown_revenue_1"},
                      target_pax:{$sum:'$target_pax'},
                      target_revenue:{$sum:'$target_revenue'},
                      market_pax:{$sum:'$market_pax'},
                      market_size:{$sum:'$market_size'},
                      target_pax_1:{$sum:'$target_pax_1'},
                      target_revenue_1:{$sum:'$target_revenue_1'},
                      market_pax_1:{$sum:'$market_pax_1'},
                      market_size_1:{$sum:'$market_size_1'},
                      distance:{$sum:'$distance'},
                      forecast_pax:{$sum:'$forecast_pax'},
                      forecast_avgFare:{$sum:'$forecast_avgFare'},
                      forecast_revenue:{$sum:'$forecast_revenue'},
             }   
    },

    {
        $project:{
            dep_month:'$_id.dep_month',
            dep_date:'$_id.dep_date',
            dep_year:'$_id.dep_year',
            pos:'$_id.pos',
            pos_country:"$_id.pos_country",
            od:'$_id.od',
            book_pax:'$book_pax',
            book_ticket:'$book_ticket',
            book_pax_1:'$book_pax_1',
            book_ticket_1:'$book_ticket_1',
            sale_pax:'$sale_pax',
            sale_revenue:'$sale_revenue',
            sale_pax_1:'$sale_pax_1',
            sale_revenue_1:'$sale_revenue_1',
            flown_pax:'$flown_pax',
            flown_revenue:'$flown_revenue',
            flown_pax_1:'$flown_pax_1',
            flown_revenue_1:'$flown_revenue_1',
            target_pax:'$target_pax',
            market_pax:'$market_pax',
            market_size:'$market_size',
            target_pax_1:'$target_pax_1',
            target_revenue_1:'$target_revenue_1',
            market_pax_1:'$market_pax_1',
            market_size_1:'$market_size_1',
            forecast_pax:'$forecast_pax',
            forecast_avgFare:'$forecast_avgFare',
            forecast_revenue:'$forecast_revenue',
            target_avgFare_1: {
                 $cond: [ { $ne: [ "$target_pax_1", 0 ] }, {$divide:['$target_revenue_1','$target_pax_1']}, 0 ]
               },
            target_avgFare: {
                 $cond: [ { $ne: [ "$target_pax", 0 ] }, {$divide:['$target_revenue','$target_pax']}, 0 ]
               },
            target_revenue:'$target_revenue',
            distance:'$distance',
            }
    },
  
    {$lookup:{
             from: 'JUP_DB_Host_OD_Capacity',
       localField: 'od',
       foreignField: 'od',
       as: 'capacityTBL'
    }},   
  {$unwind:{path: "$capacityTBL", preserveNullAndEmptyArrays: true }},
  
  


   {$group:{
    _id:{  dep_month:'$dep_month',
            dep_date:'$dep_date',
            dep_year:'$dep_year',
            pos:'$pos',
            pos_country:"$pos_country",
            od:'$od',
            book_pax:'$book_pax',
            book_ticket:'$book_ticket',
            book_pax_1:'$book_pax_1',
            book_ticket_1:'$book_ticket_1',
            sale_pax:'$sale_pax',
            sale_revenue:'$sale_revenue',
            sale_pax_1:'$sale_pax_1',
            sale_revenue_1:'$sale_revenue_1',
            flown_pax:'$flown_pax',
            flown_revenue:'$flown_revenue',
            flown_pax_1:'$flown_pax_1',
            flown_revenue_1:'$flown_revenue_1',
            target_pax:'$target_pax',
            target_avgFare: '$target_avgFare',
            target_revenue:'$target_revenue',
            market_pax:'$market_pax',
            market_size:'$market_size',
            distance:'$distance',
            target_pax_1:'$target_pax_1',
            target_revenue_1:'$target_revenue_1',
            target_avgFare_1:'$target_avgFare_1',
            market_pax_1:'$market_pax_1',
            market_size_1:'$market_size_1',
            forecast_pax:'$forecast_pax',
            forecast_avgFare:'$forecast_avgFare',
            forecast_revenue:'$forecast_revenue',
      },
            
      od_capacity:{$sum:{$cond: [ {$and: [ { $eq: [ '$capacityTBL.dep_date', '$dep_date' ] }, 
             { $or:[{$eq: [ '$capacityTBL.origin_country', '$pos_country' ]},
               {$eq: [ '$capacityTBL.destination_country', '$pos_country' ]}] } ] },
                                     '$capacityTBL.od_capacity',
                                     0 ] }},
       od_capacity_1:{$sum:{$cond: [ {$and: [ { $eq: [ '$capacityTBL.dep_date', '$dep_date' ] }, 
             { $or:[{$eq: [ '$capacityTBL.origin_country', '$pos_country' ]},
               {$eq: [ '$capacityTBL.destination_country', '$pos_country' ]}] } ] },
                                     '$capacityTBL.od_capacity_1',
                                     0 ] }},
   }
   },

      {
       $project:{
           dep_month:'$_id.dep_month',
           combine_LandingPage:{$concat:['$_id.pos',{$substr:['$_id.dep_month', 0, -1 ]},{$substr:['$_id.dep_year', 0, -1 ]}]},
           dep_date:'$_id.dep_date',
           dep_year:'$_id.dep_year',
           pos:'$_id.pos',
           pos_country:"$_id.pos_country",
           od:'$_id.od',
           book_pax:'$_id.book_pax',
           book_ticket:'$_id.book_ticket',
           book_pax_1:'$_id.book_pax_1',
           book_ticket_1:'$_id.book_ticket_1',
           sale_pax:'$_id.sale_pax',
           sale_revenue:'$_id.sale_revenue',
           sale_pax_1:'$_id.sale_pax_1',
           sale_revenue_1:'$_id.sale_revenue_1',
           flown_pax:'$_id.flown_pax',
           flown_revenue:'$_id.flown_revenue',
           flown_pax_1:'$_id.flown_pax_1',
           flown_revenue_1:'$_id.flown_revenue_1',
           target_pax:'$_id.target_pax',
           target_avgFare: '$_id.target_avgFare',
           target_revenue:'$_id.target_revenue',
           distance:'$_id.distance',
       market_pax:'$_id.market_pax',
      market_size:'$_id.market_size',
       od_capacity:'$od_capacity',
       od_capacity_1:'$od_capacity_1',
       target_pax_1:'$_id.target_pax_1',
            target_revenue_1:'$_id.target_revenue_1',
      target_avgFare_1:'$_id.target_avgFare_1',
      market_pax_1:'$_id.market_pax_1',
      market_size_1:'$_id.market_size_1',
      forecast_pax:'$_id.forecast_pax',
      forecast_avgFare:'$_id.forecast_avgFare',
      forecast_revenue:'$_id.forecast_revenue',
           }
   },

    {$lookup:{
             from: 'JUP_DB_OD_Master',
       localField: 'od',
       foreignField: 'OD',
       as: 'signList'
    }},   
  {$unwind:{path: "$signList", preserveNullAndEmptyArrays: true }},
   
   {$lookup:{
             from: 'JUP_DB_FMS_POS_Level',
           localField: 'pos',
           foreignField: 'pos',
           as: 'FMS_POSwise'
    }},   
    
      // put the condition for matching of month, year and comp
    {$addFields:{
        FMS_POSwise:{ $filter: {
             input: "$FMS_POSwise",
             as: "config",
             cond: { $and: [
                { $eq: [ "$$config.month", "$dep_month" ] },
        { $eq: [ "$$config.year", "$dep_year" ] },
                ] }
        }}
    }},
  
    {$unwind:{path: "$FMS_POSwise", preserveNullAndEmptyArrays: true }},
    
   
   {$group:{
     _id:{
      dep_month:'$dep_month',
      dep_year:'$dep_year',
      dep_date:'$dep_date',
     },
       book_pax:{$sum:'$book_pax'},
           book_ticket:{$sum:'$book_ticket'},
           book_pax_1:{$sum:'$book_pax_1'},
           book_ticket_1:{$sum:'$book_ticket_1'},
           sale_pax:{$sum:'$sale_pax'},
           sale_revenue:{$sum:'$sale_revenue'},
           sale_pax_1:{$sum:'$sale_pax_1'},
           sale_revenue_1:{$sum:'$sale_revenue_1'},
           flown_pax:{$sum:'$flown_pax'},
           flown_revenue:{$sum:'$flown_revenue'},
           flown_pax_1:{$sum:'$flown_pax_1'},
           flown_revenue_1:{$sum:'$flown_revenue_1'},
           target_pax:{$sum:'$target_pax'},
           target_avgFare: {$avg:'$target_avgFare'},
           target_revenue:{$sum:'$target_revenue'},
           distance:{$sum:'$distance'},
       od_capacity:{$sum:'$od_capacity'},
       od_capacity_1:{$sum:'$od_capacity_1'},
        market_pax:{$sum:'$market_pax'},
      market_size:{$sum:'$market_size'},
      target_pax_1:{$sum:'$target_pax_1'},
           target_avgFare_1: {$avg:'$target_avgFare_1'},
           target_revenue_1:{$sum:'$target_revenue_1'},
      market_pax_1:{$sum:'$market_pax_1'},
      market_size_1:{$sum:'$market_size_1'},
      forecast_pax:{$sum:'$forecast_pax'},
      forecast_avgFare:{$sum:'$forecast_avgFare'},
      forecast_revenue:{$sum:'$forecast_revenue'},
       list_of_pos:{$addToSet:'$pos'},
       signOD:{$addToSet:{$cond:[{ $eq: [ '$signList.significant_flag', 'significant' ]},'$od',null]}},
       nonSignOD:{$addToSet:{$cond:[{ $ne: [ '$signList.significant_flag', 'significant' ]},'$od',null]}},
       totalOD:{$addToSet:'$od'},
          FMS:{$sum:'$FMS_POSwise.FMS'}
    }},
   
   {$project:{
     dep_month:'$_id.dep_month',
       dep_date:'$_id.dep_date',
           dep_year:'$_id.dep_year',
           book_pax:'$book_pax',
           book_ticket:'$book_ticket',
           book_pax_1:'$book_pax_1',
           book_ticket_1:'$book_ticket_1',
           sale_pax:'$sale_pax',
           sale_revenue:'$sale_revenue',
           sale_pax_1:'$sale_pax_1',
           sale_revenue_1:'$sale_revenue_1',
           flown_pax:'$flown_pax',
           flown_revenue:'$flown_revenue',
           flown_pax_1:'$flown_pax_1',
           flown_revenue_1:'$flown_revenue_1',
           target_pax:'$target_pax',
           target_avgFare: '$target_avgFare',
           target_revenue:'$target_revenue',
       target_pax_1:'$target_pax_1',
           target_avgFare_1: '$target_avgFare_1',
           target_revenue_1:'$target_revenue_1',
           distance:'$distance',
       capacity:'$od_capacity',
       capacity_1:'$od_capacity_1',
       market_pax:'$market_pax',
      market_size:'$market_size',
      market_pax_1:'$market_pax_1',
      market_size_1:'$market_size_1',
      forecast_pax:'$forecast_pax',
      forecast_avgFare:'$forecast_avgFare',
      forecast_revenue:'$forecast_revenue',
//       list_of_pos:'$list_of_pos',

       signOD:'$signOD',
       nonSignOD:'$nonSignOD',
       totalOD:'$totalOD',
         FMS:{$divide:['$FMS',{$size:'$totalOD'}]},
  }
   },
   

   {$group:{
    _id:{  
      dep_month:'$dep_month',
      dep_year:'$dep_year',

       },
       book_pax:{$sum:'$book_pax'},
           book_ticket:{$sum:'$book_ticket'},
           book_pax_1:{$sum:'$book_pax_1'},
           book_ticket_1:{$sum:'$book_ticket_1'},
           sale_pax:{$sum:'$sale_pax'},
           sale_revenue:{$sum:'$sale_revenue'},
           sale_pax_1:{$sum:'$sale_pax_1'},
           sale_revenue_1:{$sum:'$sale_revenue_1'},
           flown_pax:{$sum:'$flown_pax'},
           flown_revenue:{$sum:'$flown_revenue'},
           flown_pax_1:{$sum:'$flown_pax_1'},
           flown_revenue_1:{$sum:'$flown_revenue_1'},
           target_pax:{$max:'$target_pax'},
           target_avgFare: {$avg:'$target_avgFare'},
           target_revenue:{$max:'$target_revenue'},
    target_pax_1:{$max:'$target_pax_1'},
           target_avgFare_1: {$avg:'$target_avgFare_1'},
           target_revenue_1:{$max:'$target_revenue_1'},
       market_pax:{$max:'$market_pax'},
      market_size:{$max:'$market_size'},
      market_pax_1:{$max:'$market_pax_1'},
      market_size_1:{$max:'$market_size_1'},
      forecast_pax:{$sum:'$forecast_pax'},
      forecast_avgFare:{$sum:'$forecast_avgFare'},
      forecast_revenue:{$sum:'$forecast_revenue'},
        distance:{$max:'$distance'},
       capacity:{$sum:'$capacity'},
       capacity_1:{$sum:'$capacity_1'},
        FMS:{$avg:'$FMS'},
       dep_date:{$push:{
         dep_date:'$dep_date',
         capacity:'$capacity',
         capacity_1:'$capacity_1',
         distance:'$distance',
         book_pax:'$book_pax',
         book_ticket:'$book_ticket',
         book_pax_1:'$book_pax_1',
         book_ticket_1:'$book_ticket_1',
         sale_pax:'$sale_pax',
         sale_revenue:'$sale_revenue',
         sale_pax_1:'$sale_pax_1',
         sale_revenue_1:'$sale_revenue_1',
         flown_pax:'$flown_pax',
         flown_revenue:'$flown_revenue',
         flown_pax_1:'$flown_pax_1',
         flown_revenue_1:'$flown_revenue_1',
         target_pax:'$target_pax',
         target_avgFare: '$target_avgFare',
         target_revenue:'$target_revenue',
         signOD:'$signOD',
        nonSignOD:'$nonSignOD',
        totalOD:'$totalOD'
       }}
   }
   },
   
   {$project:{
      dep_month:'$_id.dep_month',
      dep_year:'$_id.dep_year',
  //    list_of_pos:'$_id.list_of_pos',
      book_pax:'$book_pax',
           book_ticket:'$book_ticket',
           book_pax_1:'$book_pax_1',
           book_ticket_1:'$book_ticket_1',
           sale_pax:'$sale_pax',
           sale_revenue:'$sale_revenue',
           sale_pax_1:'$sale_pax_1',
           sale_revenue_1:'$sale_revenue_1',
           flown_pax:'$flown_pax',
           flown_revenue:'$flown_revenue',
           flown_pax_1:'$flown_pax_1',
           flown_revenue_1:'$flown_revenue_1',
           target_pax:'$target_pax',
            target_avgFare_1: {
                 $cond: [ { $ne: [ "$target_pax_1", 0 ] }, {$divide:['$target_revenue_1','$target_pax_1']}, 0 ]
               },
            target_avgFare: {
                 $cond: [ { $ne: [ "$target_pax", 0 ] }, {$divide:['$target_revenue','$target_pax']}, 0 ]
               },
           target_revenue:'$target_revenue',
            target_pax_1:'$target_pax_1',
           
           target_revenue_1:'$target_revenue_1',
       forecast_pax:'$forecast_pax',
      forecast_avgFare:'$forecast_avgFare',
      forecast_revenue:'$forecast_revenue',
           distance:'$distance',
       capacity:'$capacity',
       capacity_1:'$capacity_1',

        market:{
        pax:'$market_pax',
        market_size:'$market_size',
        pax_1:'$market_pax_1',
        market_size_1:'$market_size_1',
            FMS:'$FMS',
      },
      
       dep_date:'$dep_date'
   }},
   
   {$group:{
     _id:{
       dep_month:'$dep_month',
       dep_year:'$dep_year',
  //     list_of_pos:'$list_of_pos',
       this_month:{
         book_pax:'$book_pax',
         book_ticket:'$book_ticket',
         book_pax_1:'$book_pax_1',
         book_ticket_1:'$book_ticket_1',
         sale_pax:'$sale_pax',
         sale_revenue:'$sale_revenue',
         sale_pax_1:'$sale_pax_1',
         sale_revenue_1:'$sale_revenue_1',
         flown_pax:'$flown_pax',
         flown_revenue:'$flown_revenue',
         flown_pax_1:'$flown_pax_1',
         flown_revenue_1:'$flown_revenue_1',
         target_pax:'$target_pax',
         target_avgFare: '$target_avgFare',
         target_revenue:'$target_revenue',
         target_pax_1:'$target_pax_1',
         target_avgFare_1: '$target_avgFare_1',
         target_revenue_1:'$target_revenue_1',
         distance:'$distance',
         capacity:'$capacity',
         capacity_1:'$capacity_1',
         forecast_pax:'$forecast_pax',
        forecast_avgFare:'$forecast_avgFare',
        forecast_revenue:'$forecast_revenue',
         dep_date:'$dep_date',
         market:'$market'
     }
     },
     count:{$sum:1}
   }},
  {
        $addFields:{
                    user:userName
            }
    },
  {
        $addFields:{
                    list_of_pos:_posList
            }
    },
   {$project:{
          _id:0,
          user:'$user',
       dep_month:'$_id.dep_month',
       dep_year:'$_id.dep_year',
       list_of_pos:'$list_of_pos',
       this_month:'$_id.this_month'
   }},
   
    {$out:'JUP_DB_Landing_Page_'}
    ])
    })
    
    
    
   // db.JUP_DB_Landing_Page_.find()
   // db.JUP_DB_FMS_POS_Level.find()