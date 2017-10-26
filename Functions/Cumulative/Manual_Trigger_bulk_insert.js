// Booking update from cumulative collection
  var num = 1;
  var bulk = db.JUP_DB_Manual_Triggers_Module.initializeOrderedBulkOp();
  db.Test_Booking_Cumulatives.find().forEach(function(x){
        var book_pax = {};
  var book_ticket = {};
  book_pax['value'] = x.value.actual_pax;
  book_pax['cum_value'] = x.value.pax;
  book_pax['value_1'] = x.value.actual_pax_1;
  book_pax['cum_value_1'] = x.value.pax_1;
  book_ticket['value'] = x.value.actual_rev;
  book_ticket['cum_value'] =  x.value.rev;
  book_ticket['value_1'] = x.value.actual_rev_1;
  book_ticket['cum_value_1'] =  x.value.rev_1;
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
  
  var snap_month =Number( x._id.sale_date.substring(5,7));
  var snap_year = Number(x._id.sale_date.substring(0,4));
  var dep_month = Number(x._id.dep_date.substring(5,7));
  var dep_year = Number(x._id.dep_date.substring(0,4));

  // bulk update by matching parameter. if any match it will update that row otherwise insert newly
  bulk.find({
            'pos.City':pos.City,
            'origin.City':origin.City,
            'destination.City':destination.City,
            'compartment.compartment':compartment.compartment,
            //snap_date:x._id.snap_date,
            trx_date:x._id.sale_date,
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
                  trx_date:x._id.sale_date,
                  dep_date:x._id.dep_date,
                  trx_month:snap_month,
                  trx_year:snap_year,
                  dep_month:dep_month,
                  dep_year:dep_year,
                  book_pax:book_pax,
                  book_ticket:book_ticket
                  }
             }
        );

     if ( num % 1000 == 0 ){
          bulk.execute();
          bulk = db.JUP_DB_Manual_Triggers_Module.initializeOrderedBulkOp();
      }
      num++;
});   
bulk.execute();    

// Updation of sales cumulative  in bulk update method
var num = 1;
var bulk = db.JUP_DB_Manual_Triggers_Module.initializeOrderedBulkOp();
  db.Test_Sales_Cumulatives.find().forEach(function(x){
  var flown_pax = {};
  var flown_revenue = {};
  flown_pax['value'] = x.value.actual_pax;
  flown_pax['cum_value'] = x.value.pax;
  flown_pax['value_1'] = x.value.actual_pax_1;
  flown_pax['cum_value_1'] = x.value.pax_1;
  flown_revenue['value'] = x.value.actual_rev;
  flown_revenue['cum_value'] =  x.value.rev;
  flown_revenue['value_1'] = x.value.actual_rev_1;
  flown_revenue['cum_value_1'] =  x.value.rev_1;
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
  
  var snap_month =Number( x._id.sale_date.substring(5,7));
  var snap_year = Number(x._id.sale_date.substring(0,4));
  var dep_month = Number(x._id.dep_date.substring(5,7));
  var dep_year = Number(x._id.dep_date.substring(0,4));
  
  bulk.find({
        'pos.City':pos.City,
    'origin.City':origin.City,
    'destination.City':destination.City,
    'compartment.compartment':compartment.compartment,
    //snap_date:x._id.snap_date,
    trx_date:x._id.sale_date,
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
                    trx_date:x._id.sale_date,
                    dep_date:x._id.dep_date,
                    trx_month:snap_month,
                    trx_year:snap_year,
                    dep_month:dep_month,
                    dep_year:dep_year,
                    sale_pax:flown_pax,
                    sale_revenue:flown_revenue
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


// Updation of flown cumulative  in bulk update method
var num = 1;
var bulk = db.JUP_DB_Manual_Triggers_Module.initializeOrderedBulkOp();
  db.Test_Sales_Flown_Cumulatives.find().forEach(function(x){
  var flown_pax = {};
  var flown_revenue = {};
  flown_pax['value'] = x.value.actual_pax;
  flown_pax['cum_value'] = x.value.pax;
  flown_pax['value_1'] = x.value.actual_pax_1;
  flown_pax['cum_value_1'] = x.value.pax_1;
  flown_revenue['value'] = x.value.actual_rev;
  flown_revenue['cum_value'] =  x.value.rev;
  flown_revenue['value_1'] = x.value.actual_rev_1;
  flown_revenue['cum_value_1'] =  x.value.rev_1;
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
  
  var snap_month =Number( x._id.sale_date.substring(5,7));
  var snap_year = Number(x._id.sale_date.substring(0,4));
  var dep_month = Number(x._id.dep_date.substring(5,7));
  var dep_year = Number(x._id.dep_date.substring(0,4));
  
  bulk.find({
        'pos.City':pos.City,
  	'origin.City':origin.City,
  	'destination.City':destination.City,
  	'compartment.compartment':compartment.compartment,
  	//snap_date:x._id.snap_date,
  	trx_date:x._id.sale_date,
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
                    trx_date:x._id.sale_date,
                    dep_date:x._id.dep_date,
                    trx_month:snap_month,
                    trx_year:snap_year,
                    dep_month:dep_month,
                    dep_year:dep_year,
                    flown_pax:flown_pax,
                    flown_revenue:flown_revenue
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


// Market 
var num = 1;
var bulk = db.JUP_DB_Manual_Triggers_Module.initializeOrderedBulkOp();
  db.Test_Market_Cumulatives.find().forEach(function(x){
  var MS_pax = {};
  MS_pax['value'] = x.value.actual_pax;
  MS_pax['cum_value'] = x.value.pax;
  MS_pax['market_size'] = x.value.market_size;
  MS_pax['cum_marketsize'] =  x.value.cum_value_marketsize;
   MS_pax['value_1'] = x.value.actual_pax_1;
  MS_pax['cum_value_1'] = x.value.pax_1;
  MS_pax['market_size_1'] = x.value.market_size_1;
  MS_pax['cum_marketsize_1'] =  x.value.cum_value_marketsize_1;
  var snap_month =Number( x._id.snap_date.substring(5,7));
  var snap_year = Number(x._id.snap_date.substring(0,4));
  var dep_month = Number(x._id.month);
  var dep_year = Number(x._id.year);
  var od = x._id.origin+""+x._id.destination;
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



     bulk.find({
                      'pos.City':pos.City,
                      'origin.City':origin.City,
                      'destination.City':destination.City,
                      'compartment.compartment':compartment.compartment,
                      trx_month:snap_month,
                      trx_year:snap_year,
                      dep_month:dep_month,
                      dep_year:dep_year
        }).upsert().update(
            {
              $set:{
                      od:od,
                      pos:pos,
                      origin:origin,
                      destination:destination,
                      compartment:compartment,
                      //trx_date:x._id.snap_date,
                      //trx_date:x._id.sale_date,
                      //dep_date:x._id.dep_date,
                      trx_month:snap_month,
                      trx_year:snap_year,
                      dep_month:dep_month,
                      dep_year:dep_year,
                      MS_pax:MS_pax
              }
             }
        );

     if ( num % 1000 == 0 ){
          bulk.execute();
          bulk = db.JUP_DB_Manual_Triggers_Module.initializeOrderedBulkOp();
        }

        num++;
      });

    bulk.execute(); 



// Forecast  
var num = 1;
var bulk = db.JUP_DB_Manual_Triggers_Module.initializeOrderedBulkOp();
  db.JUP_DB_Forecast_OD.find().forEach(function(x){
  var forecast = {};
  forecast['pax'] = x.pax;
  forecast['avgFare'] = x.avgFare;
  forecast['revenue'] = x.revenue;
  forecast['pax_1'] = x.pax_1;
  forecast['avgFare_1'] = x.avgFare_1;
  forecast['revenue_1'] = x.revenue_1;
  var snap_month =Number( x.snap_date.substring(5,7));
  var snap_year = Number(x.snap_date.substring(0,4));
  var dep_month = Number(x.Month);
  var dep_year = Number(x.Year);
  var pos ={};
  var od = x.origin+""+ x.destination;
  pos['Network'] = 'Network';
  pos['Cluster'] = x.Cluster;
  pos['Region'] = x.region;
  pos['Country'] = x.country;
  pos['City'] = x.pos;
  var origin = {};
  origin['Network'] = x.Network_Origin;
  origin['Cluster'] = x.Cluster_Origin;
  origin['Region'] = x.Region_Origin;
  origin['Country'] = x.Country_Origin;
  origin['City'] = x.origin;
  var destination = {};
  destination['Network'] = x.Network_Dest;
  destination['Cluster'] = x.Cluster_Dest;
  destination['Region'] = x.Region_Dest;
  destination['Country'] = x.Country_Dest;
  destination['City'] = x.destination;
  var compartment = {};
  compartment['compartment'] = x.compartment ;
  compartment ['all'] = 'all';

   bulk.find({
                  'pos.City':pos.City,
                  'origin.City':origin.City,
                  'destination.City':destination.City,
                  'compartment.compartment':compartment.compartment,
                  //snap_date:x.snap_date,
                  //trx_date:x.sale_date,
                  //dep_date:x.dep_date,
                  trx_month:snap_month,
                  trx_year:snap_year,
                  dep_month:dep_month,
                  dep_year:dep_year,
    }).upsert().update(
            {
              $set:{
                  od:od,
                  pos:pos,
                  origin:origin,
                  destination:destination,
                  compartment:compartment,
                  //trx_date:x.snap_date,
                  //trx_date:x.sale_date,
                  //dep_date:x.dep_date,
                  trx_month:snap_month,
                  trx_year:snap_year,
                  dep_month:dep_month,
                  dep_year:dep_year,
                  forecast:forecast
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



// Target
var num = 1;
var bulk = db.JUP_DB_Manual_Triggers_Module.initializeOrderedBulkOp();
db.JUP_DB_Target_OD.find().forEach(function(x){
  var target = {};
  target['pax'] = x.pax;
  target['avgFare'] = x.average_fare;
  target['revenue'] = x.revenue;
  target['pax_1'] = x.pax_1;
  target['avgFare_1'] = x.average_fare_1;
  target['revenue_1'] = x.revenue_1;
  var snap_month =Number( x.snap_date.substring(5,7));
  var snap_year = Number(x.snap_date.substring(0,4));
  var dep_month = Number(x.month);
  var dep_year = Number(x.year);
  var od = x.origin+""+ x.destination;
  var pos ={};
  pos['Network'] = 'Network';
  pos['Cluster'] = x.Cluster;
  pos['Region'] = x.region;
  pos['Country'] = x.country;
  pos['City'] = x.pos;
  var origin = {};
  origin['Network'] = x.Network_Origin;
  origin['Cluster'] = x.Cluster_Origin;
  origin['Region'] = x.Region_Origin;
  origin['Country'] = x.Country_Origin;
  origin['City'] = x.origin;
  var destination = {};
  destination['Network'] = x.Network_Dest;
  destination['Cluster'] = x.Cluster_Dest;
  destination['Region'] = x.Region_Dest;
  destination['Country'] = x.Country_Dest;
  destination['City'] = x.destination;
  var compartment = {};
  compartment['compartment'] = x.compartment ;
  compartment ['all'] = 'all';
  
  bulk.find({
        'pos.City':pos.City,
        'origin.City':origin.City,
        'destination.City':destination.City,
        'compartment.compartment':compartment.compartment,
        //snap_date:x.snap_date,
        //trx_date:x.sale_date,
        //dep_date:x.dep_date,
        dep_month:dep_month,
        dep_year:dep_year,

          }).upsert().update(
            {
            $set:{
              od:od,
              pos:pos,
              origin:origin,
              destination:destination,
              compartment:compartment,
              //trx_date:x.snap_date,
              //trx_date:x.sale_date,
              //dep_date:x.dep_date,
              dep_month:dep_month,
              dep_year:dep_year,
              target:target                            
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



 
// Distance
var num = 1;
var bulk = db.JUP_DB_Manual_Triggers_Module.initializeOrderedBulkOp();
db.JUP_DB_OD_Distance_Master.find().forEach(function(x){

        bulk.find({
            'od':x.od
        }).upsert().update(
        {
          $set:{
             distance:x.distance
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