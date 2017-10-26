
// Booking
var num = 1;
var bulk = db.JUP_DB_Booking_DepDate.initializeOrderedBulkOp();
//var bulk1 = db.JUP_DB_Booking_DepDate.initializeOrderedBulkOp();
var data = db.getCollection('JUP_DB_Booking_DepDate').find().limit(1420095)
data.forEach(function(x){
    // for Pax_1
    // x.pax_1 = Math.ceil(.95 * Number(x.pax));
    // x.ticket_1 = Math.ceil(.95 * Number(x.ticket));
    
    // Take minimum and maximum of 70% pax for random
    var max_num=x.pax+x.pax*0.70;
    var min_num=x.pax-x.pax*0.70;
    var max_num_rev=x.ticket+x.ticket*0.70;
    var min_num_rev=x.ticket-x.ticket*0.70;

    x.pax_1=Math.floor(Math.random()*(max_num-min_num+1)+min_num);
    x.ticket_1=Math.floor(Math.random()*(max_num_rev-min_num_rev+1)+min_num_rev);
    
    // Update pax_1 as well as ticket_1 also
    bulk.find({
        _id:x._id
      
      }).upsert().update(
            {
              $set:x
             }
        );
     
    

    //print(x.pax);
    if ( num % 1000 == 0 ){
            bulk.execute();
            bulk = db.JUP_DB_Booking_DepDate.initializeOrderedBulkOp();
    }
    num++;
});
bulk.execute();




// Sales
var num = 1;
var bulk = db.JUP_DB_Sales.initializeOrderedBulkOp();
//var bulk1 = db.JUP_DB_Sales.initializeOrderedBulkOp();
 db.getCollection('JUP_DB_Sales').find().limit(662614).forEach(function(x){
    // x.pax_1 = Math.ceil(.95 * Number(x.pax));
    // x.revenue_1 = Math.ceil(.95 * Number(x.revenue));
    // x.revenue_base_1 = Math.ceil(.95 * Number(x.revenue_base));

    var max_num=x.pax+x.pax*0.70;
    var min_num=x.pax-x.pax*0.70;
    var max_num_rev=x.revenue+x.revenue*0.70;
    var min_num_rev=x.revenue-x.revenue*0.70;
    var max_num_rev_base=x.revenue_base+x.revenue_base*0.70;
    var min_num_rev_base=x.revenue_base-x.revenue_base*0.70;
    x.pax_1=Math.floor(Math.random()*(max_num-min_num+1)+min_num);
    x.revenue_1=Math.floor(Math.random()*(max_num_rev-min_num_rev+1)+min_num_rev);
    x.revenue_base_1=Math.floor(Math.random()*(max_num_rev-min_num_rev+1)+min_num_rev);
    // Update pax_1 as well as ticket_1 also
    bulk.find({
        _id:x._id
      
      }).upsert().update(
            {
              $set:x
             }
        );
    
    
    //print(x.pax);
    if ( num % 1000 == 0 ){
            bulk.execute();
            bulk = db.JUP_DB_Sales.initializeOrderedBulkOp();
      
    }
    num++;
    //print(x.pax);
});
bulk.execute();



// Flown 

var num = 1;
var bulk = db.JUP_DB_Sales_Flown.initializeOrderedBulkOp();
 db.getCollection('JUP_DB_Sales_Flown').find().limit(655535).forEach(function(x){
    // x.pax_1 = Math.ceil(.95 * Number(x.pax));
    // x.revenue_1 = Math.ceil(.95 * Number(x.revenue));
    // x.revenue_base_1 = Math.ceil(.95 * Number(x.revenue_base));

    var max_num=x.pax+x.pax*0.70;
    var min_num=x.pax-x.pax*0.70;
    var max_num_rev=x.revenue+x.revenue*0.70;
    var min_num_rev=x.revenue-x.revenue*0.70;
    var max_num_rev_base=x.revenue_base+x.revenue_base*0.70;
    var min_num_rev_base=x.revenue_base-x.revenue_base*0.70;
    x.pax_1=Math.floor(Math.random()*(max_num-min_num+1)+min_num);
    x.revenue_1=Math.floor(Math.random()*(max_num_rev-min_num_rev+1)+min_num_rev);
    x.revenue_base_1=Math.floor(Math.random()*(max_num_rev-min_num_rev+1)+min_num_rev);
    
    // Update pax_1 as well as ticket_1 also
    bulk.find({
        _id:x._id
      
      }).upsert().update(
            {
              $set:x
             }
        );
    
    // Execute 1000 document into only one db hit
    if ( num % 1000 == 0 ){

            bulk.execute();
            bulk = db.JUP_DB_Sales_Flown.initializeOrderedBulkOp();
           
    }
    num++;
    //print(x.pax);
});
bulk.execute();






// Market
var num = 1;
var bulk = db.JUP_DB_Market_Share.initializeOrderedBulkOp();
var data = db.getCollection('JUP_DB_Market_Share');
data.find().limit(1185578).forEach(function(x){
    
    x.pax_1 = Math.ceil(.95 * Number(x.pax));
    x.revenue_1 = Math.ceil(.95 * Number(x.revenue));
    x.market_size_1 = Math.ceil(.95 * Number(x.market_size));

    bulk.find({
        _id:x._id
      
      }).upsert().update(
            {
              $set:x
             }
        );
    
    // Execute 1000 document into only one db hit
    if ( num % 1000 == 0 ){

            bulk.execute();
            bulk = db.JUP_DB_Market_Share.initializeOrderedBulkOp();
    }
    num++;
    //print(x.pax);
});
bulk.execute();


// Forecast
var num = 1;
var bulk = db.JUP_DB_Forecast_OD.initializeOrderedBulkOp();
var data = db.getCollection('JUP_DB_Forecast_OD').find().limit(35968);
data.forEach(function(x){
    
    x.pax_1 = Math.ceil(.95 * Number(x.pax));
    x.revenue_1 = Math.ceil(.95 * Number(x.revenue));
    x.avgFare_1 = Math.ceil(.95 * Number(x.avgFare));

    bulk.find({
        _id:x._id
      
      }).upsert().update(
            {
              $set:x
             }
        );
    
    
    // Execute 1000 document into only one db hit
    if ( num % 1000 == 0 ){

            bulk.execute();
            bulk = db.JUP_DB_Forecast_OD.initializeOrderedBulkOp();
    
    }
    num++;
    //print(x.pax);
});
bulk.execute();


// Target
var num = 1;
var bulk = db.JUP_DB_Target_OD.initializeOrderedBulkOp();
db.getCollection('JUP_DB_Target_OD').find().limit(59575).forEach(function(x){
    
    x.pax_1 = Math.ceil(.95 * Number(x.pax));
    x.revenue_1 = Math.ceil(.95 * Number(x.revenue));
    x.average_fare_1 = Math.ceil(.95 * Number(x.average_fare));

    bulk.find({
        _id:x._id
      
      }).upsert().update(
            {
              $set:x
             }
        );

    
    // Execute 1000 document into only one db hit
    if ( num % 1000 == 0 ){

            bulk.execute();
            bulk = db.JUP_DB_Target_OD.initializeOrderedBulkOp();
    
    }
    num++;
    //print(x.pax);
});
bulk.execute();