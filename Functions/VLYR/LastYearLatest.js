
// Booking
var od_array =  ["TBSDXB","DXBKWI","DXBEBL","DXBDOH","DXBAMM","DOHDXB","BOMVIE","DXBBAH","BKKIST","DOHBKK","KBLDXB","ISTBKK",
"DACDOH","DELPRG","DOHDAC","DXBKBL","DXBBGW","AMMDXB"];
var _odObj = {};
_odObj['od']={'$in':od_array } ;
var data = db.getCollection('JUP_DB_Booking_DepDate').find(_odObj).limit(130000)
data.forEach(function(x){
    // for Pax_1
    x.pax_1 = Math.ceil(.95 * Number(x.pax));
    x.ticket_1 = Math.ceil(.95 * Number(x.ticket));
    db.JUP_DB_Booking_DepDate.update({
        _id:x._id
        },
            x
    );
    
    x.book_date = x.book_date.replace("2017","2016");
    x.dep_date = x.dep_date.replace("2017","2016");
    x.snap_date = x.snap_date.replace("2017","2016");
    x.pax = Math.ceil(.95 * Number(x.pax));
    x.ticket = Math.ceil(.95 * Number(x.ticket));
    delete x['_id'];
    db.JUP_DB_Booking_DepDate.insert(x)
    //print(x.pax);
});

// Sales
var od_array =  ["TBSDXB","DXBKWI","DXBEBL","DXBDOH","DXBAMM","DOHDXB","BOMVIE","DXBBAH","BKKIST","DOHBKK","KBLDXB","ISTBKK",
"DACDOH","DELPRG","DOHDAC","DXBKBL","DXBBGW","AMMDXB"];
var _odObj = {};
_odObj['od']={'$in':od_array } ;
 db.getCollection('JUP_DB_Sales').find(_odObj).limit(67149).forEach(function(x){
    x.pax_1 = Math.ceil(.95 * Number(x.pax));
    x.revenue_1 = Math.ceil(.95 * Number(x.revenue));
    x.revenue_base_1 = Math.ceil(.95 * Number(x.revenue_base));
    db.JUP_DB_Sales.update({
        _id:x._id
        },
            x
    );
    x.book_date = x.book_date.replace("2017","2016");
    x.dep_date = x.dep_date.replace("2017","2016");
    x.snap_date = x.snap_date.replace("2017","2016");
    x.year = x.year-1;
    x.pax = Math.ceil(.95 * Number(x.pax));
    x.revenue = Math.ceil(.95 * Number(x.revenue));
    x.revenue_base = Math.ceil(.95 * Number(x.revenue_base));
    delete x['_id'];
    db.JUP_DB_Sales.insert(x)
    //print(x.pax);
});


// Flown 
var od_array =  ["TBSDXB","DXBKWI","DXBEBL","DXBDOH","DXBAMM","DOHDXB","BOMVIE","DXBBAH","BKKIST","DOHBKK","KBLDXB","ISTBKK",
"DACDOH","DELPRG","DOHDAC","DXBKBL","DXBBGW","AMMDXB"];
var _odObj = {};
_odObj['od']={'$in':od_array } ;
 db.getCollection('JUP_DB_Sales_Flown').find(_odObj).count().limit(75000).forEach(function(x){
    x.pax_1 = Math.ceil(.95 * Number(x.pax));
    x.revenue_1 = Math.ceil(.95 * Number(x.revenue));
    x.revenue_base_1 = Math.ceil(.95 * Number(x.revenue_base));
    db.JUP_DB_Sales_Flown.update({
        _id:x._id
        },
            x
    );
    x.book_date = x.book_date.replace("2017","2016");
    x.dep_date = x.dep_date.replace("2017","2016");
    x.snap_date = x.snap_date.replace("2017","2016");
    x.year = x.year-1;
    x.pax = Math.ceil(.95 * Number(x.pax));
    x.revenue = Math.ceil(.95 * Number(x.revenue));
    x.revenue_base = Math.ceil(.95 * Number(x.revenue_base));
    delete x['_id'];
    db.JUP_DB_Sales_Flown.insert(x)
    //print(x.pax);
});


// Market
var od_array =  ["TBSDXB","DXBKWI","DXBEBL","DXBDOH","DXBAMM","DOHDXB","BOMVIE","DXBBAH","BKKIST","DOHBKK","KBLDXB","ISTBKK",
"DACDOH","DELPRG","DOHDAC","DXBKBL","DXBBGW","AMMDXB"];
var _odObj = {};
_odObj['od']={'$in':od_array } ;
var data = db.getCollection('JUP_DB_Market_Share')
data.find(_odObj).forEach(function(x){
    x.snap_date = x.snap_date.replace("2017","2016");
    x.year = x.year-1;
    x.pax = Math.ceil(.95 * Number(x.pax));
    x.revenue = Math.ceil(.95 * Number(x.revenue));
    x.market_size = Math.ceil(.95 * Number(x.market_size));
    delete x['_id'];
    db.JUP_DB_Market_Share.insert(x)
    //print(x.pax);
});


// Forecast
var od_array =  ["TBSDXB","DXBKWI","DXBEBL","DXBDOH","DXBAMM","DOHDXB","BOMVIE","DXBBAH","BKKIST","DOHBKK","KBLDXB","ISTBKK",
"DACDOH","DELPRG","DOHDAC","DXBKBL","DXBBGW","AMMDXB"];
var _odObj = {};
_odObj['od']={'$in':od_array } ;
var data = db.getCollection('JUP_DB_Forecast_OD').find(_odObj)
data.forEach(function(x){
    x.departureMonth = x.departureMonth.replace("2017","2016");
    x.Year = x.Year.replace("2017","2016");
    x.snap_date = x.snap_date.replace("2017","2016");
    x.pax = Math.ceil(.95 * Number(x.pax));
    x.revenue = Math.ceil(.95 * Number(x.revenue));
    x.avgFare = Math.ceil(.95 * Number(x.avgFare));
    delete x['_id'];
    db.JUP_DB_Forecast_OD.insert(x)
    //print(x.pax);
});


// Target
var od_array =  ["TBSDXB","DXBKWI","DXBEBL","DXBDOH","DXBAMM","DOHDXB","BOMVIE","DXBBAH","BKKIST","DOHBKK","KBLDXB","ISTBKK",
"DACDOH","DELPRG","DOHDAC","DXBKBL","DXBBGW","AMMDXB"];
var _odObj = {};
_odObj['od']={'$in':od_array } ;
db.getCollection('JUP_DB_Target_OD').find(_odObj).forEach(function(x){
    x.year = x.year-1;
    x.snap_date = x.snap_date.replace("2017","2016");
    x.pax = Math.ceil(.95 * Number(x.pax));
    x.revenue = Math.ceil(.95 * Number(x.revenue));
    x.average_fare = Math.ceil(.95 * Number(x.average_fare));
    delete x['_id'];
    db.JUP_DB_Target_OD.insert(x)
    //print(x.pax);
});