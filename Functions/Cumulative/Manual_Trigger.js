// exact booking
// Booking in Market level not in Farebasis level
db.JUP_DB_Booking_DepDate.mapReduce(
function() {
	emit(
	{
    "Network_Dest" : this.Network_Dest,
    "Region_Dest" : this.Region_Dest,
    "Country_Dest" : this.Country_Dest,
    "Cluster_Dest" : this.Cluster_Dest,
    "Network_Origin" : this.Network_Origin,
    "Cluster_Origin" : this.Cluster_Origin,
    "Country_Origin" : this.Country_Origin,
    "Region_Origin" : this.Region_Origin,
    "Cluster" : this.Cluster,
    "region" : this.region,
    "country" : this.country,
    "pos" : this.pos,
    "od" : this.od,
    "origin" : this.origin,
    "destination" : this.destination,
	'compartment': this.compartment,
	'dep_date': this.dep_date,
	//'sale_date': this.book_date,
	'snap_date': this.snap_date,
},
	{'pax':this.pax,
	'pax_1':this.pax_1,
	'rev_1':this.revenue_1,
	'rev':this.revenue}
	);
}
,
function(key,values) {
	paxt = 0;
	revt = 0;
	paxt_1 = 0;
	revt_1 = 0;
	for (var i in values){
	paxt += values[i].pax;
	revt += values[i].rev;
	paxt_1 += values[i].pax_1;
	revt_1 += values[i].rev_1;
	}
	return {'pax':paxt,
	'rev':revt,
	'pax_1':paxt_1,
	'rev_1':revt_1,
	};
}
,
{
	'scope':{
		'pos':'',
		'origin':'',
		'destination':'',
		'compartment':'',
		"Network_Dest" : '',
		"Region_Dest" : '',
		"Country_Dest" : '',
		"Cluster_Dest" : '',
		"Network_Origin" : '',
		"Cluster_Origin" : '',
		"Country_Origin" : '',
		"Region_Origin" : '',
		"Cluster" : '',
		"region" : '',
		"country" : '',
		'dep_date': '',
	//	'sale_date': '',
		'snap_date':'',
		'result':{}
	}
	,
	'finalize':function(key,value) {
		if (pos != key.pos || 
			origin != key.origin || 
			destination != key.destination || 
			compartment != key.compartment 
                //||
		//	pos_ != key.pos_ || origin_ != key.origin_ || destination_ != key.destination_ || compartment_ != key.compartment_
	//		|| dep_date != key.dep_date 
	//		|| sale_date != key.sale_date
			){	
			result['pax'] = 0;
			result['rev'] = 0;
			result['pax_1'] = 0;
			result['rev_1'] = 0;
			result['actual_pax'] = 0;
			result['actual_rev'] = 0;
			result['actual_pax_1'] = 0;
			result['actual_rev_1'] = 0;
		}
			result['pax'] += value.pax;
			result['rev'] += value.rev;
			result['actual_pax'] = value.pax;
			result['actual_rev'] = value.rev;
			result['pax_1'] += value.pax_1;
			result['rev_1'] += value.rev_1;
			result['actual_pax_1'] = value.pax_1;
			result['actual_rev_1'] = value.rev_1;
			pos = key.pos;
			origin = key.origin;
			destination = key.destination;
			compartment = key.compartment;
			Network_Dest = key.Network_Dest,
			Region_Dest = key.Region_Dest,
			Country_Dest = key.Country_Dest,
			Cluster_Dest = key.Cluster_Dest,
			Network_Origin = key.Network_Origin,
			Cluster_Origin = key.Cluster_Origin,
			Country_Origin = key.Country_Origin,
			Region_Origin = key.Region_Origin,
			Cluster = key.Cluster,
			region = key.region,
			country = key.country,

			dep_date = key.dep_date;
			//sale_date = key.sale_date;
			snap_date = key.snap_date;
                      
		return result;
		}
	,
	'out':'Test_Booking_Cumulatives'
	}
)




 // Updation of Booking cumulative  
 
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
  
  var snap_month =Number( x._id.snap_date.substring(5,7));
  var snap_year = Number(x._id.snap_date.substring(0,4));
  var dep_month = Number(x._id.dep_date.substring(5,7));
  var dep_year = Number(x._id.dep_date.substring(0,4));

  // bulk update by matching parameter. if any match it will update that row otherwise insert newly
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
                  book_pax:book_pax,
                  book_revenue:book_ticket
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


// Sales
db.JUP_DB_Sales.mapReduce(
function() {
	emit(
	{
    "Network_Dest" : this.Network_Dest,
    "Region_Dest" : this.Region_Dest,
    "Country_Dest" : this.Country_Dest,
    "Cluster_Dest" : this.Cluster_Dest,
    "Network_Origin" : this.Network_Origin,
    "Cluster_Origin" : this.Cluster_Origin,
    "Country_Origin" : this.Country_Origin,
    "Region_Origin" : this.Region_Origin,
    "Cluster" : this.Cluster,
    "region" : this.region,
    "country" : this.country,
    "pos" : this.pos,
    "od" : this.od,
    "origin" : this.origin,
    "destination" : this.destination,
	'compartment': this.compartment,
	'dep_date': this.dep_date,
//	'sale_date': this.book_date,
	'snap_date': this.snap_date,
	'year':this.year, 
	'month':this.month  
},
	{'pax':this.pax,
	'pax_1':this.pax_1,
	'rev_1':this.revenue_1,
	'rev':this.revenue}
	);
}
,
function(key,values) {
	paxt = 0;
	revt = 0;
	paxt_1 = 0;
	revt_1 = 0;
	for (var i in values){
	paxt += values[i].pax;
	revt += values[i].rev;
	paxt_1 += values[i].pax_1;
	revt_1 += values[i].rev_1;
	}
	return {'pax':paxt,
	'rev':revt,
	'pax_1':paxt_1,
	'rev_1':revt_1,
	};
}
,
{
	'scope':{
		'pos':'',
		'origin':'',
		'destination':'',
		'compartment':'',
		"Network_Dest" : '',
		"Region_Dest" : '',
		"Country_Dest" : '',
		"Cluster_Dest" : '',
		"Network_Origin" : '',
		"Cluster_Origin" : '',
		"Country_Origin" : '',
		"Region_Origin" : '',
		"Cluster" : '',
		"region" : '',
		"country" : '',
		'dep_date': '',
//		'sale_date': '',
		'snap_date':'',
		'month':'',
		'year':'',
		'result':{}
	}
	,
	'finalize':function(key,value) {
		if (pos != key.pos || 
			origin != key.origin || 
			destination != key.destination || 
			compartment != key.compartment 
                //||
		//	pos_ != key.pos_ || origin_ != key.origin_ || destination_ != key.destination_ || compartment_ != key.compartment_
	//		|| dep_date != key.dep_date 
	//		|| sale_date != key.sale_date
			){	
			result['pax'] = 0;
			result['rev'] = 0;
			result['pax_1'] = 0;
			result['rev_1'] = 0;
			result['actual_pax'] = 0;
			result['actual_rev'] = 0;
			result['actual_pax_1'] = 0;
			result['actual_rev_1'] = 0;
		}
			result['pax'] += value.pax;
			result['rev'] += value.rev;
			result['actual_pax'] = value.pax;
			result['actual_rev'] = value.rev;
			result['pax_1'] += value.pax_1;
			result['rev_1'] += value.rev_1;
			result['actual_pax_1'] = value.pax_1;
			result['actual_rev_1'] = value.rev_1;
			pos = key.pos;
			origin = key.origin;
			destination = key.destination;
			compartment = key.compartment;
			Network_Dest = key.Network_Dest,
			Region_Dest = key.Region_Dest,
			Country_Dest = key.Country_Dest,
			Cluster_Dest = key.Cluster_Dest,
			Network_Origin = key.Network_Origin,
			Cluster_Origin = key.Cluster_Origin,
			Country_Origin = key.Country_Origin,
			Region_Origin = key.Region_Origin,
			Cluster = key.Cluster,
			region = key.region,
			country = key.country,

			dep_date = key.dep_date;
//			sale_date = key.sale_date;
			snap_date = key.snap_date;
            month = key.month;
            year = key.year;
		return result;
		}
	,
	'out':'Test_Sales_Cumulatives'
	}
)

       
 // Updation of sales cumulative  
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





// Flown
db.JUP_DB_Sales_Flown.mapReduce(
function() {
	emit(
	{
    "Network_Dest" : this.Network_Dest,
    "Region_Dest" : this.Region_Dest,
    "Country_Dest" : this.Country_Dest,
    "Cluster_Dest" : this.Cluster_Dest,
    "Network_Origin" : this.Network_Origin,
    "Cluster_Origin" : this.Cluster_Origin,
    "Country_Origin" : this.Country_Origin,
    "Region_Origin" : this.Region_Origin,
    "Cluster" : this.Cluster,
    "region" : this.region,
    "country" : this.country,
    "pos" : this.pos,
    "od" : this.od,
    "origin" : this.origin,
    "destination" : this.destination,
	'compartment': this.compartment,
	'dep_date': this.dep_date,
//	'sale_date': this.book_date,
	'snap_date': this.snap_date,
         'year':this.year, 
          'month':this.month  
},
	{'pax':this.pax,
	'pax_1':this.pax_1,
	'rev_1':this.revenue_1,
	'rev':this.revenue}
	);
}
,
function(key,values) {
	paxt = 0;
	revt = 0;
	paxt_1 = 0;
	revt_1 = 0;
	for (var i in values){
	paxt += values[i].pax;
	revt += values[i].rev;
	paxt_1 += values[i].pax_1;
	revt_1 += values[i].rev_1;
	}
	return {'pax':paxt,
	'rev':revt,
	'pax_1':paxt_1,
	'rev_1':revt_1,
	};
}
,
{
	'scope':{
		'pos':'',
		'origin':'',
		'destination':'',
		'compartment':'',
		"Network_Dest" : '',
		"Region_Dest" : '',
		"Country_Dest" : '',
		"Cluster_Dest" : '',
		"Network_Origin" : '',
		"Cluster_Origin" : '',
		"Country_Origin" : '',
		"Region_Origin" : '',
		"Cluster" : '',
		"region" : '',
		"country" : '',
		'dep_date': '',
//		'sale_date': '',
		'snap_date':'',
		'month':'',
		'year':'',
		'result':{}
	}
	,
	'finalize':function(key,value) {
		if (pos != key.pos || 
			origin != key.origin || 
			destination != key.destination || 
			compartment != key.compartment 
                //||
		//	pos_ != key.pos_ || origin_ != key.origin_ || destination_ != key.destination_ || compartment_ != key.compartment_
	//		|| dep_date != key.dep_date 
	//		|| sale_date != key.sale_date
			){	
			result['pax'] = 0;
			result['rev'] = 0;
			result['pax_1'] = 0;
			result['rev_1'] = 0;
			result['actual_pax'] = 0;
			result['actual_rev'] = 0;
			result['actual_pax_1'] = 0;
			result['actual_rev_1'] = 0;
		}
			result['pax'] += value.pax;
			result['rev'] += value.rev;
			result['actual_pax'] = value.pax;
			result['actual_rev'] = value.rev;
			result['pax_1'] += value.pax_1;
			result['rev_1'] += value.rev_1;
			result['actual_pax_1'] = value.pax_1;
			result['actual_rev_1'] = value.rev_1;
			pos = key.pos;
			origin = key.origin;
			destination = key.destination;
			compartment = key.compartment;
			Network_Dest = key.Network_Dest,
			Region_Dest = key.Region_Dest,
			Country_Dest = key.Country_Dest,
			Cluster_Dest = key.Cluster_Dest,
			Network_Origin = key.Network_Origin,
			Cluster_Origin = key.Cluster_Origin,
			Country_Origin = key.Country_Origin,
			Region_Origin = key.Region_Origin,
			Cluster = key.Cluster,
			region = key.region,
			country = key.country,

			dep_date = key.dep_date;
//			sale_date = key.sale_date;
			snap_date = key.snap_date;
                        month = key.month;
                        year = key.year;
		return result;
		}
	,
	'out':'Test_Sales_Flown_Cumulatives'
	}
)
       
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
db.JUP_DB_Market_Share.mapReduce(
function() {
	emit(
	{
	"Network_Dest" : this.Network_Dest,
    "Region_Dest" : this.Region_Dest,
    "Country_Dest" : this.Country_Dest,
    "Cluster_Dest" : this.Cluster_Dest,
    "Network_Origin" : this.Network_Origin,
    "Cluster_Origin" : this.Cluster_Origin,
    "Country_Origin" : this.Country_Origin,
    "Region_Origin" : this.Region_Origin,
    "Cluster" : this.Cluster,
    "region" : this.region,
    "country" : this.country,
    "pos" : this.pos,
    "od" : this.od,
    "origin" : this.origin,
    "destination" : this.destination,
	'compartment': this.compartment,
	'year': this.year,
	'month': this.month,
	'snap_date':this.snap_date
},
	{'pax':this.pax,
	'rev':this.revenue,
	'pax_1':this.pax_1,
	'rev_1':this.revenue_1,
	'market_size':this.market_size,
	'market_size_1':this.market_size_1
	}
	);
}
,
function(key,values) {
	paxt = 0;
	revt = 0;
	paxt_1 = 0;
	revt_1 = 0;
	market_sizet = 0;
	market_sizet_1 = 0;
	for (var i in values){
	paxt += values[i].pax;
	revt += values[i].rev;
	paxt_1 += values[i].pax_1;
	revt_1 += values[i].rev_1;
	market_sizet += values[i].market_size;
	market_sizet_1 += values[i].market_size_1;
	}
	return {
	'pax':paxt,
	'rev':revt,
	'market_size':market_sizet,
	'pax_1':paxt_1,
	'rev_1':revt_1,
	'market_size_1':market_sizet_1
	};
}
,
{
	'scope':{
		'pos':'',
		'od':'',
		'origin':'',
		'destination':'',
		'compartment':'',
        "Network_Dest" : '',
        "Region_Dest" : '',
        "Country_Dest" : '',
        "Cluster_Dest" : '',
        "Network_Origin" : '',
        "Cluster_Origin" : '',
        "Country_Origin" : '',
        "Region_Origin" : '',
        "Cluster" : '',
        "region" : '',
        "country" : '',
		'compartment':'',
		'year': '',
		'month': '',
		'snap_date':'',
		'result':{}
	}
	,
	'finalize':function(key,value) {
		if (pos != key.pos || 
			od != key.od || 
			compartment != key.compartment
        //        ||
	//		pos_ != key.pos_ || origin_ != key.origin_ || destination_ != key.destination_ || compartment_ != key.compartment_
	//		|| dep_date != key.dep_date 
	//		|| sale_date != key.sale_date
			){	
			result['pax'] = 0;
			result['cum_value_marketsize'] = 0;
			result['market_size'] = 0;
			result['actual_pax'] = 0;
			result['pax_1'] = 0;
			result['cum_value_marketsize_1'] = 0;
			result['market_size_1'] = 0;
			result['actual_pax_1'] = 0;
			// result['ticket'] = 0;
		}
			result['pax'] += value.pax;
			result['cum_value_marketsize'] += value.market_size;
			result['market_size'] = value.market_size;
			result['actual_pax'] = value.pax;
			result['pax_1'] += value.pax_1;
			result['cum_value_marketsize_1'] += value.market_size_1;
			result['market_size_1'] = value.market_size_1;
			result['actual_pax_1'] = value.pax_1;
			// result['ticket'] += value.ticketed;
						pos = key.pos;
			origin = key.origin;
			destination = key.destination;
			compartment = key.compartment;
			Network_Dest = key.Network_Dest,
			Region_Dest = key.Region_Dest,
			Country_Dest = key.Country_Dest,
			Cluster_Dest = key.Cluster_Dest,
			Network_Origin = key.Network_Origin,
			Cluster_Origin = key.Cluster_Origin,
			Country_Origin = key.Country_Origin,
			Region_Origin = key.Region_Origin,
			Cluster = key.Cluster,
			region = key.region,
			country = key.country,
			year = key.year;
			month = key.month;
			snap_date = key.snap_date;
		return result;
		}
	,
                'query':{MarketingCarrier1:"FZ"},       
		 'out':'Test_Market_Cumulatives'
	}
)

        
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
                  //trx_month:snap_month,
                  //trx_year:snap_year,
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
                  // trx_month:snap_month,
                  // trx_year:snap_year,
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
	
// Capacity which is older version so use the latest one which is in below aggregated one 

  db.JUP_DB_Host_OD_Capacity.find().forEach(function(x){
  var capacity_od = {};
  capacity_od['value'] = x.j_cap;
  
  
  var snap_month =Number( x.last_update_date.substring(5,7));
  var snap_year = Number(x.last_update_date.substring(0,4));
  var dep_month = Number(x.month);
  var dep_year = Number(x.year);
  var od = x.origin+""+ x.destination;
  var dep_date = x.dep_date;
  var origin = {};
  origin['Network'] = "Network";
  origin['Cluster'] = x.origin_cluster;
  origin['Region'] = x.origin_region;
  origin['Country'] = x.origin_country;
  origin['City'] = x.origin;
  var destination = {};
  destination['Network'] = "Network";
  destination['Cluster'] = x.destination_cluster;
  destination['Region'] = x.destination_region;
  destination['Country'] = x.destination_country;
  destination['City'] = x.destination;
  var compartment = {};
 // We have to add compartment manually because THis capacity collection has all the compartment's within one row instead of other collection
  compartment['compartment'] = 'J' ;
  compartment ['all'] = 'all';
 
  // For J compartment 
  db.JUP_DB_Manual_Triggers_Module.update({
  	'origin.City':origin.City,
  	'destination.City':destination.City,
  	'compartment.compartment':compartment.compartment,
  	dep_date:x.dep_date,
	dep_month:dep_month,
	dep_year:dep_year,
},{
  $set:{
	od:od,
  	origin:origin,
  	destination:destination,
  	compartment:compartment,
    dep_date:x.dep_date,
	dep_month:dep_month,
	dep_year:dep_year,
  	capacity_od:capacity_od
  }
},{ upsert: true, multi: true });


// For Y compartment
capacity_od['value'] = x.j_cap;
compartment['compartment'] = 'J' ;
db.JUP_DB_Manual_Triggers_Module.update({
  	'origin.City':origin.City,
  	'destination.City':destination.City,
  	'compartment.compartment':compartment.compartment,
  	dep_date:x.dep_date,
	dep_month:dep_month,
	dep_year:dep_year,
},{
  $set:{
	od:od,
  	origin:origin,
  	destination:destination,
  	compartment:compartment,
    dep_date:x.dep_date,
	dep_month:dep_month,
	dep_year:dep_year,
  	capacity_od:capacity_od
  }
},{ upsert: true, multi: true })
})     



// Capacity latest we can update by using aggregation concept in manual trigger collection so we should do lookup of
// capacity collection 

db.JUP_DB_Manual_Triggers_Module.aggregate([

    {$lookup:{
             from: 'JUP_DB_Host_OD_Capacity',
       localField: 'od',
       foreignField: 'od',
       as: 'capacityTBL'
    }},
    {$unwind:{path: "$capacityTBL", preserveNullAndEmptyArrays: true }},
    {$group:{
		_id:{
			pos:'$pos',
            origin:"$origin",
			destination:'$destination',
            od:'$od',
			compartment:'$compartment',
			dep_month:'$dep_month',
			dep_date:'$dep_date',
            dep_year:'$dep_year',
			trx_month:"$trx_month",
			trx_year:'$trx_year',
			trx_date:'$trx_date',
            sale_pax:'$sale_pax',
			sale_revenue:'$sale_revenue',
			flown_pax:'$flown_pax',
			flown_revenue:'$flown_revenue',
			flown_farebasis:'$flown_farebasis',
			sale_farebasis:'$sale_farebasis',
			book_pax:'$book_pax',
			book_ticket:'$book_ticket',
			target:'$target',
			MS_pax:'$MS_pax',
			forecast:'$forecast',
			distance:'$distance'
			},

			od_capacity:{
						$sum:{
							$cond: [ {$eq: [ '$capacityTBL.dep_date', '$dep_date' ]},
		 							 
		 							 {
			 							 	$cond:[
					 							   {
					 							   	$eq: [ 
					 							   	'$compartment.compartment', 'J' 
					 							   	]
					 							   },
					 							   '$capacityTBL.j_cap',
					 							   '$capacityTBL.y_cap'
			 								]
			 							},
			 						
			 							0 
                                	 ] 
    	            	 }}
	 }
	 },
    
    {$project:{
      		_id:0,
			pos:'$_id.pos',
            origin:"$_id.origin",
			destination:'$_id.destination',
            od:'$_id.od',
			compartment:'$_id.compartment',
			dep_month:'$_id.dep_month',
			dep_date:'$_id.dep_date',
            dep_year:'$_id.dep_year',
			trx_month:"$_id.trx_month",
			trx_year:'$_id.trx_year',
			trx_date:'$_id.trx_date',
            sale_pax:'$_id.sale_pax',
			sale_revenue:'$_id.sale_revenue',
			flown_pax:'$_id.flown_pax',
			flown_revenue:'$_id.flown_revenue',
			flown_farebasis:'$_id.flown_farebasis',
			sale_farebasis:'$_id.sale_farebasis',
			book_pax:'$_id.book_pax',
			book_ticket:'$_id.book_ticket',
			target:'$_id.target',
			MS_pax:'$_id.MS_pax',
			forecast:'$_id.forecast',
			distance:'$_id.distance',
		    od_capacity:{
		    	value:"$od_capacity"
		    }
    }},
    {
    	$out:"JUP_DB_Manual_Triggers_Module"
    }
], 
{
  allowDiskUse:true,
  cursor:{}
}
)