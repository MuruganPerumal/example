
db.JUP_DB_Booking_DepDate.aggregate([
// {
	// $match:{snap_date:'2017-08-03'}
// },
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
			'snap_date': '$snap_date',
			'year':'$year', 
			'month':'$month'
		},
				
			pax:{$sum:{$cond:[{$eq:['$segment_status','CANCELED']},0,{ $cond:[
				{'$eq':['$isvoid',true]},
				{$multiply:[-1,'$pax']},
				'$pax'
			]}]}},
			
			revenue:{$sum:{ $cond:[
				{'$eq':['$isvoid',true]},
				{$multiply:[-1,'$revenue']},
				'$revenue'
			]
			}},   
			
			snap_pax:{$sum:{$cond:[{$eq:['$segment_status','CANCELED']},0,{ $cond:[
				{'$eq':['$isvoid',true]},
				0,
				'$pax'
			]}]}},
			
			snap_revenue:{$sum:{ $cond:[
				{'$eq':['$isvoid',true]},
				0,
				'$revenue'
			]
		}},   

}},
{$project:{
			book_pax:{
				value:'$pax'	,
				snap_value: '$snap_pax'
			},
			book_revenue:{
				value:'$revenue',
				snap_value: '$snap_revenue'
			}
}},
{'$out':'Test_Booking_Cumulatives'}
],{allowDiskUse:true})



// update Manual trigger table from sale farebasis collection
var num = 1;
var bulk = db.JUP_DB_Manual_Triggers_Module.initializeUnorderedBulkOp();
db.Test_Booking_Cumulatives.find().forEach(function(x){
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
  
var market_combined = x._id.pos+""+x._id.origin+""+x._id.destination+''+ x._id.compartment;
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
				market_combined:market_combined,
				//snap_date:x._id.snap_date,
				trx_date:x._id.snap_date,
				dep_date:x._id.dep_date,
				trx_month:snap_month,
				trx_year:snap_year,
				dep_month:dep_month,
				dep_year:dep_year,
				book_pax:x.book_pax,
				book_revenue:x.book_revenue, 
              }
             }
        );

 if ( num % 1000 == 0 ){
        	bulk.execute();
        	bulk = db.JUP_DB_Manual_Triggers_Module.initializeUnorderedBulkOp();
        }
        num++;

      })      
bulk.execute();


// Sales
db.JUP_DB_Sales.aggregate([
// {
	// $match:{snap_date:'2017-08-03'}
// },
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
			'snap_date': '$snap_date',
			'year':'$year', 
			'month':'$month'
		},
				
               pax:{$sum:{
				$cond:[
					{'$eq':['$isvoid',true]},
					{$multiply:[-1,'$pax']},
					'$pax'
					]
				}},
			
               revenue:
				{$sum:{
					$cond:[
						{'$eq':['$isvoid',true]},
						{$multiply:[-1,'$revenue']},
						'$revenue'
					]
				}},   
				snap_pax:{$sum:{
				$cond:[
					{'$eq':['$isvoid',true]},
					0,
					'$pax'
					]
				}},
			
               snap_revenue:
				{$sum:{
					$cond:[
						{'$eq':['$isvoid',true]},
						0,
						'$revenue'
					]
				}},   
				
}},
{$project:{
			sale_pax:{
				value:'$pax'	,
				snap_value:'$snap_pax'	
			},
			sale_revenue:{
				value:'$revenue',
				snap_value:'$snap_revenue',
			}
}},
{'$out':'Test_Sales_Cumulatives'}
],{allowDiskUse:true})


// update Manual trigger table from sales collection
var num = 1;
var bulk = db.JUP_DB_Manual_Triggers_Module.initializeUnorderedBulkOp();
db.Test_Sales_Cumulatives.find().forEach(function(x){
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
  var market_combined = x._id.pos+""+x._id.origin+""+x._id.destination+''+ x._id.compartment;

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
				market_combined:market_combined,
				//snap_date:x._id.snap_date,
				trx_date:x._id.snap_date,
				dep_date:x._id.dep_date,
				trx_month:snap_month,
				trx_year:snap_year,
				dep_month:dep_month,
				dep_year:dep_year,
				sale_pax:x.sale_pax,
				sale_revenue:x.sale_revenue, 
              }
             }
        );

 if ( num % 1000 == 0 ){
        	bulk.execute();
        	bulk = db.JUP_DB_Manual_Triggers_Module.initializeUnorderedBulkOp();
        }
        num++;

      })      
bulk.execute();


// flown

db.JUP_DB_Sales_Flown.aggregate([
// {
	// $match:{snap_date:'2017-08-03'}
// },
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
			'snap_date': '$snap_date',
			'year':'$year', 
			'month':'$month'
		},
				
               pax:{$sum:{
				$cond:[
					{'$eq':['$isvoid',true]},
					{$multiply:[-1,'$pax']},
					'$pax'
					]
				}},
			
               revenue:
				{$sum:{
					$cond:[
						{'$eq':['$isvoid',true]},
						{$multiply:[-1,'$revenue']},
						'$revenue'
					]
				}},    
			snap_pax:{$sum:{
				$cond:[
					{'$eq':['$isvoid',true]},
					0,
					'$pax'
					]
				}},
			
		   snap_revenue:
				{$sum:{
					$cond:[
						{'$eq':['$isvoid',true]},
						0,
						'$revenue'
					]
				}},   

}},
{$project:{
			flown_pax:{
				value:'$pax'	,
				snap_value:'$snap_pax'	
			},
			flown_revenue:{
				value:'$revenue',
				snap_value:'$snap_revenue',
			}
}},
{'$out':'Test_Sales_Flown_Cumulatives'}
],{allowDiskUse:true})


// update Manual trigger table from sales collection
var num = 1;
var bulk = db.JUP_DB_Manual_Triggers_Module.initializeUnorderedBulkOp();
db.Test_Sales_Flown_Cumulatives.find().forEach(function(x){
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
  var market_combined = x._id.pos+""+x._id.origin+""+x._id.destination+''+ x._id.compartment;

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
				market_combined:market_combined,
				//snap_date:x._id.snap_date,
				trx_date:x._id.snap_date,
				dep_date:x._id.dep_date,
				trx_month:snap_month,
				trx_year:snap_year,
				dep_month:dep_month,
				dep_year:dep_year,
				flown_pax:x.flown_pax,
				flown_revenue:x.flown_revenue, 
              }
             }
        );

 if ( num % 1000 == 0 ){
        	bulk.execute();
        	bulk = db.JUP_DB_Manual_Triggers_Module.initializeUnorderedBulkOp();
        }
        num++;

      })      
bulk.execute();
