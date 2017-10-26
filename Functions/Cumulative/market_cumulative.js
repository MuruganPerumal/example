var date="";
var time="";
var dates=db.JUP_DB_Market_Share.find({},{last_updated_date:1,last_updated_time:1}).sort({last_updated_date:1,last_updated_time:1}).limit(1)
dates.forEach( function(myDoc) { date= myDoc.last_updated_date ,time=myDoc.last_updated_time} );
if(date!=null && time!=null){
// Network level
var cursor_network=db.JUP_DB_Market_Share.aggregate([
		{"$match":{
			"last_updated_date" : {'$eq':date},
			"last_updated_time" : {'$eq':time},
			"pos" : {'$ne':null},
			"country" : {'$ne':null},
			"region" : {'$ne':null},
			"pos" : {'$ne':"NULL"},
			}},
			
		{"$group":{
			_id:{year:"$year",month:"$month",
			region:null,country:null,pos:null,
			compartment : "$compartment",MarketingCarrier1 : "$MarketingCarrier1"},
				 pos:{ $first: null },
				 country:{ $first: null },
				 region:{ $first: null },
				 pax:{ $sum: "$pax" },
				 pax_1:{ $sum: "$pax_1"},
				 pax_2:{ $sum: "$pax_2"},
				 revenue:{ $sum: "$revenue"},
 				 revenue_1:{ $sum: "$revenue_1"},
 				 revenue_2:{ $sum: "$revenue_2"}
			}},
			{"$project":{
				//combine_column: {$concat:["$_id.year", "$_id.month", "null", "null","null","$_id.compartment"]},
				year:"$_id.year",
				month:"$_id.month",
				region:"$region",
				country: "$country",
				pos:"$pos",
				compartment:"$_id.compartment",
				MarketingCarrier1:"$_id.MarketingCarrier1",  
				pax:"$pax",
				pax_1:"$pax_1",
				pax_2:"$pax_2",
				revenue:"$revenue",
				revenue_1:"$revenue_1",
				revenue_2:"$revenue_2"
				}}
				//,{ $out : "cumulative"}
		])

				var cumu_network=db.JUP_DB_Cumulative_Market_share.find();
		if(cursor_network!=null){
			cursor_network.forEach(function(x){
				//print(x.combine_column)
				//db.JUP_DB_Cumulative_Trx_Date.insert(x)
				var year_str=x.year+"";
				var month_var=x.month+"";
				var combine_column_ = year_str.concat(month_var, x.region, x.country,x.pos,x.compartment,x.MarketingCarrier1); 
				var combine_month_ = month_var.concat( x.region, x.country,x.pos,x.compartment); 
								//print(combine_column_)  
				cumu_network.forEach(function(y){
					if(combine_column_==y.combine_column){
						x.pax+=y.pax;
						x.pax_1+=y.pax_1;
						x.pax_2+=y.pax_2;
						x.revenue+=y.revenue;
						x.revenue_1+=y.revenue_1;
						x.revenue_2+=y.revenue_2;
					}		
				})
			
				db.getCollection('JUP_DB_Cumulative_Market_share').update({
					combine_column:combine_column_ 
					},{
   						  combine_column:combine_column_,					
						  combine_month:combine_month_,
							year:x.year,
							month:x.month,
							region:x.region,
							country: x.country,
							pos:x.pos,
							compartment:x.compartment,
							MarketingCarrier1:x.MarketingCarrier1,
							pax:x.pax,
							pax_1:x.pax_1,
							pax_2:x.pax_2,
							revenue:x.revenue,
							revenue_1:x.revenue_1,
							revenue_2:x.revenue_2,
					},{"upsert" : true});
					
			})
		}
		
		// region
		var cursor_region=db.JUP_DB_Market_Share.aggregate([
		{"$match":{
			"last_update_date" : {'$eq':date},
			"last_updated_time" : {'$eq':time},
			"pos" : {'$ne':null},
			"country" : {'$ne':null},
			"region" : {'$ne':null},
			"pos" : {'$ne':"NULL"},
			}},
			
		{"$group":{
			_id:{year:"$year",month:"$month",
			region:"$region",country:null,pos:null,
			compartment : "$compartment",MarketingCarrier1 : "$MarketingCarrier1"},
				 pos:{ $first: null },
				 country:{ $first: null },
				 //region:{ $first: null },
				 pax:{ $sum: "$pax" },
				 pax_1:{ $sum: "$pax_1"},
				 pax_2:{ $sum: "$pax_2"},
				 revenue:{ $sum: "$revenue"},
 				 revenue_1:{ $sum: "$revenue_1"},
 				 revenue_2:{ $sum: "$revenue_2"}
			}},
			{"$project":{
				//combine_column: {$concat:["$_id.year", "$_id.month", "null", "null","null","$_id.compartment"]},
				year:"$_id.year",
				month:"$_id.month",
				region:"$_id.region",
				country: "$country",
				pos:"$pos",
				compartment:"$_id.compartment",
				MarketingCarrier1:"$_id.MarketingCarrier1",  
				pax:"$pax",
				pax_1:"$pax_1",
				pax_2:"$pax_2",
				revenue:"$revenue",
				revenue_1:"$revenue_1",
				revenue_2:"$revenue_2"
				}}
				//,{ $out : "cumulative"}
		])

				var cumu_network=db.JUP_DB_Cumulative_Market_share.find();
		if(cursor_region!=null){
			cursor_region.forEach(function(x){
				//print(x.combine_column)
				//db.JUP_DB_Cumulative_Trx_Date.insert(x)
				var year_str=x.year+"";
				var month_var=x.month+"";
				var combine_column_ = year_str.concat(month_var, x.region, x.country,x.pos,x.compartment,x.MarketingCarrier1); 
				var combine_month_ = month_var.concat( x.region, x.country,x.pos,x.compartment); 
								//print(combine_column_)  
				cumu_network.forEach(function(y){
					if(combine_column_==y.combine_column){
						x.pax+=y.pax;
						x.pax_1+=y.pax_1;
						x.pax_2+=y.pax_2;
						x.revenue+=y.revenue;
						x.revenue_1+=y.revenue_1;
						x.revenue_2+=y.revenue_2;
					}		
				})
			
				db.getCollection('JUP_DB_Cumulative_Market_share').update({
					combine_column:combine_column_ 
					},{
   						  combine_column:combine_column_,					
						  combine_month:combine_month_,
							year:x.year,
							month:x.month,
							region:x.region,
							country: x.country,
							pos:x.pos,
							compartment:x.compartment,
							MarketingCarrier1:x.MarketingCarrier1,
							pax:x.pax,
							pax_1:x.pax_1,
							pax_2:x.pax_2,
							revenue:x.revenue,
							revenue_1:x.revenue_1,
							revenue_2:x.revenue_2,
					},{"upsert" : true});
					
			})
		}

		// country
		var cursor_country=db.JUP_DB_Market_Share.aggregate([
		{"$match":{
			"last_update_date" : {'$eq':date},
			"last_updated_time" : {'$eq':time},
			"pos" : {'$ne':null},
			"country" : {'$ne':null},
			"region" : {'$ne':null},
			"pos" : {'$ne':"NULL"},
			}},
			
		{"$group":{
			_id:{year:"$year",month:"$month",
			region:"$region",country:"$country",pos:null,
			compartment : "$compartment",MarketingCarrier1 : "$MarketingCarrier1"},
				 pos:{ $first: null },
				// country:{ $first: null },
				 //region:{ $first: null },
				 pax:{ $sum: "$pax" },
				 pax_1:{ $sum: "$pax_1"},
				 pax_2:{ $sum: "$pax_2"},
				 revenue:{ $sum: "$revenue"},
 				 revenue_1:{ $sum: "$revenue_1"},
 				 revenue_2:{ $sum: "$revenue_2"}
			}},
			{"$project":{
				//combine_column: {$concat:["$_id.year", "$_id.month", "null", "null","null","$_id.compartment"]},
				year:"$_id.year",
				month:"$_id.month",
				region:"$_id.region",
				country: "$_id.country",
				pos:"$pos",
				compartment:"$_id.compartment",
				MarketingCarrier1:"$_id.MarketingCarrier1",  
				pax:"$pax",
				pax_1:"$pax_1",
				pax_2:"$pax_2",
				revenue:"$revenue",
				revenue_1:"$revenue_1",
				revenue_2:"$revenue_2"
				}}
				//,{ $out : "cumulative"}
		])

				var cumu_network=db.JUP_DB_Cumulative_Market_share.find();
		if(cursor_country!=null){
			cursor_country.forEach(function(x){
				//print(x.combine_column)
				//db.JUP_DB_Cumulative_Trx_Date.insert(x)
				var year_str=x.year+"";
				var month_var=x.month+"";
				var combine_column_ = year_str.concat(month_var, x.region, x.country,x.pos,x.compartment,x.MarketingCarrier1); 
				var combine_month_ = month_var.concat( x.region, x.country,x.pos,x.compartment); 
								//print(combine_column_)  
				cumu_network.forEach(function(y){
					if(combine_column_==y.combine_column){
						x.pax+=y.pax;
						x.pax_1+=y.pax_1;
						x.pax_2+=y.pax_2;
						x.revenue+=y.revenue;
						x.revenue_1+=y.revenue_1;
						x.revenue_2+=y.revenue_2;
					}		
				})
			
				db.getCollection('JUP_DB_Cumulative_Market_share').update({
					combine_column:combine_column_ 
					},{
   						  combine_column:combine_column_,					
						  combine_month:combine_month_,
							year:x.year,
							month:x.month,
							region:x.region,
							country: x.country,
							pos:x.pos,
							compartment:x.compartment,
							MarketingCarrier1:x.MarketingCarrier1,
							pax:x.pax,
							pax_1:x.pax_1,
							pax_2:x.pax_2,
							revenue:x.revenue,
							revenue_1:x.revenue_1,
							revenue_2:x.revenue_2,
					},{"upsert" : true});
					
			})
		}

		
		// pos level
		var cursor_pos=db.JUP_DB_Market_Share.aggregate([
		{"$match":{
			"last_update_date" : {'$eq':date},
			"last_updated_time" : {'$eq':time},
			"pos" : {'$ne':null},
			"country" : {'$ne':null},
			"region" : {'$ne':null},
			"pos" : {'$ne':"NULL"},
			}},
			
		{"$group":{
			_id:{year:"$year",month:"$month",
			region:"$region",country:"$country",pos:"$pos",
			compartment : "$compartment",MarketingCarrier1 : "$MarketingCarrier1"},
				// pos:{ $first: null },
				// country:{ $first: null },
				 //region:{ $first: null },
				 pax:{ $sum: "$pax" },
				 pax_1:{ $sum: "$pax_1"},
				 pax_2:{ $sum: "$pax_2"},
				 revenue:{ $sum: "$revenue"},
 				 revenue_1:{ $sum: "$revenue_1"},
 				 revenue_2:{ $sum: "$revenue_2"}
			}},
			{"$project":{
				//combine_column: {$concat:["$_id.year", "$_id.month", "null", "null","null","$_id.compartment"]},
				year:"$_id.year",
				month:"$_id.month",
				region:"$_id.region",
				country: "$_id.country",
				pos:"$_id.pos",
				compartment:"$_id.compartment",
				MarketingCarrier1:"$_id.MarketingCarrier1",  
				pax:"$pax",
				pax_1:"$pax_1",
				pax_2:"$pax_2",
				revenue:"$revenue",
				revenue_1:"$revenue_1",
				revenue_2:"$revenue_2"
				}}
				//,{ $out : "cumulative"}
		])

				var cumu_network=db.JUP_DB_Cumulative_Market_share.find();
		if(cursor_pos!=null){
			cursor_pos.forEach(function(x){
				//print(x.combine_column)
				//db.JUP_DB_Cumulative_Trx_Date.insert(x)
				var year_str=x.year+"";
				var month_var=x.month+"";
				var combine_column_ = year_str.concat(month_var, x.region, x.country,x.pos,x.compartment,x.MarketingCarrier1); 
				var combine_month_ = month_var.concat( x.region, x.country,x.pos,x.compartment); 
								//print(combine_column_)  
				cumu_network.forEach(function(y){
					if(combine_column_==y.combine_column){
						x.pax+=y.pax;
						x.pax_1+=y.pax_1;
						x.pax_2+=y.pax_2;
						x.revenue+=y.revenue;
						x.revenue_1+=y.revenue_1;
						x.revenue_2+=y.revenue_2;
					}		
				})
			
				db.getCollection('JUP_DB_Cumulative_Market_share').update({
					combine_column:combine_column_ 
					},{
   						  combine_column:combine_column_,					
						  combine_month:combine_month_,
							year:x.year,
							month:x.month,
							region:x.region,
							country: x.country,
							pos:x.pos,
							compartment:x.compartment,
							MarketingCarrier1:x.MarketingCarrier1,
							pax:x.pax,
							pax_1:x.pax_1,
							pax_2:x.pax_2,
							revenue:x.revenue,
							revenue_1:x.revenue_1,
							revenue_2:x.revenue_2,
					},{"upsert" : true});
					
			})
		}
}