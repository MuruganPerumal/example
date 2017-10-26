var date="";
var time="";
var dates=db.JUP_DB_Forecast_OD.find({},{last_update_date:1,last_update_time:1}).sort({last_update_date:1,last_update_time:1}).limit(1)
dates.forEach( function(myDoc) { date= myDoc.last_update_date ,time=myDoc.last_update_time} );
if(date!=null && time!=null){
// Network level
var cursor_network=db.JUP_DB_Forecast_OD.aggregate([
		{"$match":{
			"last_update_date" : {'$eq':date},
			"last_update_time" : {'$eq':time},
			}},
			
		{"$group":{
			_id:{year:"$Year",month:"$Month",
			region:null,country:null,pos:null,
			compartment : "$compartment"},
				 pos:{ $first: null },
				 country:{ $first: null },
				 region:{ $first: null },
				 pax:{ $sum: "$pax" },
				 average_fare:{ $sum: "$average_fare"},
				 revenue:{ $sum: "$revenue"},
			}},
			{"$project":{
				//combine_column: {$concat:["$_id.year", "$_id.month", "null", "null","null","$_id.compartment"]},
				year:"$_id.year",
				month:"$_id.month",
				region:"$region",
				country: "$country",
				pos:"$pos",
				compartment:"$_id.compartment",
				
				pax:"$pax",
				revenue:"$revenue",
				average_fare:"$average_fare"
				}}
				//,{ $out : "cumulative"}
		])

				var cumu_network=db.JUP_DB_Cumulative_Forecast.find();
		if(cursor_network!=null){
			cursor_network.forEach(function(x){
				//print(x.combine_column)
				//db.JUP_DB_Cumulative_Trx_Date.insert(x)
				var year_str=x.year+"";
				var month_var=x.month+"";
				var combine_column_ = year_str.concat(month_var, x.region, x.country,x.pos,x.compartment); 
				var combine_month_ = month_var.concat( x.region, x.country,x.pos,x.compartment); 
								//print(combine_column_)  
				cumu_network.forEach(function(y){
					if(combine_column_==y.combine_column){
						x.pax+=y.pax;
						x.revenue+=y.revenue;
						x.average_fare+=y.average_fare;
					}		
				})
			
				db.getCollection('JUP_DB_Cumulative_Forecast').update({
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
							
							pax:x.pax,
							revenue:x.revenue,
							average_fare:x.average_fare,
					},{"upsert" : true});
					
			})
		}
		
		// region
		var cursor_region=db.JUP_DB_Forecast_OD.aggregate([
		{"$match":{
			"last_update_date" : {'$eq':date},
			"last_update_time" : {'$eq':time},
			}},
			
		{"$group":{
			_id:{year:"$Year",month:"$Month",
			region:"$region",country:null,pos:null,
			compartment : "$compartment"},
				 pos:{ $first: null },
				 country:{ $first: null },
				 //region:{ $first: null },
				 pax:{ $sum: "$pax" },
				 average_fare:{ $sum: "$average_fare"},
				 revenue:{ $sum: "$revenue"},
			}},
			{"$project":{
				//combine_column: {$concat:["$_id.year", "$_id.month", "null", "null","null","$_id.compartment"]},
				year:"$_id.year",
				month:"$_id.month",
				region:"$_id.region",
				country: "$country",
				pos:"$pos",
				compartment:"$_id.compartment",
				
				pax:"$pax",
				revenue:"$revenue",
				average_fare:"$average_fare"
				}}
				//,{ $out : "cumulative"}
		])

				var cumu_network=db.JUP_DB_Cumulative_Forecast.find();
		if(cursor_region!=null){
			cursor_region.forEach(function(x){
				//print(x.combine_column)
				//db.JUP_DB_Cumulative_Trx_Date.insert(x)
				var year_str=x.year+"";
				var month_var=x.month+"";
				var combine_column_ = year_str.concat(month_var, x.region, x.country,x.pos,x.compartment); 
				var combine_month_ = month_var.concat( x.region, x.country,x.pos,x.compartment); 
								//print(combine_column_)  
				cumu_network.forEach(function(y){
					if(combine_column_==y.combine_column){
						x.pax+=y.pax;
						x.revenue+=y.revenue;
						x.average_fare+=y.average_fare;
					}		
				})
			
				db.getCollection('JUP_DB_Cumulative_Forecast').update({
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
							
							pax:x.pax,
							revenue:x.revenue,
							average_fare:x.average_fare,
					},{"upsert" : true});
					
			})
		}

		// country
		var cursor_country=db.JUP_DB_Forecast_OD.aggregate([
		{"$match":{
			"last_update_date" : {'$eq':date},
			"last_update_time" : {'$eq':time},
			}},
			
		{"$group":{
			_id:{year:"$Year",month:"$Month",
			region:"$region",country:"$country",pos:null,
			compartment : "$compartment"},
				 pos:{ $first: null },
				// country:{ $first: null },
				 //region:{ $first: null },
				pax:{ $sum: "$pax" },
				 average_fare:{ $sum: "$average_fare"},
				 revenue:{ $sum: "$revenue"},
			}},
			{"$project":{
				//combine_column: {$concat:["$_id.year", "$_id.month", "null", "null","null","$_id.compartment"]},
				year:"$_id.year",
				month:"$_id.month",
				region:"$_id.region",
				country: "$_id.country",
				pos:"$pos",
				compartment:"$_id.compartment",
				
				pax:"$pax",
				revenue:"$revenue",
				average_fare:"$average_fare"
				}}
				//,{ $out : "cumulative"}
		])

				var cumu_network=db.JUP_DB_Cumulative_Forecast.find();
		if(cursor_country!=null){
			cursor_country.forEach(function(x){
				//print(x.combine_column)
				//db.JUP_DB_Cumulative_Trx_Date.insert(x)
				var year_str=x.year+"";
				var month_var=x.month+"";
				var combine_column_ = year_str.concat(month_var, x.region, x.country,x.pos,x.compartment); 
				var combine_month_ = month_var.concat( x.region, x.country,x.pos,x.compartment); 
								//print(combine_column_)  
				cumu_network.forEach(function(y){
					if(combine_column_==y.combine_column){
						x.pax+=y.pax;
						x.revenue+=y.revenue;
						x.average_fare+=y.average_fare;
					}		
				})
			
				db.getCollection('JUP_DB_Cumulative_Forecast').update({
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
							
							pax:x.pax,
							revenue:x.revenue,
							average_fare:x.average_fare,
					},{"upsert" : true});
					
			})
		}

		
		// pos level
		var cursor_pos=db.JUP_DB_Forecast_OD.aggregate([
		{"$match":{
			"last_update_date" : {'$eq':date},
			"last_update_time" : {'$eq':time},
			}},
			
		{"$group":{
			_id:{year:"$Year",month:"$Month",
			region:"$region",country:"$country",pos:"$pos",
			compartment : "$compartment"},
				// pos:{ $first: null },
				// country:{ $first: null },
				 //region:{ $first: null },
				 pax:{ $sum: "$pax" },
				 average_fare:{ $sum: "$average_fare"},
				 revenue:{ $sum: "$revenue"},
			}},
			{"$project":{
				//combine_column: {$concat:["$_id.year", "$_id.month", "null", "null","null","$_id.compartment"]},
				year:"$_id.year",
				month:"$_id.month",
				region:"$_id.region",
				country: "$_id.country",
				pos:"$_id.pos",
				compartment:"$_id.compartment",
				
				pax:"$pax",
				revenue:"$revenue",
				average_fare:"$average_fare"
				}}
				//,{ $out : "cumulative"}
		])

				var cumu_network=db.JUP_DB_Cumulative_Forecast.find();
		if(cursor_pos!=null){
			cursor_pos.forEach(function(x){
				//print(x.combine_column)
				//db.JUP_DB_Cumulative_Trx_Date.insert(x)
				var year_str=x.year+"";
				var month_var=x.month+"";
				var combine_column_ = year_str.concat(month_var, x.region, x.country,x.pos,x.compartment); 
				var combine_month_ = month_var.concat( x.region, x.country,x.pos,x.compartment); 
								//print(combine_column_)  
				cumu_network.forEach(function(y){
					if(combine_column_==y.combine_column){
						x.pax+=y.pax;
						x.revenue+=y.revenue;
						x.average_fare+=y.average_fare;
					}		
				})
			
				db.getCollection('JUP_DB_Cumulative_Forecast').update({
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
							
							pax:x.pax,
							revenue:x.revenue,
							average_fare:x.average_fare,
					},{"upsert" : true});
					
			})
		}
}