var date="";
var time="";
var dates=db.JUP_DB_Sales_Flown.find({},{last_updated_date:1,last_updated_time:1}).sort({last_updated_date:1,last_updated_time:1}).limit(1)
dates.forEach( function(myDoc) { date= myDoc.last_updated_date ,time=myDoc.last_updated_time} );
if(date!=null && time!=null){
        //print(date)
        //print(time)
		// network level

		var cursor_network=db.JUP_DB_Sales_Flown.aggregate([
		{"$match":{
			"last_updated_date" : {'$eq':date},
			"last_updated_time" : {'$eq':time},
			}},
			
		{"$group":{
			_id:{dep_date:"$dep_date",year:"$year",month:"$month",
			region:null,country:null,pos:null,
			compartment : "$compartment",channel : "$channel"},
			     
				 pos:{ $first: null },
				 country:{ $first: null },
				 region:{ $first: null },
				 pax:{ $sum: "$pax" },
				 pax_1:{ $sum: "$pax_1"},
				 pax_2:{ $sum: "$pax_2"},
				 revenue:{ $sum: "$revenue"},
				 revenue_1:{ $sum: "$revenue_1"},
				 revenue_2:{ $sum: "$revenue_2"},
				 revenue_base:{ $sum: "$revenue_base"},
				 revenue_base_1:{ $sum: "$revenue_base_1"},
				 revenue_base_2:{ $sum: "$revenue_base_2"},
			}},
			{"$project":{
				combine_column: {$concat:[ "$_id.dep_date", "null", "null","null","$_id.compartment","$_id.channel"]},
				dep_date:"$_id.dep_date",
				year:"$_id.year",
				month:"$_id.month",
				region:"$region",
				country: "$country",
				pos:"$pos",
				compartment:"$_id.compartment",
				channel:"$_id.channel",  
				flown_pax:"$pax",
				flown_pax_1:"$pax_1",
				flown_pax_2:"$pax_2",
				revenue:"$revenue",
				revenue_1:"$revenue_1",
				revenue_2:"$revenue_2",
				revenue_base:"$revenue_base",
				revenue_base_1:"$revenue_base_1",
				revenue_base_2:"$revenue_base_2"
				}}
				//,{ $out : "cumulative"}
		])

				var cumu_network=db.JUP_DB_Cumulative_Dep_Date.find();
		if(cursor_network!=null){
			cursor_network.forEach(function(x){
				//print(x.combine_column)
				//db.JUP_DB_Cumulative_Trx_Date.insert(x)
				var str1 =new Date(x.dep_date);
				var str2 =  ("" + (str1.getMonth() + 1)).slice(-2)+"";
				var str3 = str1.getDate()-1;
				var str4 = str1.getFullYear();
				var res = str4+"-"+str2+"-"+str3;
				var combine_month_ = str2.concat(x.region, x.country,x.pos,x.compartment); 
				var combine_column_ = x.dep_date.concat(x.region, x.country,x.pos,x.compartment,x.channel); 
				
								//print(combine_column_)  
				cumu_network.forEach(function(y){
					if(combine_column_==y.combine_column){
						x.flown_pax+=y.flown_pax;
						x.flown_pax_1+=y.flown_pax_1;
						x.flown_pax_2+=y.flown_pax_2;
						x.revenue+=y.revenue;
						x.revenue_1+=y.revenue_1;
						x.revenue_2+=y.revenue_2;
						x.revenue_base+=y.revenue_base;
						x.revenue_base_1+=y.revenue_base_1;
						x.revenue_base_2+=y.revenue_base_2;
					}		
				})
			
				db.getCollection('JUP_DB_Cumulative_Dep_Date').update({
					combine_column:combine_column_ 
					},{$set:{
   						    combine_column:combine_column_,					
							combine_month:combine_month_,
							dep_date:x.dep_date,
							year:x.year,
							month:x.month,
							region:x.region,
							country: x.country,
							pos:x.pos,
							compartment:x.compartment,
							channel:x.channel,
							flown_pax:x.flown_pax,
							flown_pax_1:x.flown_pax_1,
							flown_pax_2:x.flown_pax_2,
							flown_revenue:x.revenue,
							flown_revenue_1:x.revenue_1,
							flown_revenue_2:x.revenue_2,
							flown_revenue_base:x.revenue_base,
							flown_revenue_base_1:x.revenue_base_1,
							flown_revenue_base_2:x.revenue_base_2
							
					}},{"upsert" : true});
					
			})
		}

		
		// Region level


		var cursor_region=db.JUP_DB_Sales_Flown.aggregate([
		{"$match":{
			"last_updated_date" : {'$eq':date},
			"last_updated_time" : {'$eq':time},
			"region" : {'$ne':null}
			}},
			
		{"$group":{
			_id:{dep_date:"$dep_date",region: "$region",year:"$year",month:"$month",
			country:null,pos:null,
			compartment : "$compartment",channel : "$channel"},
				 pos:{ $first: null },
				 country:{ $first: null },
				 pax:{ $sum: "$pax" },
				 pax_1:{ $sum: "$pax_1"},
				 pax_2:{ $sum: "$pax_2"},
				 revenue:{ $sum: "$revenue"},
				 revenue_1:{ $sum: "$revenue_1"},
				 revenue_2:{ $sum: "$revenue_2"},
				 revenue_base:{ $sum: "$revenue_base"},
				 revenue_base_1:{ $sum: "$revenue_base_1"},
				 revenue_base_2:{ $sum: "$revenue_base_2"}
			}},
			{"$project":{
				combine_column: {$concat:[ "$_id.dep_date", "null", "null","null","$_id.compartment","$_id.channel"]},
				dep_date:"$_id.dep_date",
				year:"$_id.year",
				month:"$_id.month",
				region:"$_id.region",
				country: "$country",
				pos:"$pos",
				compartment:"$_id.compartment",
				channel:"$_id.channel",  
				flown_pax:"$pax",
				flown_pax_1:"$pax_1",
				flown_pax_2:"$pax_2",
				revenue:"$revenue",
				revenue_1:"$revenue_1",
				revenue_2:"$revenue_2",
				revenue_base:"$revenue_base",
				revenue_base_1:"$revenue_base_1",
				revenue_base_2:"$revenue_base_2"
				}}
				//,{ $out : "cumulative"}
		])

				var cumu_region=db.JUP_DB_Cumulative_Dep_Date.find();
		if(cursor_region!=null){
			cursor_region.forEach(function(x){
				print(x.combine_column)
				//db.JUP_DB_Cumulative_Trx_Date.insert(x)
				var str1 =new Date(x.dep_date);
				var str2 =  ("" + (str1.getMonth() + 1)).slice(-2)+"";
				var str3 = str1.getDate()-1;
				var str4 = str1.getFullYear();
				var res = str4+"-"+str2+"-"+str3;
				var combine_month_ = str2.concat(x.region, x.country,x.pos,x.compartment); 
				var combine_column_ = x.dep_date.concat(x.region, x.country,x.pos,x.compartment,x.channel); 
								//print(combine_column_)  
				cumu_region.forEach(function(y){
					if(combine_column_==y.combine_column){
						x.flown_pax+=y.flown_pax;
						x.flown_pax_1+=y.flown_pax_1;
						x.flown_pax_2+=y.flown_pax_2;
						x.revenue+=y.revenue;
						x.revenue_1+=y.revenue_1;
						x.revenue_2+=y.revenue_2;
						x.revenue_base+=y.revenue_base;
						x.revenue_base_1+=y.revenue_base_1;
						x.revenue_base_2+=y.revenue_base_2;
					}		
				})
			
				db.getCollection('JUP_DB_Cumulative_Dep_Date').update({
					combine_column:combine_column_ 
					},{$set:{
   						  combine_column:combine_column_,					
						  combine_month:combine_month_,
							dep_date:x.dep_date,
							year:x.year,
							month:x.month,
							region:x.region,
							country: x.country,
							pos:x.pos,
							compartment:x.compartment,
							channel:x.channel,
							flown_pax:x.flown_pax,
							flown_pax_1:x.flown_pax_1,
							flown_pax_2:x.flown_pax_2,
							flown_revenue:x.revenue,
							flown_revenue_1:x.revenue_1,
							flown_revenue_2:x.revenue_2,
							flown_revenue_base:x.revenue_base,
							flown_revenue_base_1:x.revenue_base_1,
							flown_revenue_base_2:x.revenue_base_2
					}},{"upsert" : true});
					
			})
		}

		
		// Country level cumulative

		var cursor_country=db.JUP_DB_Sales_Flown.aggregate([
		{"$match":{
			"last_updated_date" : {'$eq':date},
			"last_updated_time" : {'$eq':time},
			"country" : {'$ne':null}
			}},
			
		{"$group":{
			_id:{dep_date:"$dep_date",region: "$region",year:"$year",month:"$month",
			country : "$country",pos:null,
			compartment : "$compartment",channel : "$channel"},
				 pos:{ $first: null },
				 pax:{ $sum: "$pax" },
				 pax_1:{ $sum: "$pax_1"},
				 pax_2:{ $sum: "$pax_2"},
				 revenue:{ $sum: "$revenue"},
				 revenue_1:{ $sum: "$revenue_1"},
				 revenue_2:{ $sum: "$revenue_2"},
				 revenue_base:{ $sum: "$revenue_base"},
				 revenue_base_1:{ $sum: "$revenue_base_1"},
				 revenue_base_2:{ $sum: "$revenue_base_2"},
			}},
			{"$project":{
				combine_column: {$concat:[ "$_id.dep_date", "null", "null","null","$_id.compartment","$_id.channel"]},
				dep_date:"$_id.dep_date",
				year:"$_id.year",
				month:"$_id.month",
				region:"$_id.region",
				country: "$_id.country",
				pos:"$pos",
				compartment:"$_id.compartment",
				channel:"$_id.channel",  
				flown_pax:"$pax",
				flown_pax_1:"$pax_1",
				flown_pax_2:"$pax_2",
				revenue:"$revenue",
				revenue_1:"$revenue_1",
				revenue_2:"$revenue_2",
				revenue_base:"$revenue_base",
				revenue_base_1:"$revenue_base_1",
				revenue_base_2:"$revenue_base_2"
				}}
				//,{ $out : "cumulative"}
		])

				var cumu_country=db.JUP_DB_Cumulative_Dep_Date.find();
		if(cursor_country!=null){
			cursor_country.forEach(function(x){
				print(x.combine_column)
				//db.JUP_DB_Cumulative_Trx_Date.insert(x)
				var str1 =new Date(x.dep_date);
				var str2 =  ("" + (str1.getMonth() + 1)).slice(-2)+"";
				var str3 = str1.getDate()-1;
				var str4 = str1.getFullYear();
				var res = str4+"-"+str2+"-"+str3;
				var combine_month_ = str2.concat(x.region, x.country,x.pos,x.compartment); 
				var combine_column_ = x.dep_date.concat(x.region, x.country,x.pos,x.compartment,x.channel); 
								//print(combine_column_)  
				cumu_country.forEach(function(y){
					if(combine_column_==y.combine_column){
						x.flown_pax+=y.flown_pax;
						x.flown_pax_1+=y.flown_pax_1;
						x.flown_pax_2+=y.flown_pax_2;
						x.revenue+=y.revenue;
						x.revenue_1+=y.revenue_1;
						x.revenue_2+=y.revenue_2;
						x.revenue_base+=y.revenue_base;
						x.revenue_base_1+=y.revenue_base_1;
						x.revenue_base_2+=y.revenue_base_2;
					}		
				})
			
				db.getCollection('JUP_DB_Cumulative_Dep_Date').update({
					combine_column:combine_column_ 
					},{$set:{
   						  combine_column:combine_column_,					
						  combine_month:combine_month_,
							dep_date:x.dep_date,
							year:x.year,
							month:x.month,
							region:x.region,
							country: x.country,
							pos:x.pos,
							compartment:x.compartment,
							channel:x.channel,
							flown_pax:x.flown_pax,
							flown_pax_1:x.flown_pax_1,
							flown_pax_2:x.flown_pax_2,
							flown_revenue:x.revenue,
							flown_revenue_1:x.revenue_1,
							flown_revenue_2:x.revenue_2,
							flown_revenue_base:x.revenue_base,
							flown_revenue_base_1:x.revenue_base_1,
							flown_revenue_base_2:x.revenue_base_2
					}},{"upsert" : true});
					
			})
		}


		// pos level access
		var cursor_pos=db.JUP_DB_Sales_Flown.aggregate([
		{"$match":{
			"last_updated_date" : {'$eq':date},
			"last_updated_time" : {'$eq':time},
			"pos" : {'$ne':null}
			}},
			
		{"$group":{
			_id:{dep_date:"$dep_date",region: "$region",year:"$year",month:"$month",
			country : "$country",
			pos : "$pos",compartment : "$compartment",channel : "$channel"},
			
				 pax:{ $sum: "$pax" },
				 pax_1:{ $sum: "$pax_1"},
				 pax_2:{ $sum: "$pax_2"},
				 revenue:{ $sum: "$revenue"},
				 revenue_1:{ $sum: "$revenue_1"},
				 revenue_2:{ $sum: "$revenue_2"},
				 revenue_base:{ $sum: "$revenue_base"},
				 revenue_base_1:{ $sum: "$revenue_base_1"},
				 revenue_base_2:{ $sum: "$revenue_base_2"},
			}},
		{"$project":{
				combine_column: {$concat:[ "$_id.dep_date", "null", "null","null","$_id.compartment","$_id.channel"]},
				dep_date:"$_id.dep_date",
				year:"$_id.year",
				month:"$_id.month",
				region:"$_id.region",
				country: "$_id.country",
				pos:"$_id.pos",
				compartment:"$_id.compartment",
				channel:"$_id.channel",  
				flown_pax:"$pax",
				flown_pax_1:"$pax_1",
				flown_pax_2:"$pax_2",
				revenue:"$revenue",
				revenue_1:"$revenue_1",
				revenue_2:"$revenue_2",
				revenue_base:"$revenue_base",
				revenue_base_1:"$revenue_base_1",
				revenue_base_2:"$revenue_base_2"
			}}
			//,{ $out : "cumulative"}
		])

		var cumu_pos=db.JUP_DB_Cumulative_Dep_Date.find();
		if(cursor_pos!=null){
			cursor_pos.forEach(function(x){
				print(x.combine_column)
				//db.JUP_DB_Cumulative_Trx_Date.insert(x)
				var str1 =new Date(x.dep_date);
				var str2 =  ("" + (str1.getMonth() + 1)).slice(-2)+"";
				var str3 = str1.getDate()-1;
				var str4 = str1.getFullYear();
				var res = str4+"-"+str2+"-"+str3;
				var combine_month_ = str2.concat(x.region, x.country,x.pos,x.compartment); 
				var combine_column_ = x.dep_date.concat(x.region, x.country,x.pos,x.compartment,x.channel); 
								//print(combine_column_)  
				cumu_pos.forEach(function(y){
					if(combine_column_==y.combine_column){
						x.flown_pax+=y.flown_pax;
						x.flown_pax_1+=y.flown_pax_1;
						x.flown_pax_2+=y.flown_pax_2;
						x.revenue+=y.revenue;
						x.revenue_1+=y.revenue_1;
						x.revenue_2+=y.revenue_2;
						x.revenue_base+=y.revenue_base;
						x.revenue_base_1+=y.revenue_base_1;
						x.revenue_base_2+=y.revenue_base_2;
					}		
				})
			
				db.getCollection('JUP_DB_Cumulative_Dep_Date').update({
					combine_column:combine_column_ 
					},{$set:{
   						  combine_column:combine_column_,	
							combine_month:combine_month_,	
							dep_date:x.dep_date,
							year:x.year,
							month:x.month,
							region:x.region,
							country: x.country,
							pos:x.pos,
							compartment:x.compartment,
							channel:x.channel,
							flown_pax:x.flown_pax,
							flown_pax_1:x.flown_pax_1,
							flown_pax_2:x.flown_pax_2,
							flown_revenue:x.revenue,
							flown_revenue_1:x.revenue_1,
							flown_revenue_2:x.revenue_2,
							flown_revenue_base:x.revenue_base,
							flown_revenue_base_1:x.revenue_base_1,
							flown_revenue_base_2:x.revenue_base_2
					}},{"upsert" : true});
					
			})
		}


}
