var date="";
var time="";
var dates=db.JUP_DB_Booking_DepDate.find({},{last_updated_date:1,last_updated_time:1}).sort({last_updated_date:1,last_updated_time:1}).limit(1)
dates.forEach( function(myDoc) { date= myDoc.last_updated_date ,time=myDoc.last_updated_time} );
if(date!=null && time!=null){
        //print(date)
        //print(time)
		// network level

		var cursor_network=db.JUP_DB_Booking_DepDate.aggregate([
		{"$match":{
			"last_updated_date" : {'$eq':date},
			"last_updated_time" : {'$eq':time},
			}},
			
		{"$group":{
			_id:{dep_date:"$dep_date",
			region:null,country:null,pos:null,
			compartment : "$compartment",channel : "$channel"},
			
				 pos:{ $first: null },
				 country:{ $first: null },
				 region:{ $first: null },
				 pax:{ $sum: "$pax" },
				 pax_1:{ $sum: "$pax_1"},
				 ticket:{ $sum: "$ticket"},
				 ticket_1:{ $sum: "$ticket_1"}
			}},
			{"$project":{
				combine_column: {$concat:[ "$_id.dep_date", "null", "null","null","$_id.compartment","$_id.channel"]},
				dep_date:"$_id.dep_date",
				region:"$region",
				country: "$country",
				pos:"$pos",
				compartment:"$_id.compartment",
				channel:"$_id.channel",  
				pax:"$pax",
				pax_1:"$pax_1",
				ticket:"$ticket",
				ticket_1:"$ticket_1"
				}}
				//,{ $out : "cumulative"}
		])

				var cumu_network=db.JUP_DB_Cumulative_Dep_Date.find();
		if(cursor_network!=null){
			cursor_network.forEach(function(x){
				print(x.combine_column)
				//db.JUP_DB_Cumulative_Trx_Date.insert(x)
				var str1 =new Date(x.dep_date);
				var str2 = ("" + (str1.getMonth() + 1)).slice(-2)+"";
				var str3 = str1.getDate()-1;
				var str4 = str1.getFullYear();
				var res = str4+"-"+str2+"-"+str3;
				var combine_month_ = str2.concat(x.region, x.country,x.pos,x.compartment); 
				
				var combine_column_ = x.dep_date.concat(x.region, x.country,x.pos,x.compartment,x.channel); 
								//print(combine_column_)  
				cumu_network.forEach(function(y){
					if(combine_column_==y.combine_column){
						x.pax+=y.pax;
						x.pax_1+=y.pax_1;
						x.ticket+=y.ticket;
						x.ticket_1+=y.ticket_1;
					}		
				})
			
				db.getCollection('JUP_DB_Cumulative_Dep_Date').update({
					combine_column:combine_column_ 
					},{$set:{
   						  combine_column:combine_column_,					
						  combine_month:combine_month_,
							dep_date:x.dep_date,
							region:x.region,
							country: x.country,
							pos:x.pos,
							compartment:x.compartment,
							channel:x.channel,
							pax:x.pax,
							pax_1:x.pax_1,
							ticket:x.ticket,
							ticket_1:x.ticket_1
					}},{"upsert" : true});
					
			})
		}

		
		// Region level


		var cursor_region=db.JUP_DB_Booking_DepDate.aggregate([
		{"$match":{
			"last_updated_date" : {'$eq':date},
			"last_updated_time" : {'$eq':time},
			"region" : {'$ne':null}
			}},
			
		{"$group":{
			_id:{dep_date:"$dep_date",region: "$region",
			country:null,pos:null,
			compartment : "$compartment",channel : "$channel"},
				 pos:{ $first: null },
				 country:{ $first: null },
				 pax:{ $sum: "$pax" },
				 pax_1:{ $sum: "$pax_1"},
				 ticket:{ $sum: "$ticket"},
				 ticket_1:{ $sum: "$ticket_1"}
			}},
			{"$project":{
				combine_column: {$concat:[ "$_id.dep_date", "$_id.region", "null","null","$_id.compartment","$_id.channel"]},
				dep_date:"$_id.dep_date",
				region:"$_id.region",
				country: "$country",
				pos:"$pos",
				compartment:"$_id.compartment",
			   channel:"$_id.channel",  
				pax:"$pax",
				pax_1:"$pax_1",
				ticket:"$ticket",
				ticket_1:"$ticket_1"
				}}
				//,{ $out : "cumulative"}
		])

				var cumu_region=db.JUP_DB_Cumulative_Dep_Date.find();
		if(cursor_region!=null){
			cursor_region.forEach(function(x){
				print(x.combine_column)
				//db.JUP_DB_Cumulative_Trx_Date.insert(x)
				var combine_column_ = x.dep_date.concat(x.region, x.country,x.pos,x.compartment,x.channel); 
				var str1 =new Date(x.dep_date);
				var str2 = ("" + (str1.getMonth() + 1)).slice(-2)+"";
				var str3 = str1.getDate()-1;
				var str4 = str1.getFullYear();
				var res = str4+"-"+str2+"-"+str3;
				var combine_month_ = str2.concat(x.region, x.country,x.pos,x.compartment); 
								//print(combine_column_)  
				cumu_region.forEach(function(y){
					if(combine_column_==y.combine_column){
						x.pax+=y.pax;
						x.pax_1+=y.pax_1;
						x.ticket+=y.ticket;
						x.ticket_1+=y.ticket_1;
					}		
				})
			
				db.getCollection('JUP_DB_Cumulative_Dep_Date').update({
					combine_column:combine_column_ 
					},{$set:{
   						  combine_column:combine_column_,					
						  combine_month:combine_month_,
							dep_date:x.dep_date,
							region:x.region,
							country: x.country,
							pos:x.pos,
							compartment:x.compartment,
							channel:x.channel,
							pax:x.pax,
							pax_1:x.pax_1,
							ticket:x.ticket,
							ticket_1:x.ticket_1
					}},{"upsert" : true});
					
			})
		}

		
		// Country level cumulative

		var cursor_country=db.JUP_DB_Booking_DepDate.aggregate([
		{"$match":{
			"last_updated_date" : {'$eq':date},
			"last_updated_time" : {'$eq':time},
			"country" : {'$ne':null}
			}},
			
		{"$group":{
			_id:{dep_date:"$dep_date",region: "$region",
			country : "$country",pos:null,
			compartment : "$compartment",channel : "$channel"},
				 pos:{ $first: null },
				 pax:{ $sum: "$pax" },
				 pax_1:{ $sum: "$pax_1" },
				 ticket:{ $sum: "$ticket" },
				 ticket_1:{ $sum: "$ticket_1" }
			}},
			{"$project":{
				combine_column: {$concat:[ "$_id.dep_date", "$_id.region", "$_id.country","null","$_id.compartment","$_id.channel"]},
				dep_date:"$_id.dep_date",
				region:"$_id.region",
				country: "$_id.country",
				pos:"$pos",
				compartment:"$_id.compartment",
			   channel:"$_id.channel",  
				pax:"$pax",
				pax_1:"$pax_1",
				ticket:"$ticket",
				ticket_1:"$ticket_1"
				}}
				//,{ $out : "cumulative"}
		])

				var cumu_country=db.JUP_DB_Cumulative_Dep_Date.find();
		if(cursor_country!=null){
			cursor_country.forEach(function(x){
				print(x.combine_column)
				//db.JUP_DB_Cumulative_Trx_Date.insert(x)
				var combine_column_ = x.dep_date.concat(x.region, x.country,x.pos,x.compartment,x.channel); 
				var str1 =new Date(x.dep_date);
				var str2 = ("" + (str1.getMonth() + 1)).slice(-2)+"";
				var str3 = str1.getDate()-1;
				var str4 = str1.getFullYear();
				var res = str4+"-"+str2+"-"+str3;
				var combine_month_ = str2.concat(x.region, x.country,x.pos,x.compartment); 
								//print(combine_column_)  
				cumu_country.forEach(function(y){
					if(combine_column_==y.combine_column){
						x.pax+=y.pax;
						x.pax_1+=y.pax_1;
						x.ticket+=y.ticket;
						x.ticket_1+=y.ticket_1;
					}		
				})
			
				db.getCollection('JUP_DB_Cumulative_Dep_Date').update({
					combine_column:combine_column_ 
					},{$set:{
   						  combine_column:combine_column_,					
						  combine_month:combine_month_,
							dep_date:x.dep_date,
							region:x.region,
							country: x.country,
							pos:x.pos,
							compartment:x.compartment,
							channel:x.channel,
							pax:x.pax,
							pax_1:x.pax_1,
							ticket:x.ticket,
							ticket_1:x.ticket_1
					}},{"upsert" : true});
					
			})
		}


		// pos level access
		var cursor_pos=db.JUP_DB_Booking_DepDate.aggregate([
		{"$match":{
			"last_updated_date" : {'$eq':date},
			"last_updated_time" : {'$eq':time},
			"pos" : {'$ne':null}
			}},
			
		{"$group":{
			_id:{dep_date:"$dep_date",region: "$region",
			country : "$country",
			pos : "$pos",compartment : "$compartment",channel : "$channel"},
			
				 pax:{ $sum: "$pax" },
				 pax_1:{ $sum: "$pax_1" },
				 ticket:{ $sum: "$ticket" },
				 ticket_1:{ $sum: "$ticket_1" }
			}},
		{"$project":{
				combine_column: {$concat:[ "$_id.dep_date", "$_id.region", "$_id.country","$_id.pos","$_id.compartment","$_id.channel"]},
				dep_date:"$_id.dep_date",
				region:"$_id.region",
				country: "$_id.country",
				pos:"$_id.pos",
				compartment:"$_id.compartment",
			   channel:"$_id.channel",  
				pax:"$pax",
				pax_1:"$pax_1",
				ticket:"$ticket",
				ticket_1:"$ticket_1"
			}}
			//,{ $out : "cumulative"}
		])

		var cumu_pos=db.JUP_DB_Cumulative_Dep_Date.find();
		if(cursor_pos!=null){
			cursor_pos.forEach(function(x){
				print(x.combine_column)
				//db.JUP_DB_Cumulative_Trx_Date.insert(x)
				var combine_column_ = x.dep_date.concat(x.region, x.country,x.pos,x.compartment,x.channel); 
				var str1 =new Date(x.dep_date);
				var str2 = ("" + (str1.getMonth() + 1)).slice(-2)+"";
				var str3 = str1.getDate()-1;
				var str4 = str1.getFullYear();
				var res = str4+"-"+str2+"-"+str3;
				var combine_month_ = str2.concat(x.region, x.country,x.pos,x.compartment); 
								//print(combine_column_)  
				cumu_pos.forEach(function(y){
					if(combine_column_==y.combine_column){
						x.pax+=y.pax;
						x.pax_1+=y.pax_1;
						x.ticket+=y.ticket;
						x.ticket_1+=y.ticket_1;
					}		
				})
			
				db.getCollection('JUP_DB_Cumulative_Dep_Date').update({
					combine_column:combine_column_ 
					},{$set:{
   						  combine_column:combine_column_,					
						  combine_month:combine_month_,
							dep_date:x.dep_date,
							region:x.region,
							country: x.country,
							pos:x.pos,
							compartment:x.compartment,
							channel:x.channel,
							pax:x.pax,
							pax_1:x.pax_1,
							ticket:x.ticket,
							ticket_1:x.ticket_1
					}},{"upsert" : true});
					
			})
		}


}
