var num = 1;
var bulk = db.JUP_DB_Manual_Triggers_Module.initializeUnorderedBulkOp();
db.JUP_DB_Manual_Triggers_Module.find({"trx_year" : 2016,"trx_date":{$ne:null}}).forEach(function(x){
	
		var trx_date = x.trx_date;
		var book_date_format = new Date(trx_date); 
		var dep_date = x.dep_date;
		var dep_date_format = new Date(dep_date);
		var increase_no_of_trx_date = 364;
        var next_year_book_date = book_date_format;
        next_year_book_date.setDate(next_year_book_date.getDate() + increase_no_of_trx_date); 
        var formated_book_date = (next_year_book_date.getFullYear())  + "-" +("0" + (next_year_book_date.getMonth() + 1)).slice(-2) + "-" + ("0" + next_year_book_date.getDate()).slice(-2);
        var formated_dep_date = (dep_date_format.getFullYear()+1)  + "-" +("0" + (dep_date_format.getMonth() + 1)).slice(-2) + "-" + ("0" + dep_date_format.getDate()).slice(-2);
        //print(formated_book_date+'   '+formated_dep_date)
        var formated_book_month = next_year_book_date.getMonth() + 1;
        var formated_book_year = next_year_book_date.getFullYear();
        var formated_dep_month = dep_date_format.getMonth() + 1;
        var formated_dep_year = dep_date_format.getFullYear()+1;
        //print('formated_book_year '+formated_dep_year )
		
		
		if ("book_pax" in x) {
			//result = true;
		}
		else{
			x['book_pax'] = {'snap_value':0};
			x['book_revenue'] = {'snap_value':0};
		}
		
		if ("sale_pax" in x) {
			//result = true;
		}
		else{
			x['sale_pax'] = {'snap_value':0};
			x['sale_revenue'] = {'snap_value':0};
			
		}
		if ("flown_pax" in x) {
			//result = true;
		}
		else{
			x['flown_pax'] = {'snap_value':0};
			x['flown_revenue'] = {'snap_value':0};
			
		}
//print(x);
        bulk.find({
                      'pos.City' : x.pos.City,
//                      'origin.City' : x.origin.City,
                      'od' : x.od,
                      'compartment.compartment' : x.compartment.compartment,
                      trx_date: formated_book_date,
                      dep_date: formated_dep_date,
                      
        }).upsert().update(
            {
              $set:{
                      'pos' : x.pos,
                      'origin' : x.origin,
                      'destination' : x.destination,
                      'compartment' : x.compartment,
					  od:x.od,
                      trx_date: formated_book_date,
                      dep_date: formated_dep_date,
                      trx_month:formated_book_month,
                      trx_year:formated_book_year,
                      dep_month:formated_dep_month,
                      dep_year:formated_dep_year,  
					  'book_pax.value_1':x.book_pax.value,
					  'book_revenue.value_1':x.book_revenue.value,
					  'sale_pax.value_1':x.sale_pax.value,
					  'sale_revenue.value_1':x.sale_revenue.value,
					  'flown_pax.value_1':x.flown_pax.value,
					  'flown_revenue.value_1':x.flown_revenue.value,
					  
              }
             }
        );
        
		 if ( num % 1000 == 0 ){
          bulk.execute();
          bulk = db.JUP_DB_Manual_Triggers_Module.initializeUnorderedBulkOp();
        }

        num++;	
});
bulk.execute(); 
