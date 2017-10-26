
// VLYR for POC testing

var book_rec = db.VLYR_Test.find({}).limit(10);

var book_rej = db.VLYR_Test.find({}).limit(10);
  
book_rec.forEach(function(x){
	
    var book_date = x.book_date;
    var book_date_format = new Date(book_date); 
    var dep_date = x.dep_date;
    var dep_date_format = new Date(dep_date); 
    var reduce_no_of_date = -364;
    var last_year_dep_date = new Date(dep_date);
	
    var last_year_book_date = new Date(last_year_dep_date);
    last_year_book_date.setDate(last_year_book_date.getDate() + reduce_no_of_date); 
    // For Last year
	var formated_book_date = (book_date_format.getFullYear()-1)  + "-" +("0" + (book_date_format.getMonth() + 1)).slice(-2) + "-" + ("0" + book_date_format.getDate()).slice(-2);
    //var formated_book_date = last_year_book_date.getFullYear()  + "-" +("0" + (last_year_book_date.getMonth() + 1)).slice(-2) + "-" + ("0" + last_year_book_date.getDate()).slice(-2);
    var formated_dep_date = (dep_date_format.getFullYear()-1)  + "-" + ("0" + (dep_date_format.getMonth() + 1)).slice(-2) + "-" + ("0" + dep_date_format.getDate()).slice(-2);
	
	// For next year
	var formated_next_yr_book_date = (book_date_format.getFullYear()+1)  + "-" +("0" + (book_date_format.getMonth() + 1)).slice(-2) + "-" + ("0" + book_date_format.getDate()).slice(-2);
    //var formated_book_date = last_year_book_date.getFullYear()  + "-" +("0" + (last_year_book_date.getMonth() + 1)).slice(-2) + "-" + ("0" + last_year_book_date.getDate()).slice(-2);
    var formated_next_yr_dep_date = (dep_date_format.getFullYear()+1)  + "-" + ("0" + (dep_date_format.getMonth() + 1)).slice(-2) + "-" + ("0" + dep_date_format.getDate()).slice(-2);
	
	
	print(book_date+"  current "+dep_date +"    "+formated_book_date +" last "+formated_dep_date )
	var pos = x.pos;
	var od = x.od;
	var compartment = x.compartment;
	var channel = x.channel;
	var farebasis = x.farebasis;
        
	db.VLYR_Test.find({
		"book_date":formated_book_date,"dep_date":formated_dep_date,
		"pos":pos,"od":od,"compartment":compartment ,"channel":channel,
                "farebasis":farebasis
	}).forEach(function(y){
					if(
						formated_book_date == y.book_date && 
						formated_dep_date == y.dep_date && 
						x.pos == y.pos && 
						x.od == y.od && 
						x.compartment == y.compartment &&
						x.channel == y.channel &&
						x.farebasis == y.farebasis
					){
						x.pax_1 = y.pax;
						print("update")
						db.VLYR_Test.update({
								book_date:x.book_date,
								dep_date:x.dep_date,
								pos : x.pos,
								od : x.od,
								compartment : x.compartment,
								channel : x.channel,
								farebasis :x.farebasis
								},{
								$set:{
									pax_1 : y.pax,
									ticket_1 : y.ticket
								}
						   })
						
					}
				  else{
					  print("outside")
					  
					  }  
				})
		        
	db.VLYR_Test.update(
                            {
                                book_date:formated_next_yr_book_date,
                                dep_date:formated_next_yr_dep_date,
                                pos : x.pos,
                                od : x.od,
                                compartment : x.compartment,
                                channel : x.channel,
                                farebasis :x.farebasis,
                            }
                            ,{
                                $set:{
                                    book_date:formated_next_yr_book_date,
                                    dep_date:formated_next_yr_dep_date,
									region : x.region,
									country : x.country,
                                    pos : x.pos,
                                    od : x.od,
                                    compartment : x.compartment,
                                    channel : x.channel,
                                    farebasis :x.farebasis,
                                    pax_1 : x.pax,
                                    ticket_1 : x.ticket
                                    }
                             },
							 {
								 upsert:true
							 }
							 )	
	})
	
	
	

