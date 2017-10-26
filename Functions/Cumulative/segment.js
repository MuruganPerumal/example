
var num = 1;
var bulk = db.JUP_DB_Sales_Flown.initializeUnorderedBulkOp();
db.JUP_DB_Sales_Flown.find({segment:{$ne:null}}).forEach(function(sale_data){
		sale_data.segment=null;
	//	print(sale_data.region)
			//print(sale_data.fare_basis)
		db.JUP_DB_Segment_Rules.find().forEach(function(segs){
		//  print(segs.segment)
			if(segs.rule_id==1){
			  		//print("inside")
				if(sale_data.dep_date>=segs.dep_date & sale_data.dep_date<=segs.arr_date & sale_data.origin==segs.origin & sale_data.destination==segs.destination){
						sale_data.segment=segs.segment ;
						//print("inside")
				}
			}	
			else if(segs.rule_id==3){
					if(sale_data.fare_basis.includes(segs.farebasis_code)){
						sale_data.segment=segs.segment 
						//	print("inside")
				}
			}
			else if(segs.rule_id==2){
				
					if(sale_data.origin==segs.origin & sale_data.pos==segs.pos & sale_data.fare_basis.includes(segs.farebasis_code)){
						sale_data.segment=segs.segment 
											//	print("inside")
				}
			}
			
			else if(segs.rule_id==6){
					
					var dest = segs.destination.split(",");
					var num = 0;
					for(num=0;num<dest.length;num++){
						if(sale_data.Country_Origin==segs.origin & sale_data.country==segs.pos & dest[num]==sale_data.destination & sale_data.dep_date>=segs.dep_date & sale_data.dep_date<=segs.arr_date){
							sale_data.segment=segs.segment 
											//	print("inside")
						}
					}
					
			}
//			print(sale_data)

		});

		bulk.find({
			_id:sale_data._id
		}).upsert().update(
            {
              $set:{
                 segment:sale_data.segment
                  }
             }
        );

     if ( num % 1000 == 0 ){
          bulk.execute();
          bulk = db.JUP_DB_Sales_Flown.initializeUnorderedBulkOp();
      }
      num++;
	});
	
bulk.execute();    	  
