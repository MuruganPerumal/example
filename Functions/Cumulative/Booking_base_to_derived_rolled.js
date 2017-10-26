// Author : Murugan
// Date   : 2017-09-21 06.00 AM

var _sanp_date = '2017-08-03';
var rawDB = db.getSiblingDB('rawData');

var personOrg = rawDB.JUP_DB_Booking_1Month.aggregate([
{$match:{"snap_date" : _sanp_date }},
{$group:{
    _id:{PERSON_ORG_ID:'$PERSON_ORG_ID',CONFIRMATION_NUM:'$CONFIRMATION_NUM'},
    num : {$sum:1}
    }}
]
)

var num = 1;    
var bulk = db.JUP_DB_Booking_DepDate_Brand_New.initializeUnorderedBulkOp();
var bulk_insert = db.JUP_DB_Booking_DepDate_Brand_New.initializeUnorderedBulkOp();
personOrg.forEach(function(x){

		// Don't take enroledflag as true because we are taking only latest data 
        var dataDoc = db.JUP_DB_Booking_DepDate_Brand_New.find({"PERSON_ORG_ID" : x._id.PERSON_ORG_ID,
    			"CONFIRMATION_NUM" : x._id.CONFIRMATION_NUM,"enroledflag":{$ne:true}});
        dataDoc.forEach(function(y){
    	
    		// Update the existing row
            bulk.find({
                   _id : y._id
              }).update(
                    {
                      $set:{
                          'enroledflag':true
                          }
                     }
                );

             // insert the enrolled rows
                y.pax = y.pax;
                y.revenue_base = y.revenue_base;
                y.revenue = y.revenue ;   
                y.snap_date = _sanp_date;           
                y.enroledflag = true;
                y.isvoid = true;
             	delete y['_id'];

             	bulk_insert.insert(y);
                //print(y)
             if ( num % 1000 == 0 ){
                 bulk_insert.execute();
                  bulk_insert = db.JUP_DB_Booking_DepDate_Brand_New.initializeUnorderedBulkOp(); 
                 bulk.execute();
                  bulk = db.JUP_DB_Booking_DepDate_Brand_New.initializeUnorderedBulkOp();
              }
              num++;
            
            });
});
bulk.execute();
bulk_insert.execute();



