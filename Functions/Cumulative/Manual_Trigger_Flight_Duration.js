
    
db.JUP_DB_Market_Share.aggregate([
{ $lookup:
     {
       from: 'JUP_DB_Competitor_Ratings',
       localField: 'od',
       foreignField: 'od',
       as: 'ratings'
    }
 },
  {$unwind:{ path: "$ratings", preserveNullAndEmptyArrays: true }},
  {
     $addFields:{rating:{
              $filter: {
                 input: '$ratings.rating_for_DB',
                 as: "rate",
                 cond: { $eq: [ '$$rate.airline','$MarketingCarrier1' ] }
              }
    }} 
  },
    {$unwind:{ path: "$rating", preserveNullAndEmptyArrays: true }},
    {$out:'JUP_DB_Market_Share_Rating'}
])
    


    
// For duration

db.JUP_DB_OD_Duration.aggregate([
{$group:{
        _id:{Dept_Sta:'$Dept Sta',Arvl_Sta:'$Arvl Sta',Flt_Num:'$Flt Num'},
        duration:{$max:'$flight_duration_minutes'}
}},
{$out:'temp_collection'}
])
 
var num = 1;
var bulk = db.JUP_DB_Manual_Triggers_Module.initializeOrderedBulkOp();
db.temp_collection.find().forEach(function(x){
    var od = x._id.Dept_Sta+''+x._id.Arvl_Sta;
        bulk.find({
            od:od
        }).upsert().update(
        {
          $set:{
             'Flight_duration.Flt_Num':x._id.Flt_Num,
             'Flight_duration.duration':x.duration
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
  



