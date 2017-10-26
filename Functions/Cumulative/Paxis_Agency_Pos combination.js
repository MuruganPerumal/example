// Combination of Paxis booking, agency and pos from diff table to single table

db.getCollection('JUP_DB_Paxis_Booking_1Month').aggregate([
// {$match:{
//     "IATAAgentNumber" : "14355725",
// }},
{$group:{_id:'$IATAAgentNumber',
    count:{$sum:1}
}},
{
  $lookup:
    {
      from: "JUP_DB_Paxis_Agency",
      localField: "_id",
      foreignField: "Agenet_No",
      as: "Agency"
    }
},
{$project:{
    IATAAgentNumber :'$_id',
    Agency:{$arrayElemAt: [ "$Agency", 0 ]}
}},
{
  $lookup:
    {
      from: "JUP_DB_City_Pos_Mapping_Paxis",
      localField: "Agency.Lcity",
      foreignField: "City_Name",
      as: "CityObj"
    }
},
{ $unwind: { path: "$CityObj", preserveNullAndEmptyArrays: true } },
{$project:{
    _id:0,
    IATAAgentNumber :'$IATAAgentNumber',
    Agency_City:'$Agency.Lcity',
    Agency_Country:'$Agency.Lctry',
    Master_pos :'$CityObj.POS'
}},

{$out:'Temp_FZ'}
])


//db.Temp.find({}).count()