var org = "DXB";
var dest = "DOH";
var pos = "DXB";
var comp = "J";
db.JUP_DB_Region_Master.aggregate(
[
      {  
          $match:
            {
                $or:[
                    {
                        "POS_CD":{"$eq":org}
                    },
                ]
            }
    },
    { $unwind: { path: "$CCRN", preserveNullAndEmptyArrays: true } },
    {
        $addFields:{
                    CCRN_o:"$CCRN",
                    CCRN_d:dest
            }
        },
        
     {$project:{
         CCRN_o:"$CCRN_o",
         CCRN_d:"$CCRN_d",
         }}   ,
    { "$lookup": {
                    "from": "JUP_DB_Region_Master",
                    "localField": "CCRN_d",
                    "foreignField": "POS_CD",
                    "as": "region_master2"
                }},
    {$project:{
        CCRN_o:"$CCRN_o",
        CCRN_d:"$region_master2.CCRN",
        
        }},
    { $unwind: { path: "$CCRN_d", preserveNullAndEmptyArrays: true } },
    { $unwind: { path: "$CCRN_d", preserveNullAndEmptyArrays: true } },
   // { $unwind: { path: "$region_master2.CCRN", preserveNullAndEmptyArrays: true } },
    {
        $addFields:{
                    CCRN_pos:pos
            }
    },
    { "$lookup": {
                    "from": "JUP_DB_Region_Master",
                    "localField": "CCRN_pos",
                    "foreignField": "POS_CD",
                    "as": "region_master2"
                }},
    {$project:{
        CCRN_o:"$CCRN_o",
        CCRN_d:"$CCRN_d",
        CCRN_pos: "$region_master2.CCRN"
        }},
    { $unwind: { path: "$CCRN_pos", preserveNullAndEmptyArrays: true } },
    { $unwind: { path: "$CCRN_pos", preserveNullAndEmptyArrays: true } },
    {$project:{
        _id:0,
        CCRN_o:"$CCRN_o",
        CCRN_d:"$CCRN_d",
        CCRN_pos: "$CCRN_pos"
        }},
    {
        $addFields:{
                    CCRN_comp:comp
            }
    },            
    { "$lookup": {
                    "from": "JUP_DB_RBD_COMP_Master",
                    "localField": "CCRN_comp",
                    "foreignField": "comp",
                    "as": "comp_master"
    }},
    
    {"$addFields":{"comp_master":{$slice:["$comp_master",0,1]}}},
    //{ $unwind: { path: "$CCRN_RBD", preserveNullAndEmptyArrays: true } },
    
  //  {"$group":{
  //          _id:{CCRN_o:"$CCRN_o",CCRN_d:"$CCRN_d",CCRN_pos: "$CCRN_pos",
  //      CCRN_comp: "$CCRN_comp",CCRN_RBD:"$comp_master.comp_hrcy"},
  //              }
   //     },
    {$project:{
        CCRN_o:"$CCRN_o",
        CCRN_d:"$CCRN_d",
        CCRN_pos: "$CCRN_pos",
        CCRN_comp: "$comp_master.comp_hrcy"

        }},
    { $unwind: { path: "$CCRN_comp", preserveNullAndEmptyArrays: true } },
    { $unwind: { path: "$CCRN_comp", preserveNullAndEmptyArrays: true } },
	
    { "$lookup": {
                    "from": "JUP_DB_Competitor_Config",
                    "localField": "CCRN_pos",
                    "foreignField": "pos.value",
                    "as": "p_config"
    }},
    
  
  {$addFields:{
        p_config:{ $filter: {
     input: "$p_config",
     as: "config",
     cond: { $and: [
        { $eq: [ "$$config.origin.value", "$CCRN_o" ] },
        { $eq: [ "$$config.destination.value", "$CCRN_d" ] },
        { $eq: [ "$$config.compartment.value", "$CCRN_comp" ] }
      ] }
    }}
  
    
      }},
      
      { $unwind: { path: "$p_config" } },
    ,
      {$project:{
             CCRN_o:"$CCRN_o",
        CCRN_d:"$CCRN_d",
        CCRN_pos: "$CCRN_pos",
        CCRN_comp: "$CCRN_comp",
          priority: "$p_config.priority",
          competitors: "$p_config.competitors",
          }},
    {$sort:{priority:1}},
       {$limit:1},
    {$out:"Temp_Collection_Murugan"}
    
],allowDiskUse= true)
    
    