// drop the existing temp collections before getting create
db.temp_doc.drop();
db.tempTBL1.drop();

var num = 1;
var bulk = db.tempTBL1.initializeOrderedBulkOp();
db.JUP_DB_ATPCO_Fares_Rules.find({'Footnotes':{$ne:null}},{_id:1,OD:1,fare:1,carrier:1,compartment:1,'Footnotes.Cat_14_FN':1}).forEach(function(x){
        bulk.insert(x)
        
        if ( num % 5000 == 0 ){
            bulk.execute();
            bulk = db.tempTBL1.initializeOrderedBulkOp();
        }
        num++;
})
bulk.execute();
    
db.tempTBL1.createIndex({'OD':1})

db.JUP_DB_Manual_Triggers_Module.aggregate([
// consider for this static date as system date because this script will run daily
// {$match:{
//     "trx_date" : "2017-04-03",
//     //"od":'CGPMCT'
// }},
{$group:{
    _id:{
            dep_date:'$dep_date',
            compartment:'$compartment.compartment',
            od:'$od'
        }    
}},
// {$project:{
// od_comp:{$concat:['$_id.od','$_id.compartment']}
// }},
{$lookup:
         {
            from: "tempTBL1",
            localField: "_id.od",
            foreignField: "OD",
            as: "ATPCO_docs"
        }
},
{$project:{
    dep_date:'$_id.dep_date',
    compartment:'$_id.compartment',
    od:'$_id.od',
    ATPCO_docs:{
            $filter: {
               input: "$ATPCO_docs",
               as: "item",
               cond: {$and:[{$eq: [ "$$item.compartment", "$_id.compartment" ]},{$ne: [ "$$item.Footnotes", null ]}]  }
            }
         }
}},
{$unwind:'$ATPCO_docs'},
{$unwind:'$ATPCO_docs.Footnotes.Cat_14_FN'},
{$project:{
    dep_date:'$dep_date',
    compartment:'$compartment',
    od:'$od',
    //carrier:'$ATPCO_docs.carrier',
    carrier:'$ATPCO_docs.carrier',
    fare:'$ATPCO_docs.fare',
    total_fare:'$ATPCO_docs.total_fare',
    start_date:{$concat:[
                            '20',
                            { $substr: [ '$ATPCO_docs.Footnotes.Cat_14_FN.TRAVEL_DATES_COMM', 1, 2 ] },'-',
                            { $substr: [ '$ATPCO_docs.Footnotes.Cat_14_FN.TRAVEL_DATES_COMM', 3, 2 ] },'-',
                            { $substr: [ '$ATPCO_docs.Footnotes.Cat_14_FN.TRAVEL_DATES_COMM', 5, 2 ] }
                            ]},
    end_date:{$concat:[
                            '20',
                            { $substr: [ '$ATPCO_docs.Footnotes.Cat_14_FN.TRAVEL_DATES_EXP', 1, 2 ] },'-',
                            { $substr: [ '$ATPCO_docs.Footnotes.Cat_14_FN.TRAVEL_DATES_EXP', 3, 2 ] },'-',
                            { $substr: [ '$ATPCO_docs.Footnotes.Cat_14_FN.TRAVEL_DATES_EXP', 5, 2 ] }
                            ]},
}},

{ $redact: {
        $cond: {
           if: { $and:[ 
                        {$gte:['$dep_date','$start_date']},
                        {$lte:['$dep_date','$end_date']}
                        //true
                ]  },
           then: "$$DESCEND",
           else: "$$PRUNE"
         }
       }
 },
 {$sort:{'dep_date':1,'od':1,'compartment':1,'carrier':1}},
 {$group:{
     _id:{ dep_date:'$dep_date',
            od:'$od',
            compartment:'$compartment',
                        carrier:'$carrier'
        },
        minVal :{$min:'$fare'},
//      combDoc :{$push:{
//          fare:'$fare',
//          carrier:'$carrier'
//      }}
 }},
 {$group:{
     _id:{ dep_date:'$_id.dep_date',
            od:'$_id.od',
            compartment:'$_id.compartment',
                        
        },
        minVal :{$min:'$minVal'},
        combDoc :{$push:{
            fare:'$minVal',
            carrier:'$_id.carrier'
        }}
 }},
{$project:{
    dep_date:'$_id.dep_date',
    od:'$_id.od',
    compartment:'$_id.compartment',
    minFare:'$minVal',
    compDoc:'$combDoc'
    ,
     hostDoc:{
        $filter: {
           input: "$combDoc",
           as: "item",
           cond: {$eq: [ "$$item.carrier", "FZ" ]}
        }
     },
}},
{$out:'temp_doc'}
 
], {
  allowDiskUse:true,
  cursor:{}
 })
 
 db.tempTBL1.drop();
 // create Index before upsert in the table
 db.JUP_DB_Manual_Triggers_Module.createIndex({od:1,'compartment.compartment':1,dep_date:1})
 
var num = 1;
var bulk1 = db.JUP_DB_Manual_Triggers_Module.initializeOrderedBulkOp();
db.temp_doc.find().forEach(function(x){
  var compartment = {};
  compartment['compartment'] = x.compartment ;
  compartment ['all'] = 'all';
  
  bulk1.find({
        'od':x.od,
        'compartment.compartment':compartment.compartment,
        dep_date:x.dep_date,
        
  }).upsert().update(
  {
        $set:{
            lowestFare:x.compDoc,
            hostDoc:x.hostDoc,
            minFare:x.minFare                            
          }
     }
    );
        
    if ( num % 100 == 0 ){
          bulk1.execute();
          bulk1 = db.JUP_DB_Manual_Triggers_Module.initializeOrderedBulkOp();
    }
              
    num++;
})
bulk1.execute();
db.temp_doc.drop();