db.JUP_DB_Booking_DepDate_Brand_New.aggregate([
{$match:{
        "snap_date" : {$in:["2017-08-02","2017-08-03","2017-08-04",,"2017-08-05"]},
        "year" : 2017,
         "month" : {$in:[9]},
       // "isvoid" : {$ne:true}
}},
{$group:{
                _id:{snap_date:'$snap_date'},
                pax:{$sum:{$cond:[{$eq:['$segment_status','CANCELED']},0,{ $cond:[
                {'$eq':['$isvoid',true]},
                {$multiply:[-1,'$pax']},
                '$pax'
                ]}]}},
                revenue:{$sum:{
            $cond:[
                {'$eq':['$isvoid',true]},
                {$multiply:[-1,'$revenue']},
                '$revenue'
                ]
            }},   
}},
{$project:{
                dep_date:'$_id.dep_date',
                snap_date:'$_id.snap_date',
                pax:'$pax',
                revenue:'$revenue',
                count:'$count'
    
}},
])


// Sales/Flown

db.JUP_DB_Sales_Flown_Brand_New.aggregate([
{$match:{
        //"snap_date" : {$in:["2017-08-02","2017-08-03","2017-08-04","2017-08-05","2017-08-06","2017-08-07","2017-08-08","2017-08-09","2017-08-10"]},
        "snap_date" : {$in:["2017-08-02","2017-08-03","2017-08-04","2017-08-05"]},
        "dep_date" : {$in:["2017-08-02","2017-08-03","2017-08-04","2017-08-01"]},
//         "year" : 2017,
//         "month" : {$in:[8]},
    //    "isvoid" : {$ne:true}
}},
{$group:{
	_id:{
        snap_date:null,
        dep_date:'$dep_date'
         //   ,year:'$year',month:'$month'
            },
	pax:{$sum:{
            $cond:[
                {'$eq':['$isvoid',true]},
                {$multiply:[-1,'$pax']},
                '$pax'
                ]
            }},
	//pax1:{$sum:{$cond:[{$eq:['$segment_status','CANCELED']},0,'$pax']}},
	revenue:
   {$sum:{
            $cond:[
                {'$eq':['$isvoid',true]},
                {$multiply:[-1,'$revenue']},
                '$revenue'
                ]
            }},         
           // {$sum:'$revenue'},
}},

{$project:{
        _id:0,
	snap_date:'$_id.snap_date',
    dep_date:'$_id.dep_date',
        dep_year:'$_id.year',
	dep_month:'$_id.month',
	pax:'$pax',
	revenue:'$revenue',
//        revenue_overall:'$revenue_overall'
    
}},
{$sort:{'snap_date':1}},
//{$out:'Temp'}
])

//     
// db.Temp.find({
//     'dep_date':'2018-02-01'
//     })