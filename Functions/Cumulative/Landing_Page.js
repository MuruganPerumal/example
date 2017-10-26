//db.getCollection('JUP_DB_User').find({})
var _objList = db.JUP_DB_User.find({'username':'test'});
var _userName = _objList[0]['username'];
var _posList = _objList[0]['list_of_pos'];
var snap_date = new Date();
var snapMonth = snap_date.getMonth()
var _actualData = {};
var _thisMonth = {};
_actualData['user'] =_userName; 
_actualData['snap_date'] =snap_date;
_actualData['list_of_pos'] =_posList;
// Sale data
var _saleData = db.JUP_DB_Sales.aggregate([
{'$match':{
    'pos':{$in:_posList},
    'month':{$eq:snapMonth}
    }},
    {'$group':{
        _id:null,
        pax:{$sum:'$pax'},
        revenue:{$sum:'$revenue'}
        }},
        {$project:{
            _id:0,
            pax:'$pax',
            revenue:'$revenue'
            }},
            { $limit : 1 }
])
var _salePax = null;     
var _saleRevenue = null;            
_saleData.forEach(function(x){
    _thisMonth['Pax'] = x.pax;  
    _thisMonth['Revenue'] = x.revenue;  
//         _salePax = x.pax;  
//         _saleRevenue = x.revenue;
    })
 
// Forecast
var _forecastData = db.JUP_DB_Forecast_OD.aggregate([
{'$match':{
    'pos':{$in:_posList},
    'Month':{$eq:snapMonth}
    }},
    {'$group':{
        _id:null,
        pax:{$sum:'$pax'},
        revenue:{$sum:'$revenue'}
        }},
        {$project:{
            _id:0,
            pax:'$pax',
            revenue:'$revenue'
            }},
            { $limit : 1 }
])
_forecastData.forEach(function(x){
    _thisMonth['forecast_pax'] = x.pax;  
    _thisMonth['forecast_revenue'] = x.revenue;  
    })
// Target 
   var _targetData = db.JUP_DB_Target_OD.aggregate([
{'$match':{
    'pos':{$in:_posList},
    'month':{$eq:snapMonth}
    }},
    {'$group':{
        _id:null,
        pax:{$sum:'$pax'},
        revenue:{$sum:'$revenue'},
        average_fare:{$sum:'$average_fare'}
        }},
        {$project:{
            _id:0,
            pax:'$pax',
            revenue:'$revenue',
            average_fare:'$average_fare'
            }},
            { $limit : 1 }
])
            
_targetData.forEach(function(x){
    _thisMonth['target_pax'] = x.pax;  
    _thisMonth['target_revenue'] = x.revenue;  
    _thisMonth['target_average_fare'] = x.average_fare;  
    })
//print(_thisMonth);

// dep_date based on data  from sales

var _depDate = db.JUP_DB_Sales.aggregate([
    {$match:{
        'month':{$eq:5}
    }},

    { $lookup: {
            "from": "JUP_DB_OD_Distance",
            "localField": "od",
            "foreignField": "od",
            "as": "dist"
        }
    },
    {$unwind:{
            path:'$dist', preserveNullAndEmptyArrays: true
        }
    },
    
    
    {$group:{
            _id:'$dep_date',
            pax:{$sum:'$pax'},
            revenue:{$sum:'$revenue_base'},
            distance:{$sum:'$dist.distance'},
        }
    },
    {$project:{
            _id:0,
            value:'$_id',
            pax:1,
            revenue:1,
            distance:1,
            rpkm:{$multiply:['$pax','$distance']},
            avgFare:{$divide:['$revenue','$pax']}
        }
    },
    {$sort:{"value":1}}
]) 
var depDateArray = [];        
_depDate.forEach(function(x){
	var _obj = {};	 
		_obj['pax'] = x.pax;  
		_obj['revenue'] = x.revenue;  
		_obj['distance'] = x.distance;  
		_obj['value'] = x.value;  
		_obj['rpkm'] = x.rpkm;  
		_obj['avgFare'] = x.avgFare;  
	depDateArray.push(_obj);
    })
_thisMonth['dep_date'] = depDateArray;   
_actualData['thisMonth'] =_thisMonth; 










//db.getCollection('JUP_DB_User').find({})
var _objList = db.JUP_DB_User.find({'username':'test'});
var _userName = _objList[0]['username'];
var _posList = _objList[0]['list_of_pos'];

db.JUP_DB_Sales.aggregate([
{$group:{
    _id:{month:'$month',
        year:'$year'
        }
        
    }}
])


var snap_date = new Date();
var snapMonth = snap_date.getMonth()
var _actualData = {};
var _thisMonth = {};
_actualData['user'] =_userName; 
_actualData['month'] =snapMonth;
_actualData['list_of_pos'] =_posList;
// Sale data
var _saleData = db.JUP_DB_Sales.aggregate([
{'$match':{
    'pos':{$in:_posList},
    'month':{$eq:snapMonth}
    }},
    {'$group':{
        _id:null,
        pax:{$sum:'$pax'},
        revenue:{$sum:'$revenue'}
        }},
        {$project:{
            _id:0,
            pax:'$pax',
            revenue:'$revenue'
            }},
            { $limit : 1 }
])
var _salePax = null;     
var _saleRevenue = null;            
_saleData.forEach(function(x){
    _thisMonth['Pax'] = x.pax;  
    _thisMonth['Revenue'] = x.revenue;  
//         _salePax = x.pax;  
//         _saleRevenue = x.revenue;
    })
 
// Forecast
var _forecastData = db.JUP_DB_Forecast_OD.aggregate([
{'$match':{
    'pos':{$in:_posList},
    'Month':{$eq:snapMonth}
    }},
    {'$group':{
        _id:null,
        pax:{$sum:'$pax'},
        revenue:{$sum:'$revenue'}
        }},
        {$project:{
            _id:0,
            pax:'$pax',
            revenue:'$revenue'
            }},
            { $limit : 1 }
])
_forecastData.forEach(function(x){
    _thisMonth['forecast_pax'] = x.pax;  
    _thisMonth['forecast_revenue'] = x.revenue;  
    })
// Target 
   var _targetData = db.JUP_DB_Target_OD.aggregate([
{'$match':{
    'pos':{$in:_posList},
    'month':{$eq:snapMonth}
    }},
    {'$group':{
        _id:null,
        pax:{$sum:'$pax'},
        revenue:{$sum:'$revenue'},
        average_fare:{$sum:'$average_fare'}
        }},
        {$project:{
            _id:0,
            pax:'$pax',
            revenue:'$revenue',
            average_fare:'$average_fare'
            }},
            { $limit : 1 }
])
            
_targetData.forEach(function(x){
    _thisMonth['target_pax'] = x.pax;  
    _thisMonth['target_revenue'] = x.revenue;  
    _thisMonth['target_average_fare'] = x.average_fare;  
    })
//print(_thisMonth);

// dep_date based on data  from sales

var _depDate = db.JUP_DB_Sales.aggregate([
    {$match:{
        'month':{$eq:5}
    }},

    { $lookup: {
            "from": "JUP_DB_OD_Distance",
            "localField": "od",
            "foreignField": "od",
            "as": "dist"
        }
    },
    {$unwind:{
            path:'$dist', preserveNullAndEmptyArrays: true
        }
    },
    
    
    {$group:{
            _id:'$dep_date',
            pax:{$sum:'$pax'},
            revenue:{$sum:'$revenue_base'},
            distance:{$sum:'$dist.distance'},
        }
    },
    {$project:{
            _id:0,
            value:'$_id',
            pax:1,
            revenue:1,
            distance:1,
            rpkm:{$multiply:['$pax','$distance']},
            avgFare:{$divide:['$revenue','$pax']}
        }
    },
    {$sort:{"value":1}}
]) 
var depDateArray = [];        
_depDate.forEach(function(x){
	var _obj = {};	 
		_obj['pax'] = x.pax;  
		_obj['revenue'] = x.revenue;  
		_obj['distance'] = x.distance;  
		_obj['value'] = x.value;  
		_obj['rpkm'] = x.rpkm;  
		_obj['avgFare'] = x.avgFare;  
	depDateArray.push(_obj);
    })
_thisMonth['dep_date'] = depDateArray;   
_actualData['thisMonth'] =_thisMonth; 



