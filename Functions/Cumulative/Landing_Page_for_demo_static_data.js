//db.getCollection('JUP_DB_Workflow').find({})

// global variable have to chage this to run like list of user 
var user = 'Dummy';
var userDetail = db.JUP_DB_User.findOne({'name':user});
var listOfPos = userDetail.list_of_pos ;
var cursor = db.getCollection('JUP_DB_Workflow').find({'pos':{$in:listOfPos}})
var odUser = db.getCollection('JUP_DB_Workflow_OD_User').find({'pos':{$in:listOfPos}})
var _curAry = [];
var _curObj = {};
var _odUserAry = [];
var triggerOverAllFare = 0;
var triggerOverAllCritical = 0;
var _odArr = [];
var TriggerOd = {};
var TriggerType ={};
var TriggerOverall = {};
var Trigger = {};
// get system date I mean yesterday date
var curDay = new Date();
curDay.setDate(-65);

var yesterDay = new Date();
yesterDay.setDate(curDay.getDate() - 1);


/*'''''''''''''''''''''''''''''''''''''''''''''''*/
// For give a demo to vinassa in the month of march



curDay.setDate(curDay.getDate()); // <-- add this to make it "yesterday"
var weekDay = new Date();
weekDay.setDate(weekDay.getDate() - 8); // <-- add this to make it "one week before"
var monthDay = new Date();
monthDay.setDate(monthDay.getDate() - 30); // <-- add this to make it "one month back"

var yearDay = new Date(curDay.getFullYear()+'-01-01');



var curr_date = curDay.getDate(); // <-- don't subtract 1 anymore
var curr_month = ("0" + (curDay.getMonth()+1)).slice(-2);// global variable have to chage this to run like list of user 


// For testing give current month as month -1
//var curr_month = ("0" + (curDay.getMonth() )).slice(-2);

var curr_year = curDay.getFullYear();
var str_date = curr_year +"-"+ curr_month+"-"+curr_date

var triggerStatus = [];
var triggerStatusObjDay = {};
var triggerStatusObjWeek = {};
var triggerStatusObjMonth = {};
var triggerStatusObjYTD = {};
var triggerStatusObjRes = {};
var sysTriggerType = {};
var _TriggerOdSub = [];

var SQLArray = [];
var _SQLObj = {};
var _SQLDay = [];
var _SQLWeek = [];
var _SQLMonth = [];
var _SQLYTD = [];
var _SQLRes = {};


cursor.forEach(function(x){
    _curObj = x;
    if(x.recommendation_category == 'A'){
        triggerOverAllCritical++;
    }
    if(x.status.toLowerCase() == 'pending'){
    	_curAry.push(_curObj);
    	_odArr.push(x.od);
        TriggerOd[x.od] = (TriggerOd[x.od] == undefined ? 0 : TriggerOd[x.od]) +1;
        TriggerType[x.type_of_trigger] = (TriggerType[x.type_of_trigger] == undefined ? 0 : TriggerType[x.type_of_trigger]) +1;
        
        // to get system trigger's type of Trigger list of Count
        if(x.type_of_trigger.toLowerCase() == 'system'){
            
            sysTriggerType[x.trigger_type] = (sysTriggerType[x.trigger_type] == undefined ? 0 : sysTriggerType[x.trigger_type]) +1;
        }
        
        // to loading OD wise different trigger count
        _TriggerOdSub.push({ 'od':x.od,'type':x.type_of_trigger});
        //_TriggerOdSub[x.type_of_trigger] = (_TriggerOdSub[x.type_of_trigger] == undefined ? 0 : _TriggerOdSub[x.type_of_trigger]) +1;
        
        for(var i=0; i<TriggerOd.length-1; i++){
            target = target[TriggerOd[i]];
        }
        //TriggerOd[x.od] = 
    }
        
    // for one day graph
    //print(new Date(x.triggering_event_date)+" curr ");
    if(str_date == x.triggering_event_date){
        //print(new Date(x.triggering_event_date)+" curr ");
        
        triggerStatusObjDay[x.status] = (triggerStatusObjDay[x.status] == undefined ? 0 : triggerStatusObjDay[x.status]) +1;
        
    }
    // for one week graph
    if(new Date(x.triggering_event_date) >= weekDay ){
            //print(new Date(x.triggering_event_date)+"  week");
        triggerStatusObjWeek[x.status] = (triggerStatusObjWeek[x.status] == undefined ? 0 : triggerStatusObjWeek[x.status]) +1;
    }    
    // for one month graph
    if(new Date(x.triggering_event_date) >= monthDay){
            //print(new Date(x.triggering_event_date)+"  month");
        triggerStatusObjMonth[x.status] = (triggerStatusObjMonth[x.status] == undefined ? 0 : triggerStatusObjMonth[x.status]) +1;
    }
    // for one YTD graph
    if(new Date(x.triggering_event_date) >= yearDay ){
            //print(new Date(x.triggering_event_date)+"  YTD");
        triggerStatusObjYTD[x.status] = (triggerStatusObjYTD[x.status] == undefined ? 0 : triggerStatusObjYTD[x.status]) +1;
    }


    // for SQL
    
        // for one day graph
        if(str_date == x.triggering_event_date){
            //print(new Date(x.triggering_event_date)+" curr ");
            
            _SQLDay.push(x);
            
        }
        // for one week graph
        if(new Date(x.triggering_event_date) >= weekDay ){
                //print(new Date(x.triggering_event_date)+"  week");
            _SQLWeek.push(x);
        }    
        // for one month graph
        if(new Date(x.triggering_event_date) >= monthDay){
                //print(new Date(x.triggering_event_date)+"  month");
            _SQLMonth.push(x);
        }
        // for one YTD graph
        if(new Date(x.triggering_event_date) >= yearDay ){
               // print(new Date(x.triggering_event_date)+"  YTD");
            _SQLYTD.push(x);
        }

})
triggerStatusObjRes['Day'] = triggerStatusObjDay;
triggerStatusObjRes['Week'] = triggerStatusObjWeek;
triggerStatusObjRes['Month'] = triggerStatusObjMonth;
triggerStatusObjRes['YTD'] = triggerStatusObjYTD;

// unique OD's
var triggerOverAllOd = Array.from(new Set(_odArr));


var od = '';

function _uniquGroupArray(array){
    var od = '';
    var type = '';
    var result = {};
    var opt = [];
    var i=0;
    var arr_Length = array.length;
    array.forEach(function(x){
         if(od != x.od ){
            if(i==0){
                result[x.type] = x.value;    
                opt = [];
            }else{
                //print(result);
                opt.push(result);
                result = {}
                result[x.type] = x.value;    
                //result = {}
            }
         }
         else{
             if(type == x.type){
                result[x.type] += x.value; 
             }
             else{
                 result[x.type] = x.value; 
             }
                
         }
       i++;
        
        result['od'] = x.od;
        
        od = x.od;
        type = x.type;
        if(i==arr_Length){
            //print(result);
            opt.push(result);
        }   
         
    });
    return opt;
}

// get a unique od and trigger type count 
function _uniqueODTrigger(array){
    var od = '';
    var type = '';
    var result = {};
    var opt = [];
    var i=0;
     var arr_Length = array.length;
    array.forEach(function(x){
        if(od != x.od || type != x.type){
            
            if(i==0){
                result['value'] = 0;    
                opt = [];
            }else{
                //print(result);
                opt.push({
                    od:result['od'],
                    type:result['type'],
                    value:result['value'],
                    });
                result['value'] = 0;    
            }
            
            
        }
        i++;
        //print('outer');
        result['value'] += 1;
        result['od'] = x.od;
        result['type'] = x.type;
        od = x.od;
        type = x.type;
        if(i==arr_Length){
            //print(result);
            opt.push({
                     od:result['od'],
                    type:result['type'],
                    value:result['value'],
            });
            
        }
    });
    return opt;
}
// compare and arrange unique array list
function compare(a,b) {
  if (a.od < b.od)
    return -1;
  if (a.od > b.od)
    return 1;
  return 0;
}

// sort dep_date array based on departure date
function compare_depdate(a,b) {
  if (a.dep_date < b.dep_date)
    return -1;
  if (a.dep_date > b.dep_date)
    return 1;
  return 0;
}



odUser.forEach(function(x){
    _odUserAry.push(x);
    if(typeof x.fares_docs == 'object'){
           triggerOverAllFare+= x.fares_docs.length;
    }
})
var triggerOverAllTotal = _curAry.length;   

// To add work package also in the trigger type collection
TriggerType['work_package'] = db.JUP_DB_WorkPackage.find({user:user,filingstatus : 'pending'}).count();

TriggerOverall['Critical'] = triggerOverAllCritical;
TriggerOverall['Total'] = triggerOverAllTotal;
TriggerOverall['Fares'] = triggerOverAllFare;
TriggerOverall['od'] = triggerOverAllOd.length;
Trigger['overAll'] = TriggerOverall;
TriggerOd['detail'] = _uniquGroupArray(_uniqueODTrigger(_TriggerOdSub).sort(compare));
Trigger['triggerOd'] = TriggerOd;
TriggerType['detail'] = sysTriggerType;
Trigger['triggerTypes'] = TriggerType;
Trigger['triggerStatus'] = triggerStatusObjRes;



function _SQLGetElement(data,index){
    var num = 0;
    var hour = 0;
    for(num=0;num<index;num++){
        if(data[num].status.toLowerCase() == 'pending'){
            hour = 0;
        }
        else{
            if(data[num].action_date != null && data[num].action_time != null ){
                var testDate = new Date(data[num].action_date+'T'+data[num].action_time+':00Z');
                var testDate1 = new Date(data[num].triggering_event_date+'T'+data[num].triggering_event_time+':00Z');
                hour += Math.abs(testDate1-testDate) / 36e5;        
            }
            else{
                
            }
            
        }
    }
    return hour;
}


//_SQLGetElement()

function _SQLAlg(data_){
    var SQLArray = {};
    var lenOfArray = data_.length;
    var tenVal = Math.ceil(lenOfArray*.1);
    var tfVal = Math.ceil(lenOfArray*.25);
    var fiVal = Math.ceil(lenOfArray*.5);
    var sfVal = Math.ceil(lenOfArray*.75);
    var hnVal = Math.ceil(lenOfArray*1);
    SQLArray['10p'] = _SQLGetElement(data_,tenVal);
    SQLArray['25p'] = _SQLGetElement(data_,tfVal);
    SQLArray['50p'] = _SQLGetElement(data_,fiVal);
    SQLArray['75p'] = _SQLGetElement(data_,sfVal);
    SQLArray['100p'] = _SQLGetElement(data_,hnVal);
    var num = 0;
    var penCount = 0;
    for(num = 0;num<data_.length;num++){
        if(data_[num].status=='pending'){
           penCount++; 
        }
    }
    SQLArray['pending'] = penCount;
    return SQLArray;
}

// _SQLAlg(_SQLYTD)

// SQL code continue

    // for making order put pending triggers last in the array
function _orderPending(data){
    var tempArr1 = [];
    var tempArr2 = [];
    data.forEach(function(x){
        if(x.status == 'pending'){
            tempArr2.push(x);
        }
        else{
            tempArr1.push(x);
        }
    });
    var tempOrderedArray = [];
    if(tempArr1 == undefined){
        tempOrderedArray = tempArr1;
    }
    else{
        tempOrderedArray = tempArr1.concat(tempArr2);
    }
    
    return _SQLAlg(tempOrderedArray);
}



_SQLObj['Day'] = _orderPending(_SQLDay);
_SQLObj['Week'] = _orderPending(_SQLWeek);
_SQLObj['Month'] = _orderPending(_SQLMonth);
_SQLObj['Year'] = _orderPending(_SQLYTD);

// For KPI Grid
var _KPIObj = {};
var _KPIObjSubDoc = {};
var _saleRev = 0;
var _salePax = 0;
var _saleRev_1 = 0;
var _salePax_1 = 0;
var _capacity_1 = 0;
var _capacity = 0;
var _distance = 0;
var _onlyFlownPax = 0;
var _onlyFlownRev = 0;
var _monthTargetPax = 0;
var _monthTargetRev = 0;
var _monthForecastPax = 0;
var _monthForecastRev = 0;
// count for to check the number of days flown data come under the month
var count = 0;
// for agility we need competitor price change trigger. So till trigger is getting ready we keep agility as 0
var _agility = 0;
var _MarketShare = 0;
var _MarketShare_1 = 0;
// Ask Sid or Mahesh is that Except pending trigger or all the trigger of current month
var _ReviewOD = 0;

var data = db.JUP_DB_Landing_Page_.findOne({'user':user,'dep_month':Number(curr_month),'dep_year':Number(curr_year)})

var _updateDepDateData = [];

// For updating Yield and avg fare in the dep_date array based on the date
data.this_month.dep_date.forEach(function(x){
    if(new Date(x.dep_date) >= yesterDay){
        x['avgFare'] = x.sale_pax == 0 ? 0 : x.sale_revenue/x.sale_pax ;
        x['yield'] = x.sale_pax*x.distance == 0 ? 0 :x.sale_revenue/(x.sale_pax*x.distance) ;
    }
    else if(new Date(x.dep_date) < yesterDay){
        x['avgFare'] = x.flown_pax == 0 ? 0 : x.flown_revenue/x.flown_pax;
        x['yield'] = x.flown_pax*x.distance == 0 ? 0 : x.flown_revenue/(x.flown_pax*x.distance);
    }
    _updateDepDateData.push(x);
})

//=========================================================
// To update monthly target and forecast from actual collection to landing page based on the user profile

//db.getCollection('JUP_DB_Target_OD').find({'od':'DXBDOH','pos':'DXB','month':7})

var _target_pax_from_original = 0;
var _target_rev_from_original = 0;
var _target_avg_from_original = 0;
var _forecast_pax_from_original = 0;
var _forecast_rev_from_original = 0;
var _forecast_avg_from_original = 0;
var _target_pax_from_original_1 = 0;
var _target_rev_from_original_1 = 0;
var _target_avg_from_original_1 = 0;
var _forecast_pax_from_original_1 = 0;
var _forecast_rev_from_original_1 = 0;
var _forecast_avg_from_original_1 = 0;


var _target_val = db.JUP_DB_Target_OD.aggregate([
{$match:{
	//'dep_month':Number(curr_month),'dep_year':Number(curr_year)
    'month':{$eq:Number(curr_month)},
    'year':{$eq:Number(curr_year)},
    'pos':{$in:['AMM','CGP','CMB']},
    //'compartment':{$eq:'J'}
    }},
    {$group:{
        _id:'$month',
        rev : {$sum:'$revenue'},
        pax : {$sum:'$pax'},
        rev_1 : {$sum:'$revenue_1'},
        pax_1 : {$sum:'$pax_1'},
        }},
        {$project:{
            _id:1,
            rev:1,
            pax:1,
            avg:{$divide:['$rev','$pax']},
            rev_1:1,
            pax_1:1,
            avg_1:{$divide:['$rev_1','$pax_1']},
        }}
     
])
        
        
_target_val.forEach(function(x){
    _target_pax_from_original = x.pax;
    _target_rev_from_original = x.rev;
    _target_avg_from_original = x.avg;
    _target_pax_from_original_1 = x.pax_1;
    _target_rev_from_original_1 = x.rev_1;
    _target_avg_from_original_1 = x.avg_1;
})    	


var _forecast_val = db.JUP_DB_Forecast_OD.aggregate([
{$match:{
	//'dep_month':Number(curr_month),'dep_year':Number(curr_year)
    'Month':{$eq:curr_month},
    'Year':{$eq:curr_year+''},
    'pos':{$in:['AMM','CGP','CMB']},
    //'compartment':{$eq:'J'}
    }},
    {$group:{
        _id:'$Month',
	        rev : {$sum:'$revenue'},
	        pax : {$sum:'$pax'},
	        rev_1 : {$sum:'$revenue_1'},
	        pax_1 : {$sum:'$pax_1'},
        }},
        {$project:{
            _id:1,
            rev:1,
            pax:1,
            avg:{$divide:['$rev','$pax']},
            rev_1:1,
            pax_1:1,
            avg_1:{$divide:['$rev_1','$pax_1']}
        }}
])
        
        
_forecast_val.forEach(function(x){
	    _forecast_pax_from_original = x.pax;
	    _forecast_rev_from_original = x.rev;
	    _forecast_avg_from_original = x.avg;
	    _forecast_pax_from_original_1 = x.pax_1;
	    _forecast_rev_from_original_1 = x.rev_1;
	    _forecast_avg_from_original_1 = x.avg_1;
    })    


var _market_pax_from_original = 0;
var _market_rev_from_original = 0;
var _market_size_from_original = 0;
var _market_pax_from_original_1 = 0;
var _market_rev_from_original_1 = 0;
var _market_size_from_original_1 = 0;

var _marketShareObj = db.JUP_DB_Market_Share.aggregate([
{$match:{
	//'dep_month':Number(curr_month),'dep_year':Number(curr_year)
   'month':{$eq:Number(curr_month)},
    'year':{$eq:Number(curr_year)},
    'pos':{$in:['AMM','CGP','CMB']},
    //'compartment':{$eq:'J'}
    }},
    {$group:{
        _id:'$month',
	        rev : {$sum:'$revenue'},
	        pax : {$sum:'$pax'},
	        rev_1 : {$sum:'$revenue_1'},
	        pax_1 : {$sum:'$pax_1'},
                market_size:{$sum:'$market_size'},
                market_size_1:{$sum:'$market_size_1'}
        }},
        {$project:{
            _id:1,
            rev:1,
            pax:1,
            market_size:1,
            rev_1:1,
            pax_1:1,
            market_size_1:1
        }}
])

_marketShareObj.forEach(function(x){
        _market_pax_from_original = x.pax;
        _market_rev_from_original = x.rev;
        _market_size_from_original = x.market_size;
        _market_pax_from_original_1 = x.pax_1;
        _market_rev_from_original_1 = x.rev_1;
        _market_size_from_original_1 = x.market_size_1;    
    })  

//=========================================================



/*

==================================================

calculate pax and revenue based on present and past dates ( sales & flown )
*/
data.this_month.dep_date.forEach(function(x){
    if(new Date(x.dep_date) >= yesterDay){
        print(_salePax+"    "+_salePax_1);
        _salePax += x.sale_pax;
        _saleRev += x.sale_revenue;
        _salePax_1 += x.sale_pax_1;
        _saleRev_1 += x.sale_revenue_1;
    }
    else if(new Date(x.dep_date) < yesterDay){
        print(_salePax+"    "+_salePax_1);
        _salePax += x.flown_pax;
        _saleRev += x.flown_revenue;
        _salePax_1 += x.flown_pax_1;
        _saleRev_1 += x.flown_revenue_1;
        _onlyFlownPax += x.flown_pax;
        _onlyFlownRev += x.flown_revenue;
	count += 1; 
    }
})
//==================================================

_capacity = data.this_month.capacity;
_capacity_1 = data.this_month.capacity_1;
_distance = data.this_month.distance;
_MarketShare = _market_pax_from_original/(_market_size_from_original*100);
_MarketShare_1 = _market_pax_from_original_1/((_market_size_from_original_1==0?1:_market_size_from_original_1)*100);
var _thisMonthFromLanding = data.this_month;

_monthTargetPax = _target_pax_from_original;
_monthTargetRev = _target_rev_from_original;
_monthForecastPax = _forecast_pax_from_original;
_monthForecastRev = _forecast_rev_from_original;

_MarketShare = _market_size_from_original == 0 ? 0 : (_market_pax_from_original/_market_size_from_original)*100;
var _Target = db.JUP_DB_Analyst_Target.findOne({'user':user});
var _TargetRevenue = _Target.kpi.revenue;
var _TargetPax = _Target.kpi.ticket;
var _TargetAgility = _Target.kpi.agility;
var _TargetFMS = _Target.kpi.ms_growth;
var _TargetRevOfTopOD = _Target.kpi.review_od;
var _TargetCurrency = _Target.kpi.revenue_unit;
var _agility_unit = _Target.kpi.agility_unit;
var ticket_unit = _Target.kpi.ticket_unit;
var ms_growth_unit = _Target.kpi.ms_growth_unit ;
var review_od_unit = _Target.kpi.review_od_unit;


// Tiles are start from here
var _tiles = {};
var _Fares = {};

// fares
db.JUP_DB_Manual_Triggers_Module.aggregate([
{$match:{
    'dep_year':{ $eq: Number(curr_year) },
    'dep_month':{ $eq: Number(curr_month) }, 
    'pos.City':{ $in: listOfPos }
}},
{$unwind:'$sale_farebasis'},
{
  $group:{
      _id:{
            dep_year:'$dep_year',
             dep_month:'$dep_month', 
             fare_basis:'$sale_farebasis.fare_basis'
          },
          flagValue:{$sum:'$sale_farebasis.effectivity_calc'}
      }  
},
{$project:{
        dep_year:'$_id.dep_year',
         dep_month:'$_id.dep_month', 
         fare_basis:'$_id.fare_basis',
         flagValue:'$flagValue'
}},
{
  $group:{
      _id:{
            dep_year:'$dep_year',
             dep_month:'$dep_month', 
//             fare_basis:'$sale_farebasis.fare_basis'
          },
          totalFare:{$sum:1},
          effectiveFare:{$sum:{$cond:[
              {$gte:['$flagValue',0]},
              1,
              0
              ]}},
          inEffectiveFare:{$sum:{$cond:[
              {$lt:['$flagValue',0]},
              1,
              0
              ]}}
      }  
},
{$project:{
        dep_year:'$_id.dep_year',
         dep_month:'$_id.dep_month', 
         fare_basis:'$_id.fare_basis',
         totalFare:'$totalFare',
         effectiveFare:'$effectiveFare',
        inEffectiveFare:'$inEffectiveFare'
}},
{$out:'temp_Eff_Fare'}
])

var _resObj = db.temp_Eff_Fare.findOne()
db.temp_Eff_Fare.drop();
_Fares['totalFare'] = _resObj.totalFare;
_Fares['effectiveFare'] = _resObj.effectiveFare;
_Fares['inEffectiveFare'] = _resObj.inEffectiveFare;
_tiles['Fares'] = _Fares;
// significant and none significant OD
var totArray = [];
var sigArray = [];
var NoneSigArray = [];
_thisMonthFromLanding.dep_date.forEach(function(opendata){
    totArray = totArray.concat(opendata.totalOD);
    sigArray = sigArray.concat(opendata.signOD);
    NoneSigArray = NoneSigArray.concat(opendata.nonSignOD);
})

// function for removing null element from array
Array.prototype.remove = function() {
    var what, a = arguments, L = a.length, ax;
    while (L && this.length) {
        what = a[--L];
        while ((ax = this.indexOf(what)) !== -1) {
            this.splice(ax, 1);



        }
    }
    return this;
};

// get unique od from all the arrays 
totArray = Array.from(new Set(totArray)).remove(null);
sigArray = Array.from(new Set(sigArray)).remove(null);
NoneSigArray = Array.from(new Set(NoneSigArray)).remove(null);

// for creating and assingning of all unique sig and non sig count and array into the landing page collection
var _signOD = {};
_signOD['totalOD'] = totArray; 
_signOD['sigOD'] = sigArray;
_signOD['noneSigOD'] = NoneSigArray;
_signOD['totalOD_count'] = totArray.length; 
_signOD['sigOD_count'] = sigArray.length;
_signOD['noneSigOD_count'] = NoneSigArray.length;

// add sign embedded document into tiles doc
_tiles['Sig'] = _signOD;

// function for Yield formula 
function yield_(revenue,pax,distance){
   if(distance == 0 || pax == 0) {
     return 0;
   }
   else{
     return (revenue/(pax*distance))*100;
   }
    
}

// function for VLYR formula 
function vlyr(pax,pax_,capacity,capacity_){
     if(capacity!= 0 && capacity_!= 0){
        return ((pax*(capacity/capacity_))-pax_)/pax_;
    }
    else if(pax!= 0 && pax_!= 0){
        return ((pax-pax_)/pax_);
    }
    else{
        return 0;
    }
}


//_salePax_1 = data.this_month.sale_pax_1; 
_capacity_1 = data.this_month.capacity_1;

// if capacity doesn't have any data so give a same value by default
if (_capacity_1 == undefined || _capacity_1 == 0 ){
     _capacity_1 = _capacity;
    }

// to compute the vtgt and vlyr    
var _paxVLYR = vlyr(_salePax, _salePax_1, _capacity, _capacity_1)*100;
var _revVLYR = vlyr(_saleRev, _saleRev_1, _capacity, _capacity_1)*100;
var _yieldVLYR = (yield_(_saleRev,_salePax, _distance)-yield_(_saleRev_1,_salePax_1, _distance))/(yield_(_saleRev_1,_salePax_1, _distance)==0?1:yield_(_saleRev_1,_salePax_1, _distance))*100;
var _avgVLYR = (_saleRev ==0 &&_salePax ==0 )?0: ((_saleRev/_salePax)-(_saleRev_1/_salePax_1))/(_saleRev_1/_salePax_1)*100;
var _MarketShareVLYR = _MarketShare - _MarketShare_1;

//=====================VTGT=================================
// get number of days in month
function daysInMonth(month,year) {
    return new Date(year, month, 0).getDate();
}

function VTGT_pureFuture(_monthTargetPax, _monthForecastPax){
    return((_monthForecastPax) - _monthTargetPax) / (_monthTargetPax)
}    

function VTGT_purePast(_monthTargetPax, _monthFlownPax){
    return(_monthFlownPax - _monthTargetPax) / (_monthTargetPax)
}    

function VTGT_mix(_monthTargetPax_, _onlyFlownPax_, _monthForecastPax_, count_ ){
    if(_monthTargetPax_!=0){
        return ((_onlyFlownPax_ + (_monthForecastPax_/daysInMonth(curr_month,curr_year))*(daysInMonth(curr_month,curr_year)-count_) -_monthTargetPax) / _monthTargetPax);    
    }
    else{
        return 0;    
    }
}        

// 

//==========================================================    

//_onlyFlownPax
// Get logic from ashok and write logic for VTGT    
var _paxVTGT = 0;
var _revVTGT = 0;
var _yieldVTGT = 0;
var _avgVTGT = 0;
var _MarketShareVTGT = 0; 

// check which function has to choose for running VTGT based on date
if(new Date().getMonth()+1 == Number(curr_month)){
    _paxVTGT = VTGT_mix(_monthTargetPax,_onlyFlownPax,_monthForecastPax,count)*100;   
    _revVTGT = VTGT_mix(_monthTargetRev ,_onlyFlownRev,_monthForecastRev,count)*100;   
    _yieldVTGT = _revVTGT/(_paxVTGT*_distance)*100;   
    _avgVTGT = _revVTGT/_paxVTGT;
}
else if(new Date().getMonth()+1 > Number(curr_month)){
    
    _paxVTGT = VTGT_pureFuture(_monthTargetPax,_monthForecastPax)*100;   
    _revVTGT = VTGT_pureFuture(_monthTargetRev ,_monthForecastRev)*100;   
    _yieldVTGT = _revVTGT/(_paxVTGT*_distance)*100;   
    _avgVTGT = _revVTGT/_paxVTGT;
}
else if(new Date().getMonth()+1 < Number(curr_month)){
    //_monthTargetPax, _monthFlownPax
    _paxVTGT = VTGT_purePast(_monthTargetPax,_onlyFlownPax)*100;   
    _revVTGT =VTGT_purePast(_monthTargetRev,_onlyFlownRev)*100;   
    _yieldVTGT = _revVTGT/(_paxVTGT*_distance)*100;   
    _avgVTGT = _revVTGT/_paxVTGT;
}

var paxt = {};
var revt = {};
var yieldt = {};
var avgt = {};
var markett = {};

// vtgt and vlyr assigning embedded documents
paxt['vlyr'] = _paxVLYR;
paxt['vtgt'] = _paxVTGT;
paxt['value'] = _salePax;
paxt['value_1'] = _salePax_1;
revt['vlyr'] = _revVLYR;
revt['value'] = _saleRev;
revt['value_1'] = _saleRev_1;
yieldt['vlyr'] = _yieldVLYR;
yieldt['value'] = yield_(_saleRev,_salePax, _distance);
avgt['vlyr'] = _avgVLYR;
avgt['value'] = (_saleRev ==0 &&_salePax ==0 )?0: _saleRev/_salePax ;
markett['vlyr'] = _MarketShareVLYR;
markett['value'] = _MarketShare;
revt['vtgt'] = _revVTGT;
yieldt['vtgt'] = _yieldVTGT ;
avgt['vtgt'] = _avgVTGT;
markett['vtgt'] = _MarketShareVTGT;

// for review of top od from current month actioned triggers
var __totalTrigger = _SQLMonth.length;
var __percentageOfComp = 0;
var __processedTriggerWithinOneDay = 0;
_SQLMonth.forEach(function(x){
    
//     if(new Date(x.triggering_event_date) == new Date(x.action_date)){
//         __processedTriggerWithinOneDay +=1
//     }
    
    // check what are all Trigger processed in the same day 
    // just check two date's As per Mahesh and Sid's concept[05-07-2017]
    if(x.triggering_event_date == x.action_date){
        __processedTriggerWithinOneDay +=1
    }
})

// percentage of completion trigger with in one day
if(__totalTrigger!=0){
    __percentageOfComp = (__processedTriggerWithinOneDay/__totalTrigger)*100;
}
// assigning KPI value  
_KPIObj['Revenue'] ={'current':_saleRev,'target':_monthTargetRev} ;
_KPIObj['Forward_Booking'] ={'current':_paxVLYR,'target':_TargetPax,'unit':ticket_unit} ;
_KPIObj['Market_Share_Growth'] ={'current':_MarketShareVLYR,'target':_TargetFMS,'unit':ms_growth_unit} ;
_KPIObj['Agility'] ={'current':_agility,'target':_TargetAgility, 'unit':_agility_unit } ;
_KPIObj['Review_of_Top_OD'] ={'current':__percentageOfComp,'target':_TargetRevOfTopOD,'unit':review_od_unit} ;


// add all vtgt and vlyr into tiles sub document 
_tiles['pax'] = paxt;
_tiles['revenue'] = revt;
_tiles['market'] = markett;
_tiles['yield'] = yieldt;
_tiles['avgFare'] = avgt;

// insert the whole data 
db.db.JUP_DB_Landing_Page_.find({'user':user,'dep_month':Number(curr_month),'dep_year':Number(curr_year)}).forEach(function(x){
    db.db.JUP_DB_Landing_Page_26.insert(x);
})

// Update landing table
db.JUP_DB_Landing_Page_26.update({
        'user':user,'dep_month':Number(curr_month),'dep_year':Number(curr_year)
    },
    {
        $set:{
        	'currency':_TargetCurrency,
            'this_month.dep_date':_updateDepDateData.sort(compare_depdate),
            'this_month.target_pax':_target_pax_from_original ,
            'this_month.target_avgFare':_target_avg_from_original,
            'this_month.target_revenue':_target_rev_from_original,
            'this_month.target_pax_1':_target_pax_from_original_1,
            'this_month.target_avgFare_1':_target_avg_from_original_1,
            'this_month.target_revenue_1':_target_rev_from_original_1,
            'this_month.forecast_pax':_forecast_pax_from_original,
            'this_month.forecast_avgFare':_forecast_avg_from_original,
            'this_month.forecast_revenue':_forecast_rev_from_original,
            'this_month.forecast_pax_1':_forecast_pax_from_original_1,
            'this_month.forecast_avgFare_1':_forecast_rev_from_original_1 ,
            'this_month.forecast_revenue_1':_forecast_avg_from_original_1,
            'this_month.market.pax':_market_pax_from_original,
            'this_month.market.pax_1':_market_pax_from_original_1,
            'this_month.market.market_size':_market_size_from_original,
            'this_month.market.market_size_1':_market_size_from_original_1,
            Trigger : Trigger,
            SQL : _SQLObj,
            KPI : _KPIObj,
            Tiles : _tiles
            }
    },
    {
    	upsert:true
	});


//    for 27

// global variable have to chage this to run like list of user 
var user = 'Dummy';
var userDetail = db.JUP_DB_User.findOne({'name':user});
var listOfPos = userDetail.list_of_pos ;
var cursor = db.getCollection('JUP_DB_Workflow').find({'pos':{$in:listOfPos}})
var odUser = db.getCollection('JUP_DB_Workflow_OD_User').find({'pos':{$in:listOfPos}})
var _curAry = [];
var _curObj = {};
var _odUserAry = [];
var triggerOverAllFare = 0;
var triggerOverAllCritical = 0;
var _odArr = [];
var TriggerOd = {};
var TriggerType ={};
var TriggerOverall = {};
var Trigger = {};
// get system date I mean yesterday date
var curDay = new Date();
curDay.setDate(-64);

var yesterDay = new Date();
yesterDay.setDate(curDay.getDate() - 1);



/*'''''''''''''''''''''''''''''''''''''''''''''''*/
// For give a demo to vinassa in the month of march



curDay.setDate(curDay.getDate()); // <-- add this to make it "yesterday"
var weekDay = new Date();
weekDay.setDate(weekDay.getDate() - 8); // <-- add this to make it "one week before"
var monthDay = new Date();
monthDay.setDate(monthDay.getDate() - 30); // <-- add this to make it "one month back"

var yearDay = new Date(curDay.getFullYear()+'-01-01');



var curr_date = curDay.getDate(); // <-- don't subtract 1 anymore
var curr_month = ("0" + (curDay.getMonth()+1)).slice(-2);// global variable have to chage this to run like list of user 


// For testing give current month as month -1
//var curr_month = ("0" + (curDay.getMonth() )).slice(-2);

var curr_year = curDay.getFullYear();
var str_date = curr_year +"-"+ curr_month+"-"+curr_date

var triggerStatus = [];
var triggerStatusObjDay = {};
var triggerStatusObjWeek = {};
var triggerStatusObjMonth = {};
var triggerStatusObjYTD = {};
var triggerStatusObjRes = {};
var sysTriggerType = {};
var _TriggerOdSub = [];

var SQLArray = [];
var _SQLObj = {};
var _SQLDay = [];
var _SQLWeek = [];
var _SQLMonth = [];
var _SQLYTD = [];
var _SQLRes = {};


cursor.forEach(function(x){
    _curObj = x;
    if(x.recommendation_category == 'A'){
        triggerOverAllCritical++;
    }
    if(x.status.toLowerCase() == 'pending'){
        _curAry.push(_curObj);
        _odArr.push(x.od);
        TriggerOd[x.od] = (TriggerOd[x.od] == undefined ? 0 : TriggerOd[x.od]) +1;
        TriggerType[x.type_of_trigger] = (TriggerType[x.type_of_trigger] == undefined ? 0 : TriggerType[x.type_of_trigger]) +1;
        
        // to get system trigger's type of Trigger list of Count
        if(x.type_of_trigger.toLowerCase() == 'system'){
            
            sysTriggerType[x.trigger_type] = (sysTriggerType[x.trigger_type] == undefined ? 0 : sysTriggerType[x.trigger_type]) +1;
        }
        
        // to loading OD wise different trigger count
        _TriggerOdSub.push({ 'od':x.od,'type':x.type_of_trigger});
        //_TriggerOdSub[x.type_of_trigger] = (_TriggerOdSub[x.type_of_trigger] == undefined ? 0 : _TriggerOdSub[x.type_of_trigger]) +1;
        
        for(var i=0; i<TriggerOd.length-1; i++){
            target = target[TriggerOd[i]];
        }
        //TriggerOd[x.od] = 
    }
        
    // for one day graph
    //print(new Date(x.triggering_event_date)+" curr ");
    if(str_date == x.triggering_event_date){
        //print(new Date(x.triggering_event_date)+" curr ");
        
        triggerStatusObjDay[x.status] = (triggerStatusObjDay[x.status] == undefined ? 0 : triggerStatusObjDay[x.status]) +1;
        
    }
    // for one week graph
    if(new Date(x.triggering_event_date) >= weekDay ){
            //print(new Date(x.triggering_event_date)+"  week");
        triggerStatusObjWeek[x.status] = (triggerStatusObjWeek[x.status] == undefined ? 0 : triggerStatusObjWeek[x.status]) +1;
    }    
    // for one month graph
    if(new Date(x.triggering_event_date) >= monthDay){
            //print(new Date(x.triggering_event_date)+"  month");
        triggerStatusObjMonth[x.status] = (triggerStatusObjMonth[x.status] == undefined ? 0 : triggerStatusObjMonth[x.status]) +1;
    }
    // for one YTD graph
    if(new Date(x.triggering_event_date) >= yearDay ){
            //print(new Date(x.triggering_event_date)+"  YTD");
        triggerStatusObjYTD[x.status] = (triggerStatusObjYTD[x.status] == undefined ? 0 : triggerStatusObjYTD[x.status]) +1;
    }


    // for SQL
    
        // for one day graph
        if(str_date == x.triggering_event_date){
            //print(new Date(x.triggering_event_date)+" curr ");
            
            _SQLDay.push(x);
            
        }
        // for one week graph
        if(new Date(x.triggering_event_date) >= weekDay ){
                //print(new Date(x.triggering_event_date)+"  week");
            _SQLWeek.push(x);
        }    
        // for one month graph
        if(new Date(x.triggering_event_date) >= monthDay){
                //print(new Date(x.triggering_event_date)+"  month");
            _SQLMonth.push(x);
        }
        // for one YTD graph
        if(new Date(x.triggering_event_date) >= yearDay ){
               // print(new Date(x.triggering_event_date)+"  YTD");
            _SQLYTD.push(x);
        }

})
triggerStatusObjRes['Day'] = triggerStatusObjDay;
triggerStatusObjRes['Week'] = triggerStatusObjWeek;
triggerStatusObjRes['Month'] = triggerStatusObjMonth;
triggerStatusObjRes['YTD'] = triggerStatusObjYTD;

// unique OD's
var triggerOverAllOd = Array.from(new Set(_odArr));


var od = '';

function _uniquGroupArray(array){
    var od = '';
    var type = '';
    var result = {};
    var opt = [];
    var i=0;
    var arr_Length = array.length;
    array.forEach(function(x){
         if(od != x.od ){
            if(i==0){
                result[x.type] = x.value;    
                opt = [];
            }else{
                //print(result);
                opt.push(result);
                result = {}
                result[x.type] = x.value;    
                //result = {}
            }
         }
         else{
             if(type == x.type){
                result[x.type] += x.value; 
             }
             else{
                 result[x.type] = x.value; 
             }
                
         }
       i++;
        
        result['od'] = x.od;
        
        od = x.od;
        type = x.type;
        if(i==arr_Length){
            //print(result);
            opt.push(result);
        }   
         
    });
    return opt;
}

// get a unique od and trigger type count 
function _uniqueODTrigger(array){
    var od = '';
    var type = '';
    var result = {};
    var opt = [];
    var i=0;
     var arr_Length = array.length;
    array.forEach(function(x){
        if(od != x.od || type != x.type){
            
            if(i==0){
                result['value'] = 0;    
                opt = [];
            }else{
                //print(result);
                opt.push({
                    od:result['od'],
                    type:result['type'],
                    value:result['value'],
                    });
                result['value'] = 0;    
            }
            
            
        }
        i++;
        //print('outer');
        result['value'] += 1;
        result['od'] = x.od;
        result['type'] = x.type;
        od = x.od;
        type = x.type;
        if(i==arr_Length){
            //print(result);
            opt.push({
                     od:result['od'],
                    type:result['type'],
                    value:result['value'],
            });
            
        }
    });
    return opt;
}
// compare and arrange unique array list
function compare(a,b) {
  if (a.od < b.od)
    return -1;
  if (a.od > b.od)
    return 1;
  return 0;
}

// sort dep_date array based on departure date
function compare_depdate(a,b) {
  if (a.dep_date < b.dep_date)
    return -1;
  if (a.dep_date > b.dep_date)
    return 1;
  return 0;
}



odUser.forEach(function(x){
    _odUserAry.push(x);
    if(typeof x.fares_docs == 'object'){
           triggerOverAllFare+= x.fares_docs.length;
    }
})
var triggerOverAllTotal = _curAry.length;   

// To add work package also in the trigger type collection
TriggerType['work_package'] = db.JUP_DB_WorkPackage.find({user:user,filingstatus : 'pending'}).count();

TriggerOverall['Critical'] = triggerOverAllCritical;
TriggerOverall['Total'] = triggerOverAllTotal;
TriggerOverall['Fares'] = triggerOverAllFare;
TriggerOverall['od'] = triggerOverAllOd.length;
Trigger['overAll'] = TriggerOverall;
TriggerOd['detail'] = _uniquGroupArray(_uniqueODTrigger(_TriggerOdSub).sort(compare));
Trigger['triggerOd'] = TriggerOd;
TriggerType['detail'] = sysTriggerType;
Trigger['triggerTypes'] = TriggerType;
Trigger['triggerStatus'] = triggerStatusObjRes;



function _SQLGetElement(data,index){
    var num = 0;
    var hour = 0;
    for(num=0;num<index;num++){
        if(data[num].status.toLowerCase() == 'pending'){
            hour = 0;
        }
        else{
            if(data[num].action_date != null && data[num].action_time != null ){
                var testDate = new Date(data[num].action_date+'T'+data[num].action_time+':00Z');
                var testDate1 = new Date(data[num].triggering_event_date+'T'+data[num].triggering_event_time+':00Z');
                hour += Math.abs(testDate1-testDate) / 36e5;        
            }
            else{
                
            }
            
        }
    }
    return hour;
}


//_SQLGetElement()

function _SQLAlg(data_){
    var SQLArray = {};
    var lenOfArray = data_.length;
    var tenVal = Math.ceil(lenOfArray*.1);
    var tfVal = Math.ceil(lenOfArray*.25);
    var fiVal = Math.ceil(lenOfArray*.5);
    var sfVal = Math.ceil(lenOfArray*.75);
    var hnVal = Math.ceil(lenOfArray*1);
    SQLArray['10p'] = _SQLGetElement(data_,tenVal);
    SQLArray['25p'] = _SQLGetElement(data_,tfVal);
    SQLArray['50p'] = _SQLGetElement(data_,fiVal);
    SQLArray['75p'] = _SQLGetElement(data_,sfVal);
    SQLArray['100p'] = _SQLGetElement(data_,hnVal);
    var num = 0;
    var penCount = 0;
    for(num = 0;num<data_.length;num++){
        if(data_[num].status=='pending'){
           penCount++; 
        }
    }
    SQLArray['pending'] = penCount;
    return SQLArray;
}

// _SQLAlg(_SQLYTD)

// SQL code continue

    // for making order put pending triggers last in the array
function _orderPending(data){
    var tempArr1 = [];
    var tempArr2 = [];
    data.forEach(function(x){
        if(x.status == 'pending'){
            tempArr2.push(x);
        }
        else{
            tempArr1.push(x);
        }
    });
    var tempOrderedArray = [];
    if(tempArr1 == undefined){
        tempOrderedArray = tempArr1;
    }
    else{
        tempOrderedArray = tempArr1.concat(tempArr2);
    }
    
    return _SQLAlg(tempOrderedArray);
}



_SQLObj['Day'] = _orderPending(_SQLDay);
_SQLObj['Week'] = _orderPending(_SQLWeek);
_SQLObj['Month'] = _orderPending(_SQLMonth);
_SQLObj['Year'] = _orderPending(_SQLYTD);

// For KPI Grid
var _KPIObj = {};
var _KPIObjSubDoc = {};
var _saleRev = 0;
var _salePax = 0;
var _saleRev_1 = 0;
var _salePax_1 = 0;
var _capacity_1 = 0;
var _capacity = 0;
var _distance = 0;
var _onlyFlownPax = 0;
var _onlyFlownRev = 0;
var _monthTargetPax = 0;
var _monthTargetRev = 0;
var _monthForecastPax = 0;
var _monthForecastRev = 0;
// count for to check the number of days flown data come under the month
var count = 0;
// for agility we need competitor price change trigger. So till trigger is getting ready we keep agility as 0
var _agility = 0;
var _MarketShare = 0;
var _MarketShare_1 = 0;
// Ask Sid or Mahesh is that Except pending trigger or all the trigger of current month
var _ReviewOD = 0;

var data = db.JUP_DB_Landing_Page_.findOne({'user':user,'dep_month':Number(curr_month),'dep_year':Number(curr_year)})

var _updateDepDateData = [];

// For updating Yield and avg fare in the dep_date array based on the date
data.this_month.dep_date.forEach(function(x){
    if(new Date(x.dep_date) >= yesterDay){
        x['avgFare'] = x.sale_pax == 0 ? 0 : x.sale_revenue/x.sale_pax ;
        x['yield'] = x.sale_pax*x.distance == 0 ? 0 :x.sale_revenue/(x.sale_pax*x.distance) ;
    }
    else if(new Date(x.dep_date) < yesterDay){
        x['avgFare'] = x.flown_pax == 0 ? 0 : x.flown_revenue/x.flown_pax;
        x['yield'] = x.flown_pax*x.distance == 0 ? 0 : x.flown_revenue/(x.flown_pax*x.distance);
    }
    _updateDepDateData.push(x);
})

//=========================================================
// To update monthly target and forecast from actual collection to landing page based on the user profile

//db.getCollection('JUP_DB_Target_OD').find({'od':'DXBDOH','pos':'DXB','month':7})

var _target_pax_from_original = 0;
var _target_rev_from_original = 0;
var _target_avg_from_original = 0;
var _forecast_pax_from_original = 0;
var _forecast_rev_from_original = 0;
var _forecast_avg_from_original = 0;
var _target_pax_from_original_1 = 0;
var _target_rev_from_original_1 = 0;
var _target_avg_from_original_1 = 0;
var _forecast_pax_from_original_1 = 0;
var _forecast_rev_from_original_1 = 0;
var _forecast_avg_from_original_1 = 0;


var _target_val = db.JUP_DB_Target_OD.aggregate([
{$match:{
    //'dep_month':Number(curr_month),'dep_year':Number(curr_year)
    'month':{$eq:Number(curr_month)},
    'year':{$eq:Number(curr_year)},
    'pos':{$in:['AMM','CGP','CMB']},
    //'compartment':{$eq:'J'}
    }},
    {$group:{
        _id:'$month',
        rev : {$sum:'$revenue'},
        pax : {$sum:'$pax'},
        rev_1 : {$sum:'$revenue_1'},
        pax_1 : {$sum:'$pax_1'},
        }},
        {$project:{
            _id:1,
            rev:1,
            pax:1,
            avg:{$divide:['$rev','$pax']},
            rev_1:1,
            pax_1:1,
            avg_1:{$divide:['$rev_1','$pax_1']},
        }}
     
])
        
        
_target_val.forEach(function(x){
    _target_pax_from_original = x.pax;
    _target_rev_from_original = x.rev;
    _target_avg_from_original = x.avg;
    _target_pax_from_original_1 = x.pax_1;
    _target_rev_from_original_1 = x.rev_1;
    _target_avg_from_original_1 = x.avg_1;
})      


var _forecast_val = db.JUP_DB_Forecast_OD.aggregate([
{$match:{
    //'dep_month':Number(curr_month),'dep_year':Number(curr_year)
    'Month':{$eq:curr_month},
    'Year':{$eq:curr_year+''},
    'pos':{$in:['AMM','CGP','CMB']},
    //'compartment':{$eq:'J'}
    }},
    {$group:{
        _id:'$Month',
            rev : {$sum:'$revenue'},
            pax : {$sum:'$pax'},
            rev_1 : {$sum:'$revenue_1'},
            pax_1 : {$sum:'$pax_1'},
        }},
        {$project:{
            _id:1,
            rev:1,
            pax:1,
            avg:{$divide:['$rev','$pax']},
            rev_1:1,
            pax_1:1,
            avg_1:{$divide:['$rev_1','$pax_1']}
        }}
])
        
        
_forecast_val.forEach(function(x){
        _forecast_pax_from_original = x.pax;
        _forecast_rev_from_original = x.rev;
        _forecast_avg_from_original = x.avg;
        _forecast_pax_from_original_1 = x.pax_1;
        _forecast_rev_from_original_1 = x.rev_1;
        _forecast_avg_from_original_1 = x.avg_1;
    })    


var _market_pax_from_original = 0;
var _market_rev_from_original = 0;
var _market_size_from_original = 0;
var _market_pax_from_original_1 = 0;
var _market_rev_from_original_1 = 0;
var _market_size_from_original_1 = 0;

var _marketShareObj = db.JUP_DB_Market_Share.aggregate([
{$match:{
    //'dep_month':Number(curr_month),'dep_year':Number(curr_year)
   'month':{$eq:Number(curr_month)},
    'year':{$eq:Number(curr_year)},
    'pos':{$in:['AMM','CGP','CMB']},
    //'compartment':{$eq:'J'}
    }},
    {$group:{
        _id:'$month',
            rev : {$sum:'$revenue'},
            pax : {$sum:'$pax'},
            rev_1 : {$sum:'$revenue_1'},
            pax_1 : {$sum:'$pax_1'},
                market_size:{$sum:'$market_size'},
                market_size_1:{$sum:'$market_size_1'}
        }},
        {$project:{
            _id:1,
            rev:1,
            pax:1,
            market_size:1,
            rev_1:1,
            pax_1:1,
            market_size_1:1
        }}
])

_marketShareObj.forEach(function(x){
        _market_pax_from_original = x.pax;
        _market_rev_from_original = x.rev;
        _market_size_from_original = x.market_size;
        _market_pax_from_original_1 = x.pax_1;
        _market_rev_from_original_1 = x.rev_1;
        _market_size_from_original_1 = x.market_size_1;    
    })  

//=========================================================



/*

==================================================

calculate pax and revenue based on present and past dates ( sales & flown )
*/
data.this_month.dep_date.forEach(function(x){
    if(new Date(x.dep_date) >= yesterDay){
        print(_salePax+"    "+_salePax_1);
        _salePax += x.sale_pax;
        _saleRev += x.sale_revenue;
        _salePax_1 += x.sale_pax_1;
        _saleRev_1 += x.sale_revenue_1;
    }
    else if(new Date(x.dep_date) < yesterDay){
        print(_salePax+"    "+_salePax_1);
        _salePax += x.flown_pax;
        _saleRev += x.flown_revenue;
        _salePax_1 += x.flown_pax_1;
        _saleRev_1 += x.flown_revenue_1;
        _onlyFlownPax += x.flown_pax;
        _onlyFlownRev += x.flown_revenue;
    count += 1; 
    }
})
//==================================================

_capacity = data.this_month.capacity;
_capacity_1 = data.this_month.capacity_1;
_distance = data.this_month.distance;
_MarketShare = _market_pax_from_original/(_market_size_from_original*100);
_MarketShare_1 = _market_pax_from_original_1/((_market_size_from_original_1==0?1:_market_size_from_original_1)*100);
var _thisMonthFromLanding = data.this_month;

_monthTargetPax = _target_pax_from_original;
_monthTargetRev = _target_rev_from_original;
_monthForecastPax = _forecast_pax_from_original;
_monthForecastRev = _forecast_rev_from_original;

_MarketShare = _market_size_from_original == 0 ? 0 : (_market_pax_from_original/_market_size_from_original)*100;
var _Target = db.JUP_DB_Analyst_Target.findOne({'user':user});
var _TargetRevenue = _Target.kpi.revenue;
var _TargetPax = _Target.kpi.ticket;
var _TargetAgility = _Target.kpi.agility;
var _TargetFMS = _Target.kpi.ms_growth;
var _TargetRevOfTopOD = _Target.kpi.review_od;
var _TargetCurrency = _Target.kpi.revenue_unit;
var _agility_unit = _Target.kpi.agility_unit;
var ticket_unit = _Target.kpi.ticket_unit;
var ms_growth_unit = _Target.kpi.ms_growth_unit ;
var review_od_unit = _Target.kpi.review_od_unit;


// Tiles are start from here
var _tiles = {};
var _Fares = {};

// fares
db.JUP_DB_Manual_Triggers_Module.aggregate([
{$match:{
    'dep_year':{ $eq: Number(curr_year) },
    'dep_month':{ $eq: Number(curr_month) }, 
    'pos.City':{ $in: listOfPos }
}},
{$unwind:'$sale_farebasis'},
{
  $group:{
      _id:{
            dep_year:'$dep_year',
             dep_month:'$dep_month', 
             fare_basis:'$sale_farebasis.fare_basis'
          },
          flagValue:{$sum:'$sale_farebasis.effectivity_calc'}
      }  
},
{$project:{
        dep_year:'$_id.dep_year',
         dep_month:'$_id.dep_month', 
         fare_basis:'$_id.fare_basis',
         flagValue:'$flagValue'
}},
{
  $group:{
      _id:{
            dep_year:'$dep_year',
             dep_month:'$dep_month', 
//             fare_basis:'$sale_farebasis.fare_basis'
          },
          totalFare:{$sum:1},
          effectiveFare:{$sum:{$cond:[
              {$gte:['$flagValue',0]},
              1,
              0
              ]}},
          inEffectiveFare:{$sum:{$cond:[
              {$lt:['$flagValue',0]},
              1,
              0
              ]}}
      }  
},
{$project:{
        dep_year:'$_id.dep_year',
         dep_month:'$_id.dep_month', 
         fare_basis:'$_id.fare_basis',
         totalFare:'$totalFare',
         effectiveFare:'$effectiveFare',
        inEffectiveFare:'$inEffectiveFare'
}},
{$out:'temp_Eff_Fare'}
])

var _resObj = db.temp_Eff_Fare.findOne()
db.temp_Eff_Fare.drop();
_Fares['totalFare'] = _resObj.totalFare;
_Fares['effectiveFare'] = _resObj.effectiveFare;
_Fares['inEffectiveFare'] = _resObj.inEffectiveFare;
_tiles['Fares'] = _Fares;
// significant and none significant OD
var totArray = [];
var sigArray = [];
var NoneSigArray = [];
_thisMonthFromLanding.dep_date.forEach(function(opendata){
    totArray = totArray.concat(opendata.totalOD);
    sigArray = sigArray.concat(opendata.signOD);
    NoneSigArray = NoneSigArray.concat(opendata.nonSignOD);
})

// function for removing null element from array
Array.prototype.remove = function() {
    var what, a = arguments, L = a.length, ax;
    while (L && this.length) {
        what = a[--L];
        while ((ax = this.indexOf(what)) !== -1) {
            this.splice(ax, 1);



        }
    }
    return this;
};

// get unique od from all the arrays 
totArray = Array.from(new Set(totArray)).remove(null);
sigArray = Array.from(new Set(sigArray)).remove(null);
NoneSigArray = Array.from(new Set(NoneSigArray)).remove(null);

// for creating and assingning of all unique sig and non sig count and array into the landing page collection
var _signOD = {};
_signOD['totalOD'] = totArray; 
_signOD['sigOD'] = sigArray;
_signOD['noneSigOD'] = NoneSigArray;
_signOD['totalOD_count'] = totArray.length; 
_signOD['sigOD_count'] = sigArray.length;
_signOD['noneSigOD_count'] = NoneSigArray.length;

// add sign embedded document into tiles doc
_tiles['Sig'] = _signOD;

// function for Yield formula 
function yield_(revenue,pax,distance){
   if(distance == 0 || pax == 0) {
     return 0;
   }
   else{
     return (revenue/(pax*distance))*100;
   }
    
}

// function for VLYR formula 
function vlyr(pax,pax_,capacity,capacity_){
     if(capacity!= 0 && capacity_!= 0){
        return ((pax*(capacity/capacity_))-pax_)/pax_;
    }
    else if(pax!= 0 && pax_!= 0){
        return ((pax-pax_)/pax_);
    }
    else{
        return 0;
    }
}


//_salePax_1 = data.this_month.sale_pax_1; 
_capacity_1 = data.this_month.capacity_1;

// if capacity doesn't have any data so give a same value by default
if (_capacity_1 == undefined || _capacity_1 == 0 ){
     _capacity_1 = _capacity;
    }

// to compute the vtgt and vlyr    
var _paxVLYR = vlyr(_salePax, _salePax_1, _capacity, _capacity_1)*100;
var _revVLYR = vlyr(_saleRev, _saleRev_1, _capacity, _capacity_1)*100;
var _yieldVLYR = (yield_(_saleRev,_salePax, _distance)-yield_(_saleRev_1,_salePax_1, _distance))/(yield_(_saleRev_1,_salePax_1, _distance)==0?1:yield_(_saleRev_1,_salePax_1, _distance))*100;
var _avgVLYR = (_saleRev ==0 &&_salePax ==0 )?0: ((_saleRev/_salePax)-(_saleRev_1/_salePax_1))/(_saleRev_1/_salePax_1)*100;
var _MarketShareVLYR = _MarketShare - _MarketShare_1;

//=====================VTGT=================================
// get number of days in month
function daysInMonth(month,year) {
    return new Date(year, month, 0).getDate();
}

function VTGT_pureFuture(_monthTargetPax, _monthForecastPax){
    return((_monthForecastPax) - _monthTargetPax) / (_monthTargetPax)
}    

function VTGT_purePast(_monthTargetPax, _monthFlownPax){
    return(_monthFlownPax - _monthTargetPax) / (_monthTargetPax)
}    

function VTGT_mix(_monthTargetPax_, _onlyFlownPax_, _monthForecastPax_, count_ ){
    if(_monthTargetPax_!=0){
        return ((_onlyFlownPax_ + (_monthForecastPax_/daysInMonth(curr_month,curr_year))*(daysInMonth(curr_month,curr_year)-count_) -_monthTargetPax) / _monthTargetPax);    
    }
    else{
        return 0;    
    }
}        

// 

//==========================================================    

//_onlyFlownPax
// Get logic from ashok and write logic for VTGT    
var _paxVTGT = 0;
var _revVTGT = 0;
var _yieldVTGT = 0;
var _avgVTGT = 0;
var _MarketShareVTGT = 0; 

// check which function has to choose for running VTGT based on date
if(new Date().getMonth()+1 == Number(curr_month)){
    _paxVTGT = VTGT_mix(_monthTargetPax,_onlyFlownPax,_monthForecastPax,count)*100;   
    _revVTGT = VTGT_mix(_monthTargetRev ,_onlyFlownRev,_monthForecastRev,count)*100;   
    _yieldVTGT = _revVTGT/(_paxVTGT*_distance)*100;   
    _avgVTGT = _revVTGT/_paxVTGT;
}
else if(new Date().getMonth()+1 > Number(curr_month)){
    
    _paxVTGT = VTGT_pureFuture(_monthTargetPax,_monthForecastPax)*100;   
    _revVTGT = VTGT_pureFuture(_monthTargetRev ,_monthForecastRev)*100;   
    _yieldVTGT = _revVTGT/(_paxVTGT*_distance)*100;   
    _avgVTGT = _revVTGT/_paxVTGT;
}
else if(new Date().getMonth()+1 < Number(curr_month)){
    //_monthTargetPax, _monthFlownPax
    _paxVTGT = VTGT_purePast(_monthTargetPax,_onlyFlownPax)*100;   
    _revVTGT =VTGT_purePast(_monthTargetRev,_onlyFlownRev)*100;   
    _yieldVTGT = _revVTGT/(_paxVTGT*_distance)*100;   
    _avgVTGT = _revVTGT/_paxVTGT;
}

var paxt = {};
var revt = {};
var yieldt = {};
var avgt = {};
var markett = {};

// vtgt and vlyr assigning embedded documents
paxt['vlyr'] = _paxVLYR;
paxt['vtgt'] = _paxVTGT;
paxt['value'] = _salePax;
paxt['value_1'] = _salePax_1;
revt['vlyr'] = _revVLYR;
revt['value'] = _saleRev;
revt['value_1'] = _saleRev_1;
yieldt['vlyr'] = _yieldVLYR;
yieldt['value'] = yield_(_saleRev,_salePax, _distance);
avgt['vlyr'] = _avgVLYR;
avgt['value'] = (_saleRev ==0 &&_salePax ==0 )?0: _saleRev/_salePax ;
markett['vlyr'] = _MarketShareVLYR;
markett['value'] = _MarketShare;
revt['vtgt'] = _revVTGT;
yieldt['vtgt'] = _yieldVTGT ;
avgt['vtgt'] = _avgVTGT;
markett['vtgt'] = _MarketShareVTGT;

// for review of top od from current month actioned triggers
var __totalTrigger = _SQLMonth.length;
var __percentageOfComp = 0;
var __processedTriggerWithinOneDay = 0;
_SQLMonth.forEach(function(x){
    
//     if(new Date(x.triggering_event_date) == new Date(x.action_date)){
//         __processedTriggerWithinOneDay +=1
//     }
    
    // check what are all Trigger processed in the same day 
    // just check two date's As per Mahesh and Sid's concept[05-07-2017]
    if(x.triggering_event_date == x.action_date){
        __processedTriggerWithinOneDay +=1
    }
})

// percentage of completion trigger with in one day
if(__totalTrigger!=0){
    __percentageOfComp = (__processedTriggerWithinOneDay/__totalTrigger)*100;
}
// assigning KPI value  
_KPIObj['Revenue'] ={'current':_saleRev,'target':_monthTargetRev} ;
_KPIObj['Forward_Booking'] ={'current':_paxVLYR,'target':_TargetPax,'unit':ticket_unit} ;
_KPIObj['Market_Share_Growth'] ={'current':_MarketShareVLYR,'target':_TargetFMS,'unit':ms_growth_unit} ;
_KPIObj['Agility'] ={'current':_agility,'target':_TargetAgility, 'unit':_agility_unit } ;
_KPIObj['Review_of_Top_OD'] ={'current':__percentageOfComp,'target':_TargetRevOfTopOD,'unit':review_od_unit} ;


// add all vtgt and vlyr into tiles sub document 
_tiles['pax'] = paxt;
_tiles['revenue'] = revt;
_tiles['market'] = markett;
_tiles['yield'] = yieldt;
_tiles['avgFare'] = avgt;

// insert the whole data 
db.db.JUP_DB_Landing_Page_.find({'user':user,'dep_month':Number(curr_month),'dep_year':Number(curr_year)}).forEach(function(x){
    db.db.JUP_DB_Landing_Page_27.insert(x);
})

// Update landing table
db.JUP_DB_Landing_Page_27.update({
        'user':user,'dep_month':Number(curr_month),'dep_year':Number(curr_year)
    },
    {
        $set:{
            'currency':_TargetCurrency,
            'this_month.dep_date':_updateDepDateData.sort(compare_depdate),
            'this_month.target_pax':_target_pax_from_original ,
            'this_month.target_avgFare':_target_avg_from_original,
            'this_month.target_revenue':_target_rev_from_original,
            'this_month.target_pax_1':_target_pax_from_original_1,
            'this_month.target_avgFare_1':_target_avg_from_original_1,
            'this_month.target_revenue_1':_target_rev_from_original_1,
            'this_month.forecast_pax':_forecast_pax_from_original,
            'this_month.forecast_avgFare':_forecast_avg_from_original,
            'this_month.forecast_revenue':_forecast_rev_from_original,
            'this_month.forecast_pax_1':_forecast_pax_from_original_1,
            'this_month.forecast_avgFare_1':_forecast_rev_from_original_1 ,
            'this_month.forecast_revenue_1':_forecast_avg_from_original_1,
            'this_month.market.pax':_market_pax_from_original,
            'this_month.market.pax_1':_market_pax_from_original_1,
            'this_month.market.market_size':_market_size_from_original,
            'this_month.market.market_size_1':_market_size_from_original_1,
            Trigger : Trigger,
            SQL : _SQLObj,
            KPI : _KPIObj,
            Tiles : _tiles
            }
    },
    {
        upsert:true
    });



























// global variable have to chage this to run like list of user 
var user = 'Dummy';
var userDetail = db.JUP_DB_User.findOne({'name':user});
var listOfPos = userDetail.list_of_pos ;
var cursor = db.getCollection('JUP_DB_Workflow').find({'pos':{$in:listOfPos}})
var odUser = db.getCollection('JUP_DB_Workflow_OD_User').find({'pos':{$in:listOfPos}})
var _curAry = [];
var _curObj = {};
var _odUserAry = [];
var triggerOverAllFare = 0;
var triggerOverAllCritical = 0;
var _odArr = [];
var TriggerOd = {};
var TriggerType ={};
var TriggerOverall = {};
var Trigger = {};
// get system date I mean yesterday date
var curDay = new Date();
curDay.setDate(-63);
var yesterDay = new Date();
yesterDay.setDate(curDay.getDate() - 1);


/*'''''''''''''''''''''''''''''''''''''''''''''''*/
// For give a demo to vinassa in the month of march



curDay.setDate(curDay.getDate()); // <-- add this to make it "yesterday"
var weekDay = new Date();
weekDay.setDate(weekDay.getDate() - 8); // <-- add this to make it "one week before"
var monthDay = new Date();
monthDay.setDate(monthDay.getDate() - 30); // <-- add this to make it "one month back"

var yearDay = new Date(curDay.getFullYear()+'-01-01');



var curr_date = curDay.getDate(); // <-- don't subtract 1 anymore
var curr_month = ("0" + (curDay.getMonth()+1)).slice(-2);// global variable have to chage this to run like list of user 


// For testing give current month as month -1
//var curr_month = ("0" + (curDay.getMonth() )).slice(-2);

var curr_year = curDay.getFullYear();
var str_date = curr_year +"-"+ curr_month+"-"+curr_date

var triggerStatus = [];
var triggerStatusObjDay = {};
var triggerStatusObjWeek = {};
var triggerStatusObjMonth = {};
var triggerStatusObjYTD = {};
var triggerStatusObjRes = {};
var sysTriggerType = {};
var _TriggerOdSub = [];

var SQLArray = [];
var _SQLObj = {};
var _SQLDay = [];
var _SQLWeek = [];
var _SQLMonth = [];
var _SQLYTD = [];
var _SQLRes = {};


cursor.forEach(function(x){
    _curObj = x;
    if(x.recommendation_category == 'A'){
        triggerOverAllCritical++;
    }
    if(x.status.toLowerCase() == 'pending'){
        _curAry.push(_curObj);
        _odArr.push(x.od);
        TriggerOd[x.od] = (TriggerOd[x.od] == undefined ? 0 : TriggerOd[x.od]) +1;
        TriggerType[x.type_of_trigger] = (TriggerType[x.type_of_trigger] == undefined ? 0 : TriggerType[x.type_of_trigger]) +1;
        
        // to get system trigger's type of Trigger list of Count
        if(x.type_of_trigger.toLowerCase() == 'system'){
            
            sysTriggerType[x.trigger_type] = (sysTriggerType[x.trigger_type] == undefined ? 0 : sysTriggerType[x.trigger_type]) +1;
        }
        
        // to loading OD wise different trigger count
        _TriggerOdSub.push({ 'od':x.od,'type':x.type_of_trigger});
        //_TriggerOdSub[x.type_of_trigger] = (_TriggerOdSub[x.type_of_trigger] == undefined ? 0 : _TriggerOdSub[x.type_of_trigger]) +1;
        
        for(var i=0; i<TriggerOd.length-1; i++){
            target = target[TriggerOd[i]];
        }
        //TriggerOd[x.od] = 
    }
        
    // for one day graph
    //print(new Date(x.triggering_event_date)+" curr ");
    if(str_date == x.triggering_event_date){
        //print(new Date(x.triggering_event_date)+" curr ");
        
        triggerStatusObjDay[x.status] = (triggerStatusObjDay[x.status] == undefined ? 0 : triggerStatusObjDay[x.status]) +1;
        
    }
    // for one week graph
    if(new Date(x.triggering_event_date) >= weekDay ){
            //print(new Date(x.triggering_event_date)+"  week");
        triggerStatusObjWeek[x.status] = (triggerStatusObjWeek[x.status] == undefined ? 0 : triggerStatusObjWeek[x.status]) +1;
    }    
    // for one month graph
    if(new Date(x.triggering_event_date) >= monthDay){
            //print(new Date(x.triggering_event_date)+"  month");
        triggerStatusObjMonth[x.status] = (triggerStatusObjMonth[x.status] == undefined ? 0 : triggerStatusObjMonth[x.status]) +1;
    }
    // for one YTD graph
    if(new Date(x.triggering_event_date) >= yearDay ){
            //print(new Date(x.triggering_event_date)+"  YTD");
        triggerStatusObjYTD[x.status] = (triggerStatusObjYTD[x.status] == undefined ? 0 : triggerStatusObjYTD[x.status]) +1;
    }


    // for SQL
    
        // for one day graph
        if(str_date == x.triggering_event_date){
            //print(new Date(x.triggering_event_date)+" curr ");
            
            _SQLDay.push(x);
            
        }
        // for one week graph
        if(new Date(x.triggering_event_date) >= weekDay ){
                //print(new Date(x.triggering_event_date)+"  week");
            _SQLWeek.push(x);
        }    
        // for one month graph
        if(new Date(x.triggering_event_date) >= monthDay){
                //print(new Date(x.triggering_event_date)+"  month");
            _SQLMonth.push(x);
        }
        // for one YTD graph
        if(new Date(x.triggering_event_date) >= yearDay ){
               // print(new Date(x.triggering_event_date)+"  YTD");
            _SQLYTD.push(x);
        }

})
triggerStatusObjRes['Day'] = triggerStatusObjDay;
triggerStatusObjRes['Week'] = triggerStatusObjWeek;
triggerStatusObjRes['Month'] = triggerStatusObjMonth;
triggerStatusObjRes['YTD'] = triggerStatusObjYTD;

// unique OD's
var triggerOverAllOd = Array.from(new Set(_odArr));


var od = '';

function _uniquGroupArray(array){
    var od = '';
    var type = '';
    var result = {};
    var opt = [];
    var i=0;
    var arr_Length = array.length;
    array.forEach(function(x){
         if(od != x.od ){
            if(i==0){
                result[x.type] = x.value;    
                opt = [];
            }else{
                //print(result);
                opt.push(result);
                result = {}
                result[x.type] = x.value;    
                //result = {}
            }
         }
         else{
             if(type == x.type){
                result[x.type] += x.value; 
             }
             else{
                 result[x.type] = x.value; 
             }
                
         }
       i++;
        
        result['od'] = x.od;
        
        od = x.od;
        type = x.type;
        if(i==arr_Length){
            //print(result);
            opt.push(result);
        }   
         
    });
    return opt;
}

// get a unique od and trigger type count 
function _uniqueODTrigger(array){
    var od = '';
    var type = '';
    var result = {};
    var opt = [];
    var i=0;
     var arr_Length = array.length;
    array.forEach(function(x){
        if(od != x.od || type != x.type){
            
            if(i==0){
                result['value'] = 0;    
                opt = [];
            }else{
                //print(result);
                opt.push({
                    od:result['od'],
                    type:result['type'],
                    value:result['value'],
                    });
                result['value'] = 0;    
            }
            
            
        }
        i++;
        //print('outer');
        result['value'] += 1;
        result['od'] = x.od;
        result['type'] = x.type;
        od = x.od;
        type = x.type;
        if(i==arr_Length){
            //print(result);
            opt.push({
                     od:result['od'],
                    type:result['type'],
                    value:result['value'],
            });
            
        }
    });
    return opt;
}
// compare and arrange unique array list
function compare(a,b) {
  if (a.od < b.od)
    return -1;
  if (a.od > b.od)
    return 1;
  return 0;
}

// sort dep_date array based on departure date
function compare_depdate(a,b) {
  if (a.dep_date < b.dep_date)
    return -1;
  if (a.dep_date > b.dep_date)
    return 1;
  return 0;
}



odUser.forEach(function(x){
    _odUserAry.push(x);
    if(typeof x.fares_docs == 'object'){
           triggerOverAllFare+= x.fares_docs.length;
    }
})
var triggerOverAllTotal = _curAry.length;   

// To add work package also in the trigger type collection
TriggerType['work_package'] = db.JUP_DB_WorkPackage.find({user:user,filingstatus : 'pending'}).count();

TriggerOverall['Critical'] = triggerOverAllCritical;
TriggerOverall['Total'] = triggerOverAllTotal;
TriggerOverall['Fares'] = triggerOverAllFare;
TriggerOverall['od'] = triggerOverAllOd.length;
Trigger['overAll'] = TriggerOverall;
TriggerOd['detail'] = _uniquGroupArray(_uniqueODTrigger(_TriggerOdSub).sort(compare));
Trigger['triggerOd'] = TriggerOd;
TriggerType['detail'] = sysTriggerType;
Trigger['triggerTypes'] = TriggerType;
Trigger['triggerStatus'] = triggerStatusObjRes;



function _SQLGetElement(data,index){
    var num = 0;
    var hour = 0;
    for(num=0;num<index;num++){
        if(data[num].status.toLowerCase() == 'pending'){
            hour = 0;
        }
        else{
            if(data[num].action_date != null && data[num].action_time != null ){
                var testDate = new Date(data[num].action_date+'T'+data[num].action_time+':00Z');
                var testDate1 = new Date(data[num].triggering_event_date+'T'+data[num].triggering_event_time+':00Z');
                hour += Math.abs(testDate1-testDate) / 36e5;        
            }
            else{
                
            }
            
        }
    }
    return hour;
}


//_SQLGetElement()

function _SQLAlg(data_){
    var SQLArray = {};
    var lenOfArray = data_.length;
    var tenVal = Math.ceil(lenOfArray*.1);
    var tfVal = Math.ceil(lenOfArray*.25);
    var fiVal = Math.ceil(lenOfArray*.5);
    var sfVal = Math.ceil(lenOfArray*.75);
    var hnVal = Math.ceil(lenOfArray*1);
    SQLArray['10p'] = _SQLGetElement(data_,tenVal);
    SQLArray['25p'] = _SQLGetElement(data_,tfVal);
    SQLArray['50p'] = _SQLGetElement(data_,fiVal);
    SQLArray['75p'] = _SQLGetElement(data_,sfVal);
    SQLArray['100p'] = _SQLGetElement(data_,hnVal);
    var num = 0;
    var penCount = 0;
    for(num = 0;num<data_.length;num++){
        if(data_[num].status=='pending'){
           penCount++; 
        }
    }
    SQLArray['pending'] = penCount;
    return SQLArray;
}

// _SQLAlg(_SQLYTD)

// SQL code continue

    // for making order put pending triggers last in the array
function _orderPending(data){
    var tempArr1 = [];
    var tempArr2 = [];
    data.forEach(function(x){
        if(x.status == 'pending'){
            tempArr2.push(x);
        }
        else{
            tempArr1.push(x);
        }
    });
    var tempOrderedArray = [];
    if(tempArr1 == undefined){
        tempOrderedArray = tempArr1;
    }
    else{
        tempOrderedArray = tempArr1.concat(tempArr2);
    }
    
    return _SQLAlg(tempOrderedArray);
}



_SQLObj['Day'] = _orderPending(_SQLDay);
_SQLObj['Week'] = _orderPending(_SQLWeek);
_SQLObj['Month'] = _orderPending(_SQLMonth);
_SQLObj['Year'] = _orderPending(_SQLYTD);

// For KPI Grid
var _KPIObj = {};
var _KPIObjSubDoc = {};
var _saleRev = 0;
var _salePax = 0;
var _saleRev_1 = 0;
var _salePax_1 = 0;
var _capacity_1 = 0;
var _capacity = 0;
var _distance = 0;
var _onlyFlownPax = 0;
var _onlyFlownRev = 0;
var _monthTargetPax = 0;
var _monthTargetRev = 0;
var _monthForecastPax = 0;
var _monthForecastRev = 0;
// count for to check the number of days flown data come under the month
var count = 0;
// for agility we need competitor price change trigger. So till trigger is getting ready we keep agility as 0
var _agility = 0;
var _MarketShare = 0;
var _MarketShare_1 = 0;
// Ask Sid or Mahesh is that Except pending trigger or all the trigger of current month
var _ReviewOD = 0;

var data = db.JUP_DB_Landing_Page_.findOne({'user':user,'dep_month':Number(curr_month),'dep_year':Number(curr_year)})

var _updateDepDateData = [];

// For updating Yield and avg fare in the dep_date array based on the date
data.this_month.dep_date.forEach(function(x){
    if(new Date(x.dep_date) >= yesterDay){
        x['avgFare'] = x.sale_pax == 0 ? 0 : x.sale_revenue/x.sale_pax ;
        x['yield'] = x.sale_pax*x.distance == 0 ? 0 :x.sale_revenue/(x.sale_pax*x.distance) ;
    }
    else if(new Date(x.dep_date) < yesterDay){
        x['avgFare'] = x.flown_pax == 0 ? 0 : x.flown_revenue/x.flown_pax;
        x['yield'] = x.flown_pax*x.distance == 0 ? 0 : x.flown_revenue/(x.flown_pax*x.distance);
    }
    _updateDepDateData.push(x);
})

//=========================================================
// To update monthly target and forecast from actual collection to landing page based on the user profile

//db.getCollection('JUP_DB_Target_OD').find({'od':'DXBDOH','pos':'DXB','month':7})

var _target_pax_from_original = 0;
var _target_rev_from_original = 0;
var _target_avg_from_original = 0;
var _forecast_pax_from_original = 0;
var _forecast_rev_from_original = 0;
var _forecast_avg_from_original = 0;
var _target_pax_from_original_1 = 0;
var _target_rev_from_original_1 = 0;
var _target_avg_from_original_1 = 0;
var _forecast_pax_from_original_1 = 0;
var _forecast_rev_from_original_1 = 0;
var _forecast_avg_from_original_1 = 0;


var _target_val = db.JUP_DB_Target_OD.aggregate([
{$match:{
    //'dep_month':Number(curr_month),'dep_year':Number(curr_year)
    'month':{$eq:Number(curr_month)},
    'year':{$eq:Number(curr_year)},
    'pos':{$in:['AMM','CGP','CMB']},
    //'compartment':{$eq:'J'}
    }},
    {$group:{
        _id:'$month',
        rev : {$sum:'$revenue'},
        pax : {$sum:'$pax'},
        rev_1 : {$sum:'$revenue_1'},
        pax_1 : {$sum:'$pax_1'},
        }},
        {$project:{
            _id:1,
            rev:1,
            pax:1,
            avg:{$divide:['$rev','$pax']},
            rev_1:1,
            pax_1:1,
            avg_1:{$divide:['$rev_1','$pax_1']},
        }}
     
])
        
        
_target_val.forEach(function(x){
    _target_pax_from_original = x.pax;
    _target_rev_from_original = x.rev;
    _target_avg_from_original = x.avg;
    _target_pax_from_original_1 = x.pax_1;
    _target_rev_from_original_1 = x.rev_1;
    _target_avg_from_original_1 = x.avg_1;
})      


var _forecast_val = db.JUP_DB_Forecast_OD.aggregate([
{$match:{
    //'dep_month':Number(curr_month),'dep_year':Number(curr_year)
    'Month':{$eq:curr_month},
    'Year':{$eq:curr_year+''},
    'pos':{$in:['AMM','CGP','CMB']},
    //'compartment':{$eq:'J'}
    }},
    {$group:{
        _id:'$Month',
            rev : {$sum:'$revenue'},
            pax : {$sum:'$pax'},
            rev_1 : {$sum:'$revenue_1'},
            pax_1 : {$sum:'$pax_1'},
        }},
        {$project:{
            _id:1,
            rev:1,
            pax:1,
            avg:{$divide:['$rev','$pax']},
            rev_1:1,
            pax_1:1,
            avg_1:{$divide:['$rev_1','$pax_1']}
        }}
])
        
        
_forecast_val.forEach(function(x){
        _forecast_pax_from_original = x.pax;
        _forecast_rev_from_original = x.rev;
        _forecast_avg_from_original = x.avg;
        _forecast_pax_from_original_1 = x.pax_1;
        _forecast_rev_from_original_1 = x.rev_1;
        _forecast_avg_from_original_1 = x.avg_1;
    })    


var _market_pax_from_original = 0;
var _market_rev_from_original = 0;
var _market_size_from_original = 0;
var _market_pax_from_original_1 = 0;
var _market_rev_from_original_1 = 0;
var _market_size_from_original_1 = 0;

var _marketShareObj = db.JUP_DB_Market_Share.aggregate([
{$match:{
    //'dep_month':Number(curr_month),'dep_year':Number(curr_year)
   'month':{$eq:Number(curr_month)},
    'year':{$eq:Number(curr_year)},
    'pos':{$in:['AMM','CGP','CMB']},
    //'compartment':{$eq:'J'}
    }},
    {$group:{
        _id:'$month',
            rev : {$sum:'$revenue'},
            pax : {$sum:'$pax'},
            rev_1 : {$sum:'$revenue_1'},
            pax_1 : {$sum:'$pax_1'},
                market_size:{$sum:'$market_size'},
                market_size_1:{$sum:'$market_size_1'}
        }},
        {$project:{
            _id:1,
            rev:1,
            pax:1,
            market_size:1,
            rev_1:1,
            pax_1:1,
            market_size_1:1
        }}
])

_marketShareObj.forEach(function(x){
        _market_pax_from_original = x.pax;
        _market_rev_from_original = x.rev;
        _market_size_from_original = x.market_size;
        _market_pax_from_original_1 = x.pax_1;
        _market_rev_from_original_1 = x.rev_1;
        _market_size_from_original_1 = x.market_size_1;    
    })  

//=========================================================



/*

==================================================

calculate pax and revenue based on present and past dates ( sales & flown )
*/
data.this_month.dep_date.forEach(function(x){
    if(new Date(x.dep_date) >= yesterDay){
        print(_salePax+"    "+_salePax_1);
        _salePax += x.sale_pax;
        _saleRev += x.sale_revenue;
        _salePax_1 += x.sale_pax_1;
        _saleRev_1 += x.sale_revenue_1;
    }
    else if(new Date(x.dep_date) < yesterDay){
        print(_salePax+"    "+_salePax_1);
        _salePax += x.flown_pax;
        _saleRev += x.flown_revenue;
        _salePax_1 += x.flown_pax_1;
        _saleRev_1 += x.flown_revenue_1;
        _onlyFlownPax += x.flown_pax;
        _onlyFlownRev += x.flown_revenue;
    count += 1; 
    }
})
//==================================================

_capacity = data.this_month.capacity;
_capacity_1 = data.this_month.capacity_1;
_distance = data.this_month.distance;
_MarketShare = _market_pax_from_original/(_market_size_from_original*100);
_MarketShare_1 = _market_pax_from_original_1/((_market_size_from_original_1==0?1:_market_size_from_original_1)*100);
var _thisMonthFromLanding = data.this_month;

_monthTargetPax = _target_pax_from_original;
_monthTargetRev = _target_rev_from_original;
_monthForecastPax = _forecast_pax_from_original;
_monthForecastRev = _forecast_rev_from_original;

_MarketShare = _market_size_from_original == 0 ? 0 : (_market_pax_from_original/_market_size_from_original)*100;
var _Target = db.JUP_DB_Analyst_Target.findOne({'user':user});
var _TargetRevenue = _Target.kpi.revenue;
var _TargetPax = _Target.kpi.ticket;
var _TargetAgility = _Target.kpi.agility;
var _TargetFMS = _Target.kpi.ms_growth;
var _TargetRevOfTopOD = _Target.kpi.review_od;
var _TargetCurrency = _Target.kpi.revenue_unit;
var _agility_unit = _Target.kpi.agility_unit;
var ticket_unit = _Target.kpi.ticket_unit;
var ms_growth_unit = _Target.kpi.ms_growth_unit ;
var review_od_unit = _Target.kpi.review_od_unit;


// Tiles are start from here
var _tiles = {};
var _Fares = {};

// fares
db.JUP_DB_Manual_Triggers_Module.aggregate([
{$match:{
    'dep_year':{ $eq: Number(curr_year) },
    'dep_month':{ $eq: Number(curr_month) }, 
    'pos.City':{ $in: listOfPos }
}},
{$unwind:'$sale_farebasis'},
{
  $group:{
      _id:{
            dep_year:'$dep_year',
             dep_month:'$dep_month', 
             fare_basis:'$sale_farebasis.fare_basis'
          },
          flagValue:{$sum:'$sale_farebasis.effectivity_calc'}
      }  
},
{$project:{
        dep_year:'$_id.dep_year',
         dep_month:'$_id.dep_month', 
         fare_basis:'$_id.fare_basis',
         flagValue:'$flagValue'
}},
{
  $group:{
      _id:{
            dep_year:'$dep_year',
             dep_month:'$dep_month', 
//             fare_basis:'$sale_farebasis.fare_basis'
          },
          totalFare:{$sum:1},
          effectiveFare:{$sum:{$cond:[
              {$gte:['$flagValue',0]},
              1,
              0
              ]}},
          inEffectiveFare:{$sum:{$cond:[
              {$lt:['$flagValue',0]},
              1,
              0
              ]}}
      }  
},
{$project:{
        dep_year:'$_id.dep_year',
         dep_month:'$_id.dep_month', 
         fare_basis:'$_id.fare_basis',
         totalFare:'$totalFare',
         effectiveFare:'$effectiveFare',
        inEffectiveFare:'$inEffectiveFare'
}},
{$out:'temp_Eff_Fare'}
])

var _resObj = db.temp_Eff_Fare.findOne()
db.temp_Eff_Fare.drop();
_Fares['totalFare'] = _resObj.totalFare;
_Fares['effectiveFare'] = _resObj.effectiveFare;
_Fares['inEffectiveFare'] = _resObj.inEffectiveFare;
_tiles['Fares'] = _Fares;
// significant and none significant OD
var totArray = [];
var sigArray = [];
var NoneSigArray = [];
_thisMonthFromLanding.dep_date.forEach(function(opendata){
    totArray = totArray.concat(opendata.totalOD);
    sigArray = sigArray.concat(opendata.signOD);
    NoneSigArray = NoneSigArray.concat(opendata.nonSignOD);
})

// function for removing null element from array
Array.prototype.remove = function() {
    var what, a = arguments, L = a.length, ax;
    while (L && this.length) {
        what = a[--L];
        while ((ax = this.indexOf(what)) !== -1) {
            this.splice(ax, 1);



        }
    }
    return this;
};

// get unique od from all the arrays 
totArray = Array.from(new Set(totArray)).remove(null);
sigArray = Array.from(new Set(sigArray)).remove(null);
NoneSigArray = Array.from(new Set(NoneSigArray)).remove(null);

// for creating and assingning of all unique sig and non sig count and array into the landing page collection
var _signOD = {};
_signOD['totalOD'] = totArray; 
_signOD['sigOD'] = sigArray;
_signOD['noneSigOD'] = NoneSigArray;
_signOD['totalOD_count'] = totArray.length; 
_signOD['sigOD_count'] = sigArray.length;
_signOD['noneSigOD_count'] = NoneSigArray.length;

// add sign embedded document into tiles doc
_tiles['Sig'] = _signOD;

// function for Yield formula 
function yield_(revenue,pax,distance){
   if(distance == 0 || pax == 0) {
     return 0;
   }
   else{
     return (revenue/(pax*distance))*100;
   }
    
}

// function for VLYR formula 
function vlyr(pax,pax_,capacity,capacity_){
     if(capacity!= 0 && capacity_!= 0){
        return ((pax*(capacity/capacity_))-pax_)/pax_;
    }
    else if(pax!= 0 && pax_!= 0){
        return ((pax-pax_)/pax_);
    }
    else{
        return 0;
    }
}


//_salePax_1 = data.this_month.sale_pax_1; 
_capacity_1 = data.this_month.capacity_1;

// if capacity doesn't have any data so give a same value by default
if (_capacity_1 == undefined || _capacity_1 == 0 ){
     _capacity_1 = _capacity;
    }

// to compute the vtgt and vlyr    
var _paxVLYR = vlyr(_salePax, _salePax_1, _capacity, _capacity_1)*100;
var _revVLYR = vlyr(_saleRev, _saleRev_1, _capacity, _capacity_1)*100;
var _yieldVLYR = (yield_(_saleRev,_salePax, _distance)-yield_(_saleRev_1,_salePax_1, _distance))/(yield_(_saleRev_1,_salePax_1, _distance)==0?1:yield_(_saleRev_1,_salePax_1, _distance))*100;
var _avgVLYR = (_saleRev ==0 &&_salePax ==0 )?0: ((_saleRev/_salePax)-(_saleRev_1/_salePax_1))/(_saleRev_1/_salePax_1)*100;
var _MarketShareVLYR = _MarketShare - _MarketShare_1;

//=====================VTGT=================================
// get number of days in month
function daysInMonth(month,year) {
    return new Date(year, month, 0).getDate();
}

function VTGT_pureFuture(_monthTargetPax, _monthForecastPax){
    return((_monthForecastPax) - _monthTargetPax) / (_monthTargetPax)
}    

function VTGT_purePast(_monthTargetPax, _monthFlownPax){
    return(_monthFlownPax - _monthTargetPax) / (_monthTargetPax)
}    

function VTGT_mix(_monthTargetPax_, _onlyFlownPax_, _monthForecastPax_, count_ ){
    if(_monthTargetPax_!=0){
        return ((_onlyFlownPax_ + (_monthForecastPax_/daysInMonth(curr_month,curr_year))*(daysInMonth(curr_month,curr_year)-count_) -_monthTargetPax) / _monthTargetPax);    
    }
    else{
        return 0;    
    }
}        

// 

//==========================================================    

//_onlyFlownPax
// Get logic from ashok and write logic for VTGT    
var _paxVTGT = 0;
var _revVTGT = 0;
var _yieldVTGT = 0;
var _avgVTGT = 0;
var _MarketShareVTGT = 0; 

// check which function has to choose for running VTGT based on date
if(new Date().getMonth()+1 == Number(curr_month)){
    _paxVTGT = VTGT_mix(_monthTargetPax,_onlyFlownPax,_monthForecastPax,count)*100;   
    _revVTGT = VTGT_mix(_monthTargetRev ,_onlyFlownRev,_monthForecastRev,count)*100;   
    _yieldVTGT = _revVTGT/(_paxVTGT*_distance)*100;   
    _avgVTGT = _revVTGT/_paxVTGT;
}
else if(new Date().getMonth()+1 > Number(curr_month)){
    
    _paxVTGT = VTGT_pureFuture(_monthTargetPax,_monthForecastPax)*100;   
    _revVTGT = VTGT_pureFuture(_monthTargetRev ,_monthForecastRev)*100;   
    _yieldVTGT = _revVTGT/(_paxVTGT*_distance)*100;   
    _avgVTGT = _revVTGT/_paxVTGT;
}
else if(new Date().getMonth()+1 < Number(curr_month)){
    //_monthTargetPax, _monthFlownPax
    _paxVTGT = VTGT_purePast(_monthTargetPax,_onlyFlownPax)*100;   
    _revVTGT =VTGT_purePast(_monthTargetRev,_onlyFlownRev)*100;   
    _yieldVTGT = _revVTGT/(_paxVTGT*_distance)*100;   
    _avgVTGT = _revVTGT/_paxVTGT;
}

var paxt = {};
var revt = {};
var yieldt = {};
var avgt = {};
var markett = {};

// vtgt and vlyr assigning embedded documents
paxt['vlyr'] = _paxVLYR;
paxt['vtgt'] = _paxVTGT;
paxt['value'] = _salePax;
paxt['value_1'] = _salePax_1;
revt['vlyr'] = _revVLYR;
revt['value'] = _saleRev;
revt['value_1'] = _saleRev_1;
yieldt['vlyr'] = _yieldVLYR;
yieldt['value'] = yield_(_saleRev,_salePax, _distance);
avgt['vlyr'] = _avgVLYR;
avgt['value'] = (_saleRev ==0 &&_salePax ==0 )?0: _saleRev/_salePax ;
markett['vlyr'] = _MarketShareVLYR;
markett['value'] = _MarketShare;
revt['vtgt'] = _revVTGT;
yieldt['vtgt'] = _yieldVTGT ;
avgt['vtgt'] = _avgVTGT;
markett['vtgt'] = _MarketShareVTGT;

// for review of top od from current month actioned triggers
var __totalTrigger = _SQLMonth.length;
var __percentageOfComp = 0;
var __processedTriggerWithinOneDay = 0;
_SQLMonth.forEach(function(x){
    
//     if(new Date(x.triggering_event_date) == new Date(x.action_date)){
//         __processedTriggerWithinOneDay +=1
//     }
    
    // check what are all Trigger processed in the same day 
    // just check two date's As per Mahesh and Sid's concept[05-07-2017]
    if(x.triggering_event_date == x.action_date){
        __processedTriggerWithinOneDay +=1
    }
})

// percentage of completion trigger with in one day
if(__totalTrigger!=0){
    __percentageOfComp = (__processedTriggerWithinOneDay/__totalTrigger)*100;
}
// assigning KPI value  
_KPIObj['Revenue'] ={'current':_saleRev,'target':_monthTargetRev} ;
_KPIObj['Forward_Booking'] ={'current':_paxVLYR,'target':_TargetPax,'unit':ticket_unit} ;
_KPIObj['Market_Share_Growth'] ={'current':_MarketShareVLYR,'target':_TargetFMS,'unit':ms_growth_unit} ;
_KPIObj['Agility'] ={'current':_agility,'target':_TargetAgility, 'unit':_agility_unit } ;
_KPIObj['Review_of_Top_OD'] ={'current':__percentageOfComp,'target':_TargetRevOfTopOD,'unit':review_od_unit} ;


// add all vtgt and vlyr into tiles sub document 
_tiles['pax'] = paxt;
_tiles['revenue'] = revt;
_tiles['market'] = markett;
_tiles['yield'] = yieldt;
_tiles['avgFare'] = avgt;

// insert the whole data 
db.db.JUP_DB_Landing_Page_.find({'user':user,'dep_month':Number(curr_month),'dep_year':Number(curr_year)}).forEach(function(x){
    db.db.JUP_DB_Landing_Page_28.insert(x);
})

// Update landing table
db.JUP_DB_Landing_Page_28.update({
        'user':user,'dep_month':Number(curr_month),'dep_year':Number(curr_year)
    },
    {
        $set:{
            'currency':_TargetCurrency,
            'this_month.dep_date':_updateDepDateData.sort(compare_depdate),
            'this_month.target_pax':_target_pax_from_original ,
            'this_month.target_avgFare':_target_avg_from_original,
            'this_month.target_revenue':_target_rev_from_original,
            'this_month.target_pax_1':_target_pax_from_original_1,
            'this_month.target_avgFare_1':_target_avg_from_original_1,
            'this_month.target_revenue_1':_target_rev_from_original_1,
            'this_month.forecast_pax':_forecast_pax_from_original,
            'this_month.forecast_avgFare':_forecast_avg_from_original,
            'this_month.forecast_revenue':_forecast_rev_from_original,
            'this_month.forecast_pax_1':_forecast_pax_from_original_1,
            'this_month.forecast_avgFare_1':_forecast_rev_from_original_1 ,
            'this_month.forecast_revenue_1':_forecast_avg_from_original_1,
            'this_month.market.pax':_market_pax_from_original,
            'this_month.market.pax_1':_market_pax_from_original_1,
            'this_month.market.market_size':_market_size_from_original,
            'this_month.market.market_size_1':_market_size_from_original_1,
            Trigger : Trigger,
            SQL : _SQLObj,
            KPI : _KPIObj,
            Tiles : _tiles
            }
    },
    {
        upsert:true
    });



















// global variable have to chage this to run like list of user 
var user = 'Dummy';
var userDetail = db.JUP_DB_User.findOne({'name':user});
var listOfPos = userDetail.list_of_pos ;
var cursor = db.getCollection('JUP_DB_Workflow').find({'pos':{$in:listOfPos}})
var odUser = db.getCollection('JUP_DB_Workflow_OD_User').find({'pos':{$in:listOfPos}})
var _curAry = [];
var _curObj = {};
var _odUserAry = [];
var triggerOverAllFare = 0;
var triggerOverAllCritical = 0;
var _odArr = [];
var TriggerOd = {};
var TriggerType ={};
var TriggerOverall = {};
var Trigger = {};
// get system date I mean yesterday date
var curDay = new Date();
curDay.setDate(-62);
var yesterDay = new Date();
yesterDay.setDate(curDay.getDate() - 1);



/*'''''''''''''''''''''''''''''''''''''''''''''''*/
// For give a demo to vinassa in the month of march



curDay.setDate(curDay.getDate()); // <-- add this to make it "yesterday"
var weekDay = new Date();
weekDay.setDate(weekDay.getDate() - 8); // <-- add this to make it "one week before"
var monthDay = new Date();
monthDay.setDate(monthDay.getDate() - 30); // <-- add this to make it "one month back"

var yearDay = new Date(curDay.getFullYear()+'-01-01');



var curr_date = curDay.getDate(); // <-- don't subtract 1 anymore
var curr_month = ("0" + (curDay.getMonth()+1)).slice(-2);// global variable have to chage this to run like list of user 


// For testing give current month as month -1
//var curr_month = ("0" + (curDay.getMonth() )).slice(-2);

var curr_year = curDay.getFullYear();
var str_date = curr_year +"-"+ curr_month+"-"+curr_date

var triggerStatus = [];
var triggerStatusObjDay = {};
var triggerStatusObjWeek = {};
var triggerStatusObjMonth = {};
var triggerStatusObjYTD = {};
var triggerStatusObjRes = {};
var sysTriggerType = {};
var _TriggerOdSub = [];

var SQLArray = [];
var _SQLObj = {};
var _SQLDay = [];
var _SQLWeek = [];
var _SQLMonth = [];
var _SQLYTD = [];
var _SQLRes = {};


cursor.forEach(function(x){
    _curObj = x;
    if(x.recommendation_category == 'A'){
        triggerOverAllCritical++;
    }
    if(x.status.toLowerCase() == 'pending'){
        _curAry.push(_curObj);
        _odArr.push(x.od);
        TriggerOd[x.od] = (TriggerOd[x.od] == undefined ? 0 : TriggerOd[x.od]) +1;
        TriggerType[x.type_of_trigger] = (TriggerType[x.type_of_trigger] == undefined ? 0 : TriggerType[x.type_of_trigger]) +1;
        
        // to get system trigger's type of Trigger list of Count
        if(x.type_of_trigger.toLowerCase() == 'system'){
            
            sysTriggerType[x.trigger_type] = (sysTriggerType[x.trigger_type] == undefined ? 0 : sysTriggerType[x.trigger_type]) +1;
        }
        
        // to loading OD wise different trigger count
        _TriggerOdSub.push({ 'od':x.od,'type':x.type_of_trigger});
        //_TriggerOdSub[x.type_of_trigger] = (_TriggerOdSub[x.type_of_trigger] == undefined ? 0 : _TriggerOdSub[x.type_of_trigger]) +1;
        
        for(var i=0; i<TriggerOd.length-1; i++){
            target = target[TriggerOd[i]];
        }
        //TriggerOd[x.od] = 
    }
        
    // for one day graph
    //print(new Date(x.triggering_event_date)+" curr ");
    if(str_date == x.triggering_event_date){
        //print(new Date(x.triggering_event_date)+" curr ");
        
        triggerStatusObjDay[x.status] = (triggerStatusObjDay[x.status] == undefined ? 0 : triggerStatusObjDay[x.status]) +1;
        
    }
    // for one week graph
    if(new Date(x.triggering_event_date) >= weekDay ){
            //print(new Date(x.triggering_event_date)+"  week");
        triggerStatusObjWeek[x.status] = (triggerStatusObjWeek[x.status] == undefined ? 0 : triggerStatusObjWeek[x.status]) +1;
    }    
    // for one month graph
    if(new Date(x.triggering_event_date) >= monthDay){
            //print(new Date(x.triggering_event_date)+"  month");
        triggerStatusObjMonth[x.status] = (triggerStatusObjMonth[x.status] == undefined ? 0 : triggerStatusObjMonth[x.status]) +1;
    }
    // for one YTD graph
    if(new Date(x.triggering_event_date) >= yearDay ){
            //print(new Date(x.triggering_event_date)+"  YTD");
        triggerStatusObjYTD[x.status] = (triggerStatusObjYTD[x.status] == undefined ? 0 : triggerStatusObjYTD[x.status]) +1;
    }


    // for SQL
    
        // for one day graph
        if(str_date == x.triggering_event_date){
            //print(new Date(x.triggering_event_date)+" curr ");
            
            _SQLDay.push(x);
            
        }
        // for one week graph
        if(new Date(x.triggering_event_date) >= weekDay ){
                //print(new Date(x.triggering_event_date)+"  week");
            _SQLWeek.push(x);
        }    
        // for one month graph
        if(new Date(x.triggering_event_date) >= monthDay){
                //print(new Date(x.triggering_event_date)+"  month");
            _SQLMonth.push(x);
        }
        // for one YTD graph
        if(new Date(x.triggering_event_date) >= yearDay ){
               // print(new Date(x.triggering_event_date)+"  YTD");
            _SQLYTD.push(x);
        }

})
triggerStatusObjRes['Day'] = triggerStatusObjDay;
triggerStatusObjRes['Week'] = triggerStatusObjWeek;
triggerStatusObjRes['Month'] = triggerStatusObjMonth;
triggerStatusObjRes['YTD'] = triggerStatusObjYTD;

// unique OD's
var triggerOverAllOd = Array.from(new Set(_odArr));


var od = '';

function _uniquGroupArray(array){
    var od = '';
    var type = '';
    var result = {};
    var opt = [];
    var i=0;
    var arr_Length = array.length;
    array.forEach(function(x){
         if(od != x.od ){
            if(i==0){
                result[x.type] = x.value;    
                opt = [];
            }else{
                //print(result);
                opt.push(result);
                result = {}
                result[x.type] = x.value;    
                //result = {}
            }
         }
         else{
             if(type == x.type){
                result[x.type] += x.value; 
             }
             else{
                 result[x.type] = x.value; 
             }
                
         }
       i++;
        
        result['od'] = x.od;
        
        od = x.od;
        type = x.type;
        if(i==arr_Length){
            //print(result);
            opt.push(result);
        }   
         
    });
    return opt;
}

// get a unique od and trigger type count 
function _uniqueODTrigger(array){
    var od = '';
    var type = '';
    var result = {};
    var opt = [];
    var i=0;
     var arr_Length = array.length;
    array.forEach(function(x){
        if(od != x.od || type != x.type){
            
            if(i==0){
                result['value'] = 0;    
                opt = [];
            }else{
                //print(result);
                opt.push({
                    od:result['od'],
                    type:result['type'],
                    value:result['value'],
                    });
                result['value'] = 0;    
            }
            
            
        }
        i++;
        //print('outer');
        result['value'] += 1;
        result['od'] = x.od;
        result['type'] = x.type;
        od = x.od;
        type = x.type;
        if(i==arr_Length){
            //print(result);
            opt.push({
                     od:result['od'],
                    type:result['type'],
                    value:result['value'],
            });
            
        }
    });
    return opt;
}
// compare and arrange unique array list
function compare(a,b) {
  if (a.od < b.od)
    return -1;
  if (a.od > b.od)
    return 1;
  return 0;
}

// sort dep_date array based on departure date
function compare_depdate(a,b) {
  if (a.dep_date < b.dep_date)
    return -1;
  if (a.dep_date > b.dep_date)
    return 1;
  return 0;
}



odUser.forEach(function(x){
    _odUserAry.push(x);
    if(typeof x.fares_docs == 'object'){
           triggerOverAllFare+= x.fares_docs.length;
    }
})
var triggerOverAllTotal = _curAry.length;   

// To add work package also in the trigger type collection
TriggerType['work_package'] = db.JUP_DB_WorkPackage.find({user:user,filingstatus : 'pending'}).count();

TriggerOverall['Critical'] = triggerOverAllCritical;
TriggerOverall['Total'] = triggerOverAllTotal;
TriggerOverall['Fares'] = triggerOverAllFare;
TriggerOverall['od'] = triggerOverAllOd.length;
Trigger['overAll'] = TriggerOverall;
TriggerOd['detail'] = _uniquGroupArray(_uniqueODTrigger(_TriggerOdSub).sort(compare));
Trigger['triggerOd'] = TriggerOd;
TriggerType['detail'] = sysTriggerType;
Trigger['triggerTypes'] = TriggerType;
Trigger['triggerStatus'] = triggerStatusObjRes;



function _SQLGetElement(data,index){
    var num = 0;
    var hour = 0;
    for(num=0;num<index;num++){
        if(data[num].status.toLowerCase() == 'pending'){
            hour = 0;
        }
        else{
            if(data[num].action_date != null && data[num].action_time != null ){
                var testDate = new Date(data[num].action_date+'T'+data[num].action_time+':00Z');
                var testDate1 = new Date(data[num].triggering_event_date+'T'+data[num].triggering_event_time+':00Z');
                hour += Math.abs(testDate1-testDate) / 36e5;        
            }
            else{
                
            }
            
        }
    }
    return hour;
}


//_SQLGetElement()

function _SQLAlg(data_){
    var SQLArray = {};
    var lenOfArray = data_.length;
    var tenVal = Math.ceil(lenOfArray*.1);
    var tfVal = Math.ceil(lenOfArray*.25);
    var fiVal = Math.ceil(lenOfArray*.5);
    var sfVal = Math.ceil(lenOfArray*.75);
    var hnVal = Math.ceil(lenOfArray*1);
    SQLArray['10p'] = _SQLGetElement(data_,tenVal);
    SQLArray['25p'] = _SQLGetElement(data_,tfVal);
    SQLArray['50p'] = _SQLGetElement(data_,fiVal);
    SQLArray['75p'] = _SQLGetElement(data_,sfVal);
    SQLArray['100p'] = _SQLGetElement(data_,hnVal);
    var num = 0;
    var penCount = 0;
    for(num = 0;num<data_.length;num++){
        if(data_[num].status=='pending'){
           penCount++; 
        }
    }
    SQLArray['pending'] = penCount;
    return SQLArray;
}

// _SQLAlg(_SQLYTD)

// SQL code continue

    // for making order put pending triggers last in the array
function _orderPending(data){
    var tempArr1 = [];
    var tempArr2 = [];
    data.forEach(function(x){
        if(x.status == 'pending'){
            tempArr2.push(x);
        }
        else{
            tempArr1.push(x);
        }
    });
    var tempOrderedArray = [];
    if(tempArr1 == undefined){
        tempOrderedArray = tempArr1;
    }
    else{
        tempOrderedArray = tempArr1.concat(tempArr2);
    }
    
    return _SQLAlg(tempOrderedArray);
}



_SQLObj['Day'] = _orderPending(_SQLDay);
_SQLObj['Week'] = _orderPending(_SQLWeek);
_SQLObj['Month'] = _orderPending(_SQLMonth);
_SQLObj['Year'] = _orderPending(_SQLYTD);

// For KPI Grid
var _KPIObj = {};
var _KPIObjSubDoc = {};
var _saleRev = 0;
var _salePax = 0;
var _saleRev_1 = 0;
var _salePax_1 = 0;
var _capacity_1 = 0;
var _capacity = 0;
var _distance = 0;
var _onlyFlownPax = 0;
var _onlyFlownRev = 0;
var _monthTargetPax = 0;
var _monthTargetRev = 0;
var _monthForecastPax = 0;
var _monthForecastRev = 0;
// count for to check the number of days flown data come under the month
var count = 0;
// for agility we need competitor price change trigger. So till trigger is getting ready we keep agility as 0
var _agility = 0;
var _MarketShare = 0;
var _MarketShare_1 = 0;
// Ask Sid or Mahesh is that Except pending trigger or all the trigger of current month
var _ReviewOD = 0;

var data = db.JUP_DB_Landing_Page_.findOne({'user':user,'dep_month':Number(curr_month),'dep_year':Number(curr_year)})

var _updateDepDateData = [];

// For updating Yield and avg fare in the dep_date array based on the date
data.this_month.dep_date.forEach(function(x){
    if(new Date(x.dep_date) >= yesterDay){
        x['avgFare'] = x.sale_pax == 0 ? 0 : x.sale_revenue/x.sale_pax ;
        x['yield'] = x.sale_pax*x.distance == 0 ? 0 :x.sale_revenue/(x.sale_pax*x.distance) ;
    }
    else if(new Date(x.dep_date) < yesterDay){
        x['avgFare'] = x.flown_pax == 0 ? 0 : x.flown_revenue/x.flown_pax;
        x['yield'] = x.flown_pax*x.distance == 0 ? 0 : x.flown_revenue/(x.flown_pax*x.distance);
    }
    _updateDepDateData.push(x);
})

//=========================================================
// To update monthly target and forecast from actual collection to landing page based on the user profile

//db.getCollection('JUP_DB_Target_OD').find({'od':'DXBDOH','pos':'DXB','month':7})

var _target_pax_from_original = 0;
var _target_rev_from_original = 0;
var _target_avg_from_original = 0;
var _forecast_pax_from_original = 0;
var _forecast_rev_from_original = 0;
var _forecast_avg_from_original = 0;
var _target_pax_from_original_1 = 0;
var _target_rev_from_original_1 = 0;
var _target_avg_from_original_1 = 0;
var _forecast_pax_from_original_1 = 0;
var _forecast_rev_from_original_1 = 0;
var _forecast_avg_from_original_1 = 0;


var _target_val = db.JUP_DB_Target_OD.aggregate([
{$match:{
    //'dep_month':Number(curr_month),'dep_year':Number(curr_year)
    'month':{$eq:Number(curr_month)},
    'year':{$eq:Number(curr_year)},
    'pos':{$in:['AMM','CGP','CMB']},
    //'compartment':{$eq:'J'}
    }},
    {$group:{
        _id:'$month',
        rev : {$sum:'$revenue'},
        pax : {$sum:'$pax'},
        rev_1 : {$sum:'$revenue_1'},
        pax_1 : {$sum:'$pax_1'},
        }},
        {$project:{
            _id:1,
            rev:1,
            pax:1,
            avg:{$divide:['$rev','$pax']},
            rev_1:1,
            pax_1:1,
            avg_1:{$divide:['$rev_1','$pax_1']},
        }}
     
])
        
        
_target_val.forEach(function(x){
    _target_pax_from_original = x.pax;
    _target_rev_from_original = x.rev;
    _target_avg_from_original = x.avg;
    _target_pax_from_original_1 = x.pax_1;
    _target_rev_from_original_1 = x.rev_1;
    _target_avg_from_original_1 = x.avg_1;
})      


var _forecast_val = db.JUP_DB_Forecast_OD.aggregate([
{$match:{
    //'dep_month':Number(curr_month),'dep_year':Number(curr_year)
    'Month':{$eq:curr_month},
    'Year':{$eq:curr_year+''},
    'pos':{$in:['AMM','CGP','CMB']},
    //'compartment':{$eq:'J'}
    }},
    {$group:{
        _id:'$Month',
            rev : {$sum:'$revenue'},
            pax : {$sum:'$pax'},
            rev_1 : {$sum:'$revenue_1'},
            pax_1 : {$sum:'$pax_1'},
        }},
        {$project:{
            _id:1,
            rev:1,
            pax:1,
            avg:{$divide:['$rev','$pax']},
            rev_1:1,
            pax_1:1,
            avg_1:{$divide:['$rev_1','$pax_1']}
        }}
])
        
        
_forecast_val.forEach(function(x){
        _forecast_pax_from_original = x.pax;
        _forecast_rev_from_original = x.rev;
        _forecast_avg_from_original = x.avg;
        _forecast_pax_from_original_1 = x.pax_1;
        _forecast_rev_from_original_1 = x.rev_1;
        _forecast_avg_from_original_1 = x.avg_1;
    })    


var _market_pax_from_original = 0;
var _market_rev_from_original = 0;
var _market_size_from_original = 0;
var _market_pax_from_original_1 = 0;
var _market_rev_from_original_1 = 0;
var _market_size_from_original_1 = 0;

var _marketShareObj = db.JUP_DB_Market_Share.aggregate([
{$match:{
    //'dep_month':Number(curr_month),'dep_year':Number(curr_year)
   'month':{$eq:Number(curr_month)},
    'year':{$eq:Number(curr_year)},
    'pos':{$in:['AMM','CGP','CMB']},
    //'compartment':{$eq:'J'}
    }},
    {$group:{
        _id:'$month',
            rev : {$sum:'$revenue'},
            pax : {$sum:'$pax'},
            rev_1 : {$sum:'$revenue_1'},
            pax_1 : {$sum:'$pax_1'},
                market_size:{$sum:'$market_size'},
                market_size_1:{$sum:'$market_size_1'}
        }},
        {$project:{
            _id:1,
            rev:1,
            pax:1,
            market_size:1,
            rev_1:1,
            pax_1:1,
            market_size_1:1
        }}
])

_marketShareObj.forEach(function(x){
        _market_pax_from_original = x.pax;
        _market_rev_from_original = x.rev;
        _market_size_from_original = x.market_size;
        _market_pax_from_original_1 = x.pax_1;
        _market_rev_from_original_1 = x.rev_1;
        _market_size_from_original_1 = x.market_size_1;    
    })  

//=========================================================



/*

==================================================

calculate pax and revenue based on present and past dates ( sales & flown )
*/
data.this_month.dep_date.forEach(function(x){
    if(new Date(x.dep_date) >= yesterDay){
        print(_salePax+"    "+_salePax_1);
        _salePax += x.sale_pax;
        _saleRev += x.sale_revenue;
        _salePax_1 += x.sale_pax_1;
        _saleRev_1 += x.sale_revenue_1;
    }
    else if(new Date(x.dep_date) < yesterDay){
        print(_salePax+"    "+_salePax_1);
        _salePax += x.flown_pax;
        _saleRev += x.flown_revenue;
        _salePax_1 += x.flown_pax_1;
        _saleRev_1 += x.flown_revenue_1;
        _onlyFlownPax += x.flown_pax;
        _onlyFlownRev += x.flown_revenue;
    count += 1; 
    }
})
//==================================================

_capacity = data.this_month.capacity;
_capacity_1 = data.this_month.capacity_1;
_distance = data.this_month.distance;
_MarketShare = _market_pax_from_original/(_market_size_from_original*100);
_MarketShare_1 = _market_pax_from_original_1/((_market_size_from_original_1==0?1:_market_size_from_original_1)*100);
var _thisMonthFromLanding = data.this_month;

_monthTargetPax = _target_pax_from_original;
_monthTargetRev = _target_rev_from_original;
_monthForecastPax = _forecast_pax_from_original;
_monthForecastRev = _forecast_rev_from_original;

_MarketShare = _market_size_from_original == 0 ? 0 : (_market_pax_from_original/_market_size_from_original)*100;
var _Target = db.JUP_DB_Analyst_Target.findOne({'user':user});
var _TargetRevenue = _Target.kpi.revenue;
var _TargetPax = _Target.kpi.ticket;
var _TargetAgility = _Target.kpi.agility;
var _TargetFMS = _Target.kpi.ms_growth;
var _TargetRevOfTopOD = _Target.kpi.review_od;
var _TargetCurrency = _Target.kpi.revenue_unit;
var _agility_unit = _Target.kpi.agility_unit;
var ticket_unit = _Target.kpi.ticket_unit;
var ms_growth_unit = _Target.kpi.ms_growth_unit ;
var review_od_unit = _Target.kpi.review_od_unit;


// Tiles are start from here
var _tiles = {};
var _Fares = {};

// fares
db.JUP_DB_Manual_Triggers_Module.aggregate([
{$match:{
    'dep_year':{ $eq: Number(curr_year) },
    'dep_month':{ $eq: Number(curr_month) }, 
    'pos.City':{ $in: listOfPos }
}},
{$unwind:'$sale_farebasis'},
{
  $group:{
      _id:{
            dep_year:'$dep_year',
             dep_month:'$dep_month', 
             fare_basis:'$sale_farebasis.fare_basis'
          },
          flagValue:{$sum:'$sale_farebasis.effectivity_calc'}
      }  
},
{$project:{
        dep_year:'$_id.dep_year',
         dep_month:'$_id.dep_month', 
         fare_basis:'$_id.fare_basis',
         flagValue:'$flagValue'
}},
{
  $group:{
      _id:{
            dep_year:'$dep_year',
             dep_month:'$dep_month', 
//             fare_basis:'$sale_farebasis.fare_basis'
          },
          totalFare:{$sum:1},
          effectiveFare:{$sum:{$cond:[
              {$gte:['$flagValue',0]},
              1,
              0
              ]}},
          inEffectiveFare:{$sum:{$cond:[
              {$lt:['$flagValue',0]},
              1,
              0
              ]}}
      }  
},
{$project:{
        dep_year:'$_id.dep_year',
         dep_month:'$_id.dep_month', 
         fare_basis:'$_id.fare_basis',
         totalFare:'$totalFare',
         effectiveFare:'$effectiveFare',
        inEffectiveFare:'$inEffectiveFare'
}},
{$out:'temp_Eff_Fare'}
])

var _resObj = db.temp_Eff_Fare.findOne()
db.temp_Eff_Fare.drop();
_Fares['totalFare'] = _resObj.totalFare;
_Fares['effectiveFare'] = _resObj.effectiveFare;
_Fares['inEffectiveFare'] = _resObj.inEffectiveFare;
_tiles['Fares'] = _Fares;
// significant and none significant OD
var totArray = [];
var sigArray = [];
var NoneSigArray = [];
_thisMonthFromLanding.dep_date.forEach(function(opendata){
    totArray = totArray.concat(opendata.totalOD);
    sigArray = sigArray.concat(opendata.signOD);
    NoneSigArray = NoneSigArray.concat(opendata.nonSignOD);
})

// function for removing null element from array
Array.prototype.remove = function() {
    var what, a = arguments, L = a.length, ax;
    while (L && this.length) {
        what = a[--L];
        while ((ax = this.indexOf(what)) !== -1) {
            this.splice(ax, 1);



        }
    }
    return this;
};

// get unique od from all the arrays 
totArray = Array.from(new Set(totArray)).remove(null);
sigArray = Array.from(new Set(sigArray)).remove(null);
NoneSigArray = Array.from(new Set(NoneSigArray)).remove(null);

// for creating and assingning of all unique sig and non sig count and array into the landing page collection
var _signOD = {};
_signOD['totalOD'] = totArray; 
_signOD['sigOD'] = sigArray;
_signOD['noneSigOD'] = NoneSigArray;
_signOD['totalOD_count'] = totArray.length; 
_signOD['sigOD_count'] = sigArray.length;
_signOD['noneSigOD_count'] = NoneSigArray.length;

// add sign embedded document into tiles doc
_tiles['Sig'] = _signOD;

// function for Yield formula 
function yield_(revenue,pax,distance){
   if(distance == 0 || pax == 0) {
     return 0;
   }
   else{
     return (revenue/(pax*distance))*100;
   }
    
}

// function for VLYR formula 
function vlyr(pax,pax_,capacity,capacity_){
     if(capacity!= 0 && capacity_!= 0){
        return ((pax*(capacity/capacity_))-pax_)/pax_;
    }
    else if(pax!= 0 && pax_!= 0){
        return ((pax-pax_)/pax_);
    }
    else{
        return 0;
    }
}


//_salePax_1 = data.this_month.sale_pax_1; 
_capacity_1 = data.this_month.capacity_1;

// if capacity doesn't have any data so give a same value by default
if (_capacity_1 == undefined || _capacity_1 == 0 ){
     _capacity_1 = _capacity;
    }

// to compute the vtgt and vlyr    
var _paxVLYR = vlyr(_salePax, _salePax_1, _capacity, _capacity_1)*100;
var _revVLYR = vlyr(_saleRev, _saleRev_1, _capacity, _capacity_1)*100;
var _yieldVLYR = (yield_(_saleRev,_salePax, _distance)-yield_(_saleRev_1,_salePax_1, _distance))/(yield_(_saleRev_1,_salePax_1, _distance)==0?1:yield_(_saleRev_1,_salePax_1, _distance))*100;
var _avgVLYR = (_saleRev ==0 &&_salePax ==0 )?0: ((_saleRev/_salePax)-(_saleRev_1/_salePax_1))/(_saleRev_1/_salePax_1)*100;
var _MarketShareVLYR = _MarketShare - _MarketShare_1;

//=====================VTGT=================================
// get number of days in month
function daysInMonth(month,year) {
    return new Date(year, month, 0).getDate();
}

function VTGT_pureFuture(_monthTargetPax, _monthForecastPax){
    return((_monthForecastPax) - _monthTargetPax) / (_monthTargetPax)
}    

function VTGT_purePast(_monthTargetPax, _monthFlownPax){
    return(_monthFlownPax - _monthTargetPax) / (_monthTargetPax)
}    

function VTGT_mix(_monthTargetPax_, _onlyFlownPax_, _monthForecastPax_, count_ ){
    if(_monthTargetPax_!=0){
        return ((_onlyFlownPax_ + (_monthForecastPax_/daysInMonth(curr_month,curr_year))*(daysInMonth(curr_month,curr_year)-count_) -_monthTargetPax) / _monthTargetPax);    
    }
    else{
        return 0;    
    }
}        

// 

//==========================================================    

//_onlyFlownPax
// Get logic from ashok and write logic for VTGT    
var _paxVTGT = 0;
var _revVTGT = 0;
var _yieldVTGT = 0;
var _avgVTGT = 0;
var _MarketShareVTGT = 0; 

// check which function has to choose for running VTGT based on date
if(new Date().getMonth()+1 == Number(curr_month)){
    _paxVTGT = VTGT_mix(_monthTargetPax,_onlyFlownPax,_monthForecastPax,count)*100;   
    _revVTGT = VTGT_mix(_monthTargetRev ,_onlyFlownRev,_monthForecastRev,count)*100;   
    _yieldVTGT = _revVTGT/(_paxVTGT*_distance)*100;   
    _avgVTGT = _revVTGT/_paxVTGT;
}
else if(new Date().getMonth()+1 > Number(curr_month)){
    
    _paxVTGT = VTGT_pureFuture(_monthTargetPax,_monthForecastPax)*100;   
    _revVTGT = VTGT_pureFuture(_monthTargetRev ,_monthForecastRev)*100;   
    _yieldVTGT = _revVTGT/(_paxVTGT*_distance)*100;   
    _avgVTGT = _revVTGT/_paxVTGT;
}
else if(new Date().getMonth()+1 < Number(curr_month)){
    //_monthTargetPax, _monthFlownPax
    _paxVTGT = VTGT_purePast(_monthTargetPax,_onlyFlownPax)*100;   
    _revVTGT =VTGT_purePast(_monthTargetRev,_onlyFlownRev)*100;   
    _yieldVTGT = _revVTGT/(_paxVTGT*_distance)*100;   
    _avgVTGT = _revVTGT/_paxVTGT;
}

var paxt = {};
var revt = {};
var yieldt = {};
var avgt = {};
var markett = {};

// vtgt and vlyr assigning embedded documents
paxt['vlyr'] = _paxVLYR;
paxt['vtgt'] = _paxVTGT;
paxt['value'] = _salePax;
paxt['value_1'] = _salePax_1;
revt['vlyr'] = _revVLYR;
revt['value'] = _saleRev;
revt['value_1'] = _saleRev_1;
yieldt['vlyr'] = _yieldVLYR;
yieldt['value'] = yield_(_saleRev,_salePax, _distance);
avgt['vlyr'] = _avgVLYR;
avgt['value'] = (_saleRev ==0 &&_salePax ==0 )?0: _saleRev/_salePax ;
markett['vlyr'] = _MarketShareVLYR;
markett['value'] = _MarketShare;
revt['vtgt'] = _revVTGT;
yieldt['vtgt'] = _yieldVTGT ;
avgt['vtgt'] = _avgVTGT;
markett['vtgt'] = _MarketShareVTGT;

// for review of top od from current month actioned triggers
var __totalTrigger = _SQLMonth.length;
var __percentageOfComp = 0;
var __processedTriggerWithinOneDay = 0;
_SQLMonth.forEach(function(x){
    
//     if(new Date(x.triggering_event_date) == new Date(x.action_date)){
//         __processedTriggerWithinOneDay +=1
//     }
    
    // check what are all Trigger processed in the same day 
    // just check two date's As per Mahesh and Sid's concept[05-07-2017]
    if(x.triggering_event_date == x.action_date){
        __processedTriggerWithinOneDay +=1
    }
})

// percentage of completion trigger with in one day
if(__totalTrigger!=0){
    __percentageOfComp = (__processedTriggerWithinOneDay/__totalTrigger)*100;
}
// assigning KPI value  
_KPIObj['Revenue'] ={'current':_saleRev,'target':_monthTargetRev} ;
_KPIObj['Forward_Booking'] ={'current':_paxVLYR,'target':_TargetPax,'unit':ticket_unit} ;
_KPIObj['Market_Share_Growth'] ={'current':_MarketShareVLYR,'target':_TargetFMS,'unit':ms_growth_unit} ;
_KPIObj['Agility'] ={'current':_agility,'target':_TargetAgility, 'unit':_agility_unit } ;
_KPIObj['Review_of_Top_OD'] ={'current':__percentageOfComp,'target':_TargetRevOfTopOD,'unit':review_od_unit} ;


// add all vtgt and vlyr into tiles sub document 
_tiles['pax'] = paxt;
_tiles['revenue'] = revt;
_tiles['market'] = markett;
_tiles['yield'] = yieldt;
_tiles['avgFare'] = avgt;

// insert the whole data 
db.db.JUP_DB_Landing_Page_.find({'user':user,'dep_month':Number(curr_month),'dep_year':Number(curr_year)}).forEach(function(x){
    db.db.JUP_DB_Landing_Page_29.insert(x);
})

// Update landing table
db.JUP_DB_Landing_Page_29.update({
        'user':user,'dep_month':Number(curr_month),'dep_year':Number(curr_year)
    },
    {
        $set:{
            'currency':_TargetCurrency,
            'this_month.dep_date':_updateDepDateData.sort(compare_depdate),
            'this_month.target_pax':_target_pax_from_original ,
            'this_month.target_avgFare':_target_avg_from_original,
            'this_month.target_revenue':_target_rev_from_original,
            'this_month.target_pax_1':_target_pax_from_original_1,
            'this_month.target_avgFare_1':_target_avg_from_original_1,
            'this_month.target_revenue_1':_target_rev_from_original_1,
            'this_month.forecast_pax':_forecast_pax_from_original,
            'this_month.forecast_avgFare':_forecast_avg_from_original,
            'this_month.forecast_revenue':_forecast_rev_from_original,
            'this_month.forecast_pax_1':_forecast_pax_from_original_1,
            'this_month.forecast_avgFare_1':_forecast_rev_from_original_1 ,
            'this_month.forecast_revenue_1':_forecast_avg_from_original_1,
            'this_month.market.pax':_market_pax_from_original,
            'this_month.market.pax_1':_market_pax_from_original_1,
            'this_month.market.market_size':_market_size_from_original,
            'this_month.market.market_size_1':_market_size_from_original_1,
            Trigger : Trigger,
            SQL : _SQLObj,
            KPI : _KPIObj,
            Tiles : _tiles
            }
    },
    {
        upsert:true
    });



















// global variable have to chage this to run like list of user 
var user = 'Dummy';
var userDetail = db.JUP_DB_User.findOne({'name':user});
var listOfPos = userDetail.list_of_pos ;
var cursor = db.getCollection('JUP_DB_Workflow').find({'pos':{$in:listOfPos}})
var odUser = db.getCollection('JUP_DB_Workflow_OD_User').find({'pos':{$in:listOfPos}})
var _curAry = [];
var _curObj = {};
var _odUserAry = [];
var triggerOverAllFare = 0;
var triggerOverAllCritical = 0;
var _odArr = [];
var TriggerOd = {};
var TriggerType ={};
var TriggerOverall = {};
var Trigger = {};
// get system date I mean yesterday date
var curDay = new Date();
curDay.setDate(-61);
var yesterDay = new Date();
yesterDay.setDate(curDay.getDate() - 1);



/*'''''''''''''''''''''''''''''''''''''''''''''''*/
// For give a demo to vinassa in the month of march



curDay.setDate(curDay.getDate()); // <-- add this to make it "yesterday"
var weekDay = new Date();
weekDay.setDate(weekDay.getDate() - 8); // <-- add this to make it "one week before"
var monthDay = new Date();
monthDay.setDate(monthDay.getDate() - 30); // <-- add this to make it "one month back"

var yearDay = new Date(curDay.getFullYear()+'-01-01');



var curr_date = curDay.getDate(); // <-- don't subtract 1 anymore
var curr_month = ("0" + (curDay.getMonth()+1)).slice(-2);// global variable have to chage this to run like list of user 


// For testing give current month as month -1
//var curr_month = ("0" + (curDay.getMonth() )).slice(-2);

var curr_year = curDay.getFullYear();
var str_date = curr_year +"-"+ curr_month+"-"+curr_date

var triggerStatus = [];
var triggerStatusObjDay = {};
var triggerStatusObjWeek = {};
var triggerStatusObjMonth = {};
var triggerStatusObjYTD = {};
var triggerStatusObjRes = {};
var sysTriggerType = {};
var _TriggerOdSub = [];

var SQLArray = [];
var _SQLObj = {};
var _SQLDay = [];
var _SQLWeek = [];
var _SQLMonth = [];
var _SQLYTD = [];
var _SQLRes = {};


cursor.forEach(function(x){
    _curObj = x;
    if(x.recommendation_category == 'A'){
        triggerOverAllCritical++;
    }
    if(x.status.toLowerCase() == 'pending'){
        _curAry.push(_curObj);
        _odArr.push(x.od);
        TriggerOd[x.od] = (TriggerOd[x.od] == undefined ? 0 : TriggerOd[x.od]) +1;
        TriggerType[x.type_of_trigger] = (TriggerType[x.type_of_trigger] == undefined ? 0 : TriggerType[x.type_of_trigger]) +1;
        
        // to get system trigger's type of Trigger list of Count
        if(x.type_of_trigger.toLowerCase() == 'system'){
            
            sysTriggerType[x.trigger_type] = (sysTriggerType[x.trigger_type] == undefined ? 0 : sysTriggerType[x.trigger_type]) +1;
        }
        
        // to loading OD wise different trigger count
        _TriggerOdSub.push({ 'od':x.od,'type':x.type_of_trigger});
        //_TriggerOdSub[x.type_of_trigger] = (_TriggerOdSub[x.type_of_trigger] == undefined ? 0 : _TriggerOdSub[x.type_of_trigger]) +1;
        
        for(var i=0; i<TriggerOd.length-1; i++){
            target = target[TriggerOd[i]];
        }
        //TriggerOd[x.od] = 
    }
        
    // for one day graph
    //print(new Date(x.triggering_event_date)+" curr ");
    if(str_date == x.triggering_event_date){
        //print(new Date(x.triggering_event_date)+" curr ");
        
        triggerStatusObjDay[x.status] = (triggerStatusObjDay[x.status] == undefined ? 0 : triggerStatusObjDay[x.status]) +1;
        
    }
    // for one week graph
    if(new Date(x.triggering_event_date) >= weekDay ){
            //print(new Date(x.triggering_event_date)+"  week");
        triggerStatusObjWeek[x.status] = (triggerStatusObjWeek[x.status] == undefined ? 0 : triggerStatusObjWeek[x.status]) +1;
    }    
    // for one month graph
    if(new Date(x.triggering_event_date) >= monthDay){
            //print(new Date(x.triggering_event_date)+"  month");
        triggerStatusObjMonth[x.status] = (triggerStatusObjMonth[x.status] == undefined ? 0 : triggerStatusObjMonth[x.status]) +1;
    }
    // for one YTD graph
    if(new Date(x.triggering_event_date) >= yearDay ){
            //print(new Date(x.triggering_event_date)+"  YTD");
        triggerStatusObjYTD[x.status] = (triggerStatusObjYTD[x.status] == undefined ? 0 : triggerStatusObjYTD[x.status]) +1;
    }


    // for SQL
    
        // for one day graph
        if(str_date == x.triggering_event_date){
            //print(new Date(x.triggering_event_date)+" curr ");
            
            _SQLDay.push(x);
            
        }
        // for one week graph
        if(new Date(x.triggering_event_date) >= weekDay ){
                //print(new Date(x.triggering_event_date)+"  week");
            _SQLWeek.push(x);
        }    
        // for one month graph
        if(new Date(x.triggering_event_date) >= monthDay){
                //print(new Date(x.triggering_event_date)+"  month");
            _SQLMonth.push(x);
        }
        // for one YTD graph
        if(new Date(x.triggering_event_date) >= yearDay ){
               // print(new Date(x.triggering_event_date)+"  YTD");
            _SQLYTD.push(x);
        }

})
triggerStatusObjRes['Day'] = triggerStatusObjDay;
triggerStatusObjRes['Week'] = triggerStatusObjWeek;
triggerStatusObjRes['Month'] = triggerStatusObjMonth;
triggerStatusObjRes['YTD'] = triggerStatusObjYTD;

// unique OD's
var triggerOverAllOd = Array.from(new Set(_odArr));


var od = '';

function _uniquGroupArray(array){
    var od = '';
    var type = '';
    var result = {};
    var opt = [];
    var i=0;
    var arr_Length = array.length;
    array.forEach(function(x){
         if(od != x.od ){
            if(i==0){
                result[x.type] = x.value;    
                opt = [];
            }else{
                //print(result);
                opt.push(result);
                result = {}
                result[x.type] = x.value;    
                //result = {}
            }
         }
         else{
             if(type == x.type){
                result[x.type] += x.value; 
             }
             else{
                 result[x.type] = x.value; 
             }
                
         }
       i++;
        
        result['od'] = x.od;
        
        od = x.od;
        type = x.type;
        if(i==arr_Length){
            //print(result);
            opt.push(result);
        }   
         
    });
    return opt;
}

// get a unique od and trigger type count 
function _uniqueODTrigger(array){
    var od = '';
    var type = '';
    var result = {};
    var opt = [];
    var i=0;
     var arr_Length = array.length;
    array.forEach(function(x){
        if(od != x.od || type != x.type){
            
            if(i==0){
                result['value'] = 0;    
                opt = [];
            }else{
                //print(result);
                opt.push({
                    od:result['od'],
                    type:result['type'],
                    value:result['value'],
                    });
                result['value'] = 0;    
            }
            
            
        }
        i++;
        //print('outer');
        result['value'] += 1;
        result['od'] = x.od;
        result['type'] = x.type;
        od = x.od;
        type = x.type;
        if(i==arr_Length){
            //print(result);
            opt.push({
                     od:result['od'],
                    type:result['type'],
                    value:result['value'],
            });
            
        }
    });
    return opt;
}
// compare and arrange unique array list
function compare(a,b) {
  if (a.od < b.od)
    return -1;
  if (a.od > b.od)
    return 1;
  return 0;
}

// sort dep_date array based on departure date
function compare_depdate(a,b) {
  if (a.dep_date < b.dep_date)
    return -1;
  if (a.dep_date > b.dep_date)
    return 1;
  return 0;
}



odUser.forEach(function(x){
    _odUserAry.push(x);
    if(typeof x.fares_docs == 'object'){
           triggerOverAllFare+= x.fares_docs.length;
    }
})
var triggerOverAllTotal = _curAry.length;   

// To add work package also in the trigger type collection
TriggerType['work_package'] = db.JUP_DB_WorkPackage.find({user:user,filingstatus : 'pending'}).count();

TriggerOverall['Critical'] = triggerOverAllCritical;
TriggerOverall['Total'] = triggerOverAllTotal;
TriggerOverall['Fares'] = triggerOverAllFare;
TriggerOverall['od'] = triggerOverAllOd.length;
Trigger['overAll'] = TriggerOverall;
TriggerOd['detail'] = _uniquGroupArray(_uniqueODTrigger(_TriggerOdSub).sort(compare));
Trigger['triggerOd'] = TriggerOd;
TriggerType['detail'] = sysTriggerType;
Trigger['triggerTypes'] = TriggerType;
Trigger['triggerStatus'] = triggerStatusObjRes;



function _SQLGetElement(data,index){
    var num = 0;
    var hour = 0;
    for(num=0;num<index;num++){
        if(data[num].status.toLowerCase() == 'pending'){
            hour = 0;
        }
        else{
            if(data[num].action_date != null && data[num].action_time != null ){
                var testDate = new Date(data[num].action_date+'T'+data[num].action_time+':00Z');
                var testDate1 = new Date(data[num].triggering_event_date+'T'+data[num].triggering_event_time+':00Z');
                hour += Math.abs(testDate1-testDate) / 36e5;        
            }
            else{
                
            }
            
        }
    }
    return hour;
}


//_SQLGetElement()

function _SQLAlg(data_){
    var SQLArray = {};
    var lenOfArray = data_.length;
    var tenVal = Math.ceil(lenOfArray*.1);
    var tfVal = Math.ceil(lenOfArray*.25);
    var fiVal = Math.ceil(lenOfArray*.5);
    var sfVal = Math.ceil(lenOfArray*.75);
    var hnVal = Math.ceil(lenOfArray*1);
    SQLArray['10p'] = _SQLGetElement(data_,tenVal);
    SQLArray['25p'] = _SQLGetElement(data_,tfVal);
    SQLArray['50p'] = _SQLGetElement(data_,fiVal);
    SQLArray['75p'] = _SQLGetElement(data_,sfVal);
    SQLArray['100p'] = _SQLGetElement(data_,hnVal);
    var num = 0;
    var penCount = 0;
    for(num = 0;num<data_.length;num++){
        if(data_[num].status=='pending'){
           penCount++; 
        }
    }
    SQLArray['pending'] = penCount;
    return SQLArray;
}

// _SQLAlg(_SQLYTD)

// SQL code continue

    // for making order put pending triggers last in the array
function _orderPending(data){
    var tempArr1 = [];
    var tempArr2 = [];
    data.forEach(function(x){
        if(x.status == 'pending'){
            tempArr2.push(x);
        }
        else{
            tempArr1.push(x);
        }
    });
    var tempOrderedArray = [];
    if(tempArr1 == undefined){
        tempOrderedArray = tempArr1;
    }
    else{
        tempOrderedArray = tempArr1.concat(tempArr2);
    }
    
    return _SQLAlg(tempOrderedArray);
}



_SQLObj['Day'] = _orderPending(_SQLDay);
_SQLObj['Week'] = _orderPending(_SQLWeek);
_SQLObj['Month'] = _orderPending(_SQLMonth);
_SQLObj['Year'] = _orderPending(_SQLYTD);

// For KPI Grid
var _KPIObj = {};
var _KPIObjSubDoc = {};
var _saleRev = 0;
var _salePax = 0;
var _saleRev_1 = 0;
var _salePax_1 = 0;
var _capacity_1 = 0;
var _capacity = 0;
var _distance = 0;
var _onlyFlownPax = 0;
var _onlyFlownRev = 0;
var _monthTargetPax = 0;
var _monthTargetRev = 0;
var _monthForecastPax = 0;
var _monthForecastRev = 0;
// count for to check the number of days flown data come under the month
var count = 0;
// for agility we need competitor price change trigger. So till trigger is getting ready we keep agility as 0
var _agility = 0;
var _MarketShare = 0;
var _MarketShare_1 = 0;
// Ask Sid or Mahesh is that Except pending trigger or all the trigger of current month
var _ReviewOD = 0;

var data = db.JUP_DB_Landing_Page_.findOne({'user':user,'dep_month':Number(curr_month),'dep_year':Number(curr_year)})

var _updateDepDateData = [];

// For updating Yield and avg fare in the dep_date array based on the date
data.this_month.dep_date.forEach(function(x){
    if(new Date(x.dep_date) >= yesterDay){
        x['avgFare'] = x.sale_pax == 0 ? 0 : x.sale_revenue/x.sale_pax ;
        x['yield'] = x.sale_pax*x.distance == 0 ? 0 :x.sale_revenue/(x.sale_pax*x.distance) ;
    }
    else if(new Date(x.dep_date) < yesterDay){
        x['avgFare'] = x.flown_pax == 0 ? 0 : x.flown_revenue/x.flown_pax;
        x['yield'] = x.flown_pax*x.distance == 0 ? 0 : x.flown_revenue/(x.flown_pax*x.distance);
    }
    _updateDepDateData.push(x);
})

//=========================================================
// To update monthly target and forecast from actual collection to landing page based on the user profile

//db.getCollection('JUP_DB_Target_OD').find({'od':'DXBDOH','pos':'DXB','month':7})

var _target_pax_from_original = 0;
var _target_rev_from_original = 0;
var _target_avg_from_original = 0;
var _forecast_pax_from_original = 0;
var _forecast_rev_from_original = 0;
var _forecast_avg_from_original = 0;
var _target_pax_from_original_1 = 0;
var _target_rev_from_original_1 = 0;
var _target_avg_from_original_1 = 0;
var _forecast_pax_from_original_1 = 0;
var _forecast_rev_from_original_1 = 0;
var _forecast_avg_from_original_1 = 0;


var _target_val = db.JUP_DB_Target_OD.aggregate([
{$match:{
    //'dep_month':Number(curr_month),'dep_year':Number(curr_year)
    'month':{$eq:Number(curr_month)},
    'year':{$eq:Number(curr_year)},
    'pos':{$in:['AMM','CGP','CMB']},
    //'compartment':{$eq:'J'}
    }},
    {$group:{
        _id:'$month',
        rev : {$sum:'$revenue'},
        pax : {$sum:'$pax'},
        rev_1 : {$sum:'$revenue_1'},
        pax_1 : {$sum:'$pax_1'},
        }},
        {$project:{
            _id:1,
            rev:1,
            pax:1,
            avg:{$divide:['$rev','$pax']},
            rev_1:1,
            pax_1:1,
            avg_1:{$divide:['$rev_1','$pax_1']},
        }}
     
])
        
        
_target_val.forEach(function(x){
    _target_pax_from_original = x.pax;
    _target_rev_from_original = x.rev;
    _target_avg_from_original = x.avg;
    _target_pax_from_original_1 = x.pax_1;
    _target_rev_from_original_1 = x.rev_1;
    _target_avg_from_original_1 = x.avg_1;
})      


var _forecast_val = db.JUP_DB_Forecast_OD.aggregate([
{$match:{
    //'dep_month':Number(curr_month),'dep_year':Number(curr_year)
    'Month':{$eq:curr_month},
    'Year':{$eq:curr_year+''},
    'pos':{$in:['AMM','CGP','CMB']},
    //'compartment':{$eq:'J'}
    }},
    {$group:{
        _id:'$Month',
            rev : {$sum:'$revenue'},
            pax : {$sum:'$pax'},
            rev_1 : {$sum:'$revenue_1'},
            pax_1 : {$sum:'$pax_1'},
        }},
        {$project:{
            _id:1,
            rev:1,
            pax:1,
            avg:{$divide:['$rev','$pax']},
            rev_1:1,
            pax_1:1,
            avg_1:{$divide:['$rev_1','$pax_1']}
        }}
])
        
        
_forecast_val.forEach(function(x){
        _forecast_pax_from_original = x.pax;
        _forecast_rev_from_original = x.rev;
        _forecast_avg_from_original = x.avg;
        _forecast_pax_from_original_1 = x.pax_1;
        _forecast_rev_from_original_1 = x.rev_1;
        _forecast_avg_from_original_1 = x.avg_1;
    })    


var _market_pax_from_original = 0;
var _market_rev_from_original = 0;
var _market_size_from_original = 0;
var _market_pax_from_original_1 = 0;
var _market_rev_from_original_1 = 0;
var _market_size_from_original_1 = 0;

var _marketShareObj = db.JUP_DB_Market_Share.aggregate([
{$match:{
    //'dep_month':Number(curr_month),'dep_year':Number(curr_year)
   'month':{$eq:Number(curr_month)},
    'year':{$eq:Number(curr_year)},
    'pos':{$in:['AMM','CGP','CMB']},
    //'compartment':{$eq:'J'}
    }},
    {$group:{
        _id:'$month',
            rev : {$sum:'$revenue'},
            pax : {$sum:'$pax'},
            rev_1 : {$sum:'$revenue_1'},
            pax_1 : {$sum:'$pax_1'},
                market_size:{$sum:'$market_size'},
                market_size_1:{$sum:'$market_size_1'}
        }},
        {$project:{
            _id:1,
            rev:1,
            pax:1,
            market_size:1,
            rev_1:1,
            pax_1:1,
            market_size_1:1
        }}
])

_marketShareObj.forEach(function(x){
        _market_pax_from_original = x.pax;
        _market_rev_from_original = x.rev;
        _market_size_from_original = x.market_size;
        _market_pax_from_original_1 = x.pax_1;
        _market_rev_from_original_1 = x.rev_1;
        _market_size_from_original_1 = x.market_size_1;    
    })  

//=========================================================



/*

==================================================

calculate pax and revenue based on present and past dates ( sales & flown )
*/
data.this_month.dep_date.forEach(function(x){
    if(new Date(x.dep_date) >= yesterDay){
        print(_salePax+"    "+_salePax_1);
        _salePax += x.sale_pax;
        _saleRev += x.sale_revenue;
        _salePax_1 += x.sale_pax_1;
        _saleRev_1 += x.sale_revenue_1;
    }
    else if(new Date(x.dep_date) < yesterDay){
        print(_salePax+"    "+_salePax_1);
        _salePax += x.flown_pax;
        _saleRev += x.flown_revenue;
        _salePax_1 += x.flown_pax_1;
        _saleRev_1 += x.flown_revenue_1;
        _onlyFlownPax += x.flown_pax;
        _onlyFlownRev += x.flown_revenue;
    count += 1; 
    }
})
//==================================================

_capacity = data.this_month.capacity;
_capacity_1 = data.this_month.capacity_1;
_distance = data.this_month.distance;
_MarketShare = _market_pax_from_original/(_market_size_from_original*100);
_MarketShare_1 = _market_pax_from_original_1/((_market_size_from_original_1==0?1:_market_size_from_original_1)*100);
var _thisMonthFromLanding = data.this_month;

_monthTargetPax = _target_pax_from_original;
_monthTargetRev = _target_rev_from_original;
_monthForecastPax = _forecast_pax_from_original;
_monthForecastRev = _forecast_rev_from_original;

_MarketShare = _market_size_from_original == 0 ? 0 : (_market_pax_from_original/_market_size_from_original)*100;
var _Target = db.JUP_DB_Analyst_Target.findOne({'user':user});
var _TargetRevenue = _Target.kpi.revenue;
var _TargetPax = _Target.kpi.ticket;
var _TargetAgility = _Target.kpi.agility;
var _TargetFMS = _Target.kpi.ms_growth;
var _TargetRevOfTopOD = _Target.kpi.review_od;
var _TargetCurrency = _Target.kpi.revenue_unit;
var _agility_unit = _Target.kpi.agility_unit;
var ticket_unit = _Target.kpi.ticket_unit;
var ms_growth_unit = _Target.kpi.ms_growth_unit ;
var review_od_unit = _Target.kpi.review_od_unit;


// Tiles are start from here
var _tiles = {};
var _Fares = {};

// fares
db.JUP_DB_Manual_Triggers_Module.aggregate([
{$match:{
    'dep_year':{ $eq: Number(curr_year) },
    'dep_month':{ $eq: Number(curr_month) }, 
    'pos.City':{ $in: listOfPos }
}},
{$unwind:'$sale_farebasis'},
{
  $group:{
      _id:{
            dep_year:'$dep_year',
             dep_month:'$dep_month', 
             fare_basis:'$sale_farebasis.fare_basis'
          },
          flagValue:{$sum:'$sale_farebasis.effectivity_calc'}
      }  
},
{$project:{
        dep_year:'$_id.dep_year',
         dep_month:'$_id.dep_month', 
         fare_basis:'$_id.fare_basis',
         flagValue:'$flagValue'
}},
{
  $group:{
      _id:{
            dep_year:'$dep_year',
             dep_month:'$dep_month', 
//             fare_basis:'$sale_farebasis.fare_basis'
          },
          totalFare:{$sum:1},
          effectiveFare:{$sum:{$cond:[
              {$gte:['$flagValue',0]},
              1,
              0
              ]}},
          inEffectiveFare:{$sum:{$cond:[
              {$lt:['$flagValue',0]},
              1,
              0
              ]}}
      }  
},
{$project:{
        dep_year:'$_id.dep_year',
         dep_month:'$_id.dep_month', 
         fare_basis:'$_id.fare_basis',
         totalFare:'$totalFare',
         effectiveFare:'$effectiveFare',
        inEffectiveFare:'$inEffectiveFare'
}},
{$out:'temp_Eff_Fare'}
])

var _resObj = db.temp_Eff_Fare.findOne()
db.temp_Eff_Fare.drop();
_Fares['totalFare'] = _resObj.totalFare;
_Fares['effectiveFare'] = _resObj.effectiveFare;
_Fares['inEffectiveFare'] = _resObj.inEffectiveFare;
_tiles['Fares'] = _Fares;
// significant and none significant OD
var totArray = [];
var sigArray = [];
var NoneSigArray = [];
_thisMonthFromLanding.dep_date.forEach(function(opendata){
    totArray = totArray.concat(opendata.totalOD);
    sigArray = sigArray.concat(opendata.signOD);
    NoneSigArray = NoneSigArray.concat(opendata.nonSignOD);
})

// function for removing null element from array
Array.prototype.remove = function() {
    var what, a = arguments, L = a.length, ax;
    while (L && this.length) {
        what = a[--L];
        while ((ax = this.indexOf(what)) !== -1) {
            this.splice(ax, 1);



        }
    }
    return this;
};

// get unique od from all the arrays 
totArray = Array.from(new Set(totArray)).remove(null);
sigArray = Array.from(new Set(sigArray)).remove(null);
NoneSigArray = Array.from(new Set(NoneSigArray)).remove(null);

// for creating and assingning of all unique sig and non sig count and array into the landing page collection
var _signOD = {};
_signOD['totalOD'] = totArray; 
_signOD['sigOD'] = sigArray;
_signOD['noneSigOD'] = NoneSigArray;
_signOD['totalOD_count'] = totArray.length; 
_signOD['sigOD_count'] = sigArray.length;
_signOD['noneSigOD_count'] = NoneSigArray.length;

// add sign embedded document into tiles doc
_tiles['Sig'] = _signOD;

// function for Yield formula 
function yield_(revenue,pax,distance){
   if(distance == 0 || pax == 0) {
     return 0;
   }
   else{
     return (revenue/(pax*distance))*100;
   }
    
}

// function for VLYR formula 
function vlyr(pax,pax_,capacity,capacity_){
     if(capacity!= 0 && capacity_!= 0){
        return ((pax*(capacity/capacity_))-pax_)/pax_;
    }
    else if(pax!= 0 && pax_!= 0){
        return ((pax-pax_)/pax_);
    }
    else{
        return 0;
    }
}


//_salePax_1 = data.this_month.sale_pax_1; 
_capacity_1 = data.this_month.capacity_1;

// if capacity doesn't have any data so give a same value by default
if (_capacity_1 == undefined || _capacity_1 == 0 ){
     _capacity_1 = _capacity;
    }

// to compute the vtgt and vlyr    
var _paxVLYR = vlyr(_salePax, _salePax_1, _capacity, _capacity_1)*100;
var _revVLYR = vlyr(_saleRev, _saleRev_1, _capacity, _capacity_1)*100;
var _yieldVLYR = (yield_(_saleRev,_salePax, _distance)-yield_(_saleRev_1,_salePax_1, _distance))/(yield_(_saleRev_1,_salePax_1, _distance)==0?1:yield_(_saleRev_1,_salePax_1, _distance))*100;
var _avgVLYR = (_saleRev ==0 &&_salePax ==0 )?0: ((_saleRev/_salePax)-(_saleRev_1/_salePax_1))/(_saleRev_1/_salePax_1)*100;
var _MarketShareVLYR = _MarketShare - _MarketShare_1;

//=====================VTGT=================================
// get number of days in month
function daysInMonth(month,year) {
    return new Date(year, month, 0).getDate();
}

function VTGT_pureFuture(_monthTargetPax, _monthForecastPax){
    return((_monthForecastPax) - _monthTargetPax) / (_monthTargetPax)
}    

function VTGT_purePast(_monthTargetPax, _monthFlownPax){
    return(_monthFlownPax - _monthTargetPax) / (_monthTargetPax)
}    

function VTGT_mix(_monthTargetPax_, _onlyFlownPax_, _monthForecastPax_, count_ ){
    if(_monthTargetPax_!=0){
        return ((_onlyFlownPax_ + (_monthForecastPax_/daysInMonth(curr_month,curr_year))*(daysInMonth(curr_month,curr_year)-count_) -_monthTargetPax) / _monthTargetPax);    
    }
    else{
        return 0;    
    }
}        

// 

//==========================================================    

//_onlyFlownPax
// Get logic from ashok and write logic for VTGT    
var _paxVTGT = 0;
var _revVTGT = 0;
var _yieldVTGT = 0;
var _avgVTGT = 0;
var _MarketShareVTGT = 0; 

// check which function has to choose for running VTGT based on date
if(new Date().getMonth()+1 == Number(curr_month)){
    _paxVTGT = VTGT_mix(_monthTargetPax,_onlyFlownPax,_monthForecastPax,count)*100;   
    _revVTGT = VTGT_mix(_monthTargetRev ,_onlyFlownRev,_monthForecastRev,count)*100;   
    _yieldVTGT = _revVTGT/(_paxVTGT*_distance)*100;   
    _avgVTGT = _revVTGT/_paxVTGT;
}
else if(new Date().getMonth()+1 > Number(curr_month)){
    
    _paxVTGT = VTGT_pureFuture(_monthTargetPax,_monthForecastPax)*100;   
    _revVTGT = VTGT_pureFuture(_monthTargetRev ,_monthForecastRev)*100;   
    _yieldVTGT = _revVTGT/(_paxVTGT*_distance)*100;   
    _avgVTGT = _revVTGT/_paxVTGT;
}
else if(new Date().getMonth()+1 < Number(curr_month)){
    //_monthTargetPax, _monthFlownPax
    _paxVTGT = VTGT_purePast(_monthTargetPax,_onlyFlownPax)*100;   
    _revVTGT =VTGT_purePast(_monthTargetRev,_onlyFlownRev)*100;   
    _yieldVTGT = _revVTGT/(_paxVTGT*_distance)*100;   
    _avgVTGT = _revVTGT/_paxVTGT;
}

var paxt = {};
var revt = {};
var yieldt = {};
var avgt = {};
var markett = {};

// vtgt and vlyr assigning embedded documents
paxt['vlyr'] = _paxVLYR;
paxt['vtgt'] = _paxVTGT;
paxt['value'] = _salePax;
paxt['value_1'] = _salePax_1;
revt['vlyr'] = _revVLYR;
revt['value'] = _saleRev;
revt['value_1'] = _saleRev_1;
yieldt['vlyr'] = _yieldVLYR;
yieldt['value'] = yield_(_saleRev,_salePax, _distance);
avgt['vlyr'] = _avgVLYR;
avgt['value'] = (_saleRev ==0 &&_salePax ==0 )?0: _saleRev/_salePax ;
markett['vlyr'] = _MarketShareVLYR;
markett['value'] = _MarketShare;
revt['vtgt'] = _revVTGT;
yieldt['vtgt'] = _yieldVTGT ;
avgt['vtgt'] = _avgVTGT;
markett['vtgt'] = _MarketShareVTGT;

// for review of top od from current month actioned triggers
var __totalTrigger = _SQLMonth.length;
var __percentageOfComp = 0;
var __processedTriggerWithinOneDay = 0;
_SQLMonth.forEach(function(x){
    
//     if(new Date(x.triggering_event_date) == new Date(x.action_date)){
//         __processedTriggerWithinOneDay +=1
//     }
    
    // check what are all Trigger processed in the same day 
    // just check two date's As per Mahesh and Sid's concept[05-07-2017]
    if(x.triggering_event_date == x.action_date){
        __processedTriggerWithinOneDay +=1
    }
})

// percentage of completion trigger with in one day
if(__totalTrigger!=0){
    __percentageOfComp = (__processedTriggerWithinOneDay/__totalTrigger)*100;
}
// assigning KPI value  
_KPIObj['Revenue'] ={'current':_saleRev,'target':_monthTargetRev} ;
_KPIObj['Forward_Booking'] ={'current':_paxVLYR,'target':_TargetPax,'unit':ticket_unit} ;
_KPIObj['Market_Share_Growth'] ={'current':_MarketShareVLYR,'target':_TargetFMS,'unit':ms_growth_unit} ;
_KPIObj['Agility'] ={'current':_agility,'target':_TargetAgility, 'unit':_agility_unit } ;
_KPIObj['Review_of_Top_OD'] ={'current':__percentageOfComp,'target':_TargetRevOfTopOD,'unit':review_od_unit} ;


// add all vtgt and vlyr into tiles sub document 
_tiles['pax'] = paxt;
_tiles['revenue'] = revt;
_tiles['market'] = markett;
_tiles['yield'] = yieldt;
_tiles['avgFare'] = avgt;

// insert the whole data 
db.db.JUP_DB_Landing_Page_.find({'user':user,'dep_month':Number(curr_month),'dep_year':Number(curr_year)}).forEach(function(x){
    db.db.JUP_DB_Landing_Page_30.insert(x);
})

// Update landing table
db.JUP_DB_Landing_Page_30.update({
        'user':user,'dep_month':Number(curr_month),'dep_year':Number(curr_year)
    },
    {
        $set:{
            'currency':_TargetCurrency,
            'this_month.dep_date':_updateDepDateData.sort(compare_depdate),
            'this_month.target_pax':_target_pax_from_original ,
            'this_month.target_avgFare':_target_avg_from_original,
            'this_month.target_revenue':_target_rev_from_original,
            'this_month.target_pax_1':_target_pax_from_original_1,
            'this_month.target_avgFare_1':_target_avg_from_original_1,
            'this_month.target_revenue_1':_target_rev_from_original_1,
            'this_month.forecast_pax':_forecast_pax_from_original,
            'this_month.forecast_avgFare':_forecast_avg_from_original,
            'this_month.forecast_revenue':_forecast_rev_from_original,
            'this_month.forecast_pax_1':_forecast_pax_from_original_1,
            'this_month.forecast_avgFare_1':_forecast_rev_from_original_1 ,
            'this_month.forecast_revenue_1':_forecast_avg_from_original_1,
            'this_month.market.pax':_market_pax_from_original,
            'this_month.market.pax_1':_market_pax_from_original_1,
            'this_month.market.market_size':_market_size_from_original,
            'this_month.market.market_size_1':_market_size_from_original_1,
            Trigger : Trigger,
            SQL : _SQLObj,
            KPI : _KPIObj,
            Tiles : _tiles
            }
    },
    {
        upsert:true
    });
