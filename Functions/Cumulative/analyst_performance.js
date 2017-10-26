

// To load analyst 
var user = 'Europa';
var userDetail = db.JUP_DB_User.findOne({'name':user});
var listOfPos = userDetail.list_of_pos ;
var cursor = db.getCollection('JUP_DB_Workflow').find({'pos':{$in:listOfPos}})


/*'''''''''''''''''''''''''''''''''''''''''''''''*/
// For give a demo to vinassa in the month of march

var curDay = new Date('2017-04-30');

var yesterDay = new Date(curDay);
yesterDay.setDate(curDay.getDate() - 1);

curDay.setDate(curDay.getDate()); // <-- add this to make it "yesterday"
var weekDay = new Date(curDay);
weekDay.setDate(weekDay.getDate() - 8); // <-- add this to make it "one week before"
var monthDay = new Date(curDay);
monthDay.setDate(monthDay.getDate() - 30); // <-- add this to make it "one month back"

var yearDay = new Date(curDay.getFullYear()+'-01-01');

var curr_date = curDay.getDate(); // <-- don't subtract 1 anymore
var curr_month = ("0" + (curDay.getMonth()+1)).slice(-2);// global variable have to chage this to run like list of user 


// For testing give current month as month -1
//var curr_month = ("0" + (curDay.getMonth() )).slice(-2);

var curr_year = curDay.getFullYear();
var str_date = curr_year +"-"+ curr_month+"-"+curr_date


var SQLArray = [];
var _ManualSQLObj = {};
var _ManualSQLDay = [];
var _ManualSQLWeek = [];
var _ManualSQLMonth = [];
var _ManualSQLYTD = [];
var _SQLRes = {};

//var SQLArray = [];
var _SystemSQLObj = {};
var _SystemSQLDay = [];
var _SystemSQLWeek = [];
var _SystemSQLMonth = [];
var _SystemSQLYTD = [];


cursor.forEach(function(x){
    
    if(x.status.toLowerCase() == 'pending'){

    // for SQL
        if(x.type_of_trigger.toLowerCase() == 'system'){
            // for one day graph
            if(str_date == x.triggering_event_date){
            //print(new Date(x.triggering_event_date)+" curr ");
            
            _SystemSQLDay.push(x);
            
            }
            // for one week graph
            if(new Date(x.triggering_event_date) >= weekDay ){
                    //print(new Date(x.triggering_event_date)+"  week");
                _SystemSQLWeek.push(x);
            }    
            // for one month graph
            if(new Date(x.triggering_event_date) >= monthDay){
                    //print(new Date(x.triggering_event_date)+"  month");
                _SystemSQLMonth.push(x);
            }
            // for one YTD graph
            if(new Date(x.triggering_event_date) >= yearDay ){
                   // print(new Date(x.triggering_event_date)+"  YTD");
                _SystemSQLYTD.push(x);
            }

        }
        else if(x.type_of_trigger.toLowerCase() == 'manual'){
            // for one day graph
            if(str_date == x.triggering_event_date){
                //print(new Date(x.triggering_event_date)+" curr ");
                
                _ManualSQLDay.push(x);
                
            }
            // for one week graph
            if(new Date(x.triggering_event_date) >= weekDay ){
                    //print(new Date(x.triggering_event_date)+"  week");
                _ManualSQLWeek.push(x);
            }    
            // for one month graph
            if(new Date(x.triggering_event_date) >= monthDay){
                    //print(new Date(x.triggering_event_date)+"  month");
                _ManualSQLMonth.push(x);
            }
            // for one YTD graph
            if(new Date(x.triggering_event_date) >= yearDay ){
                   // print(new Date(x.triggering_event_date)+"  YTD");
                _ManualSQLYTD.push(x);
            }

        }
        
    }
})



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



_SystemSQLObj['Day'] = _orderPending(_SystemSQLDay);
_SystemSQLObj['Week'] = _orderPending(_SystemSQLWeek);
_SystemSQLObj['Month'] = _orderPending(_SystemSQLMonth);
_SystemSQLObj['Year'] = _orderPending(_SystemSQLYTD);

_ManualSQLObj['Day'] = _orderPending(_ManualSQLDay);
_ManualSQLObj['Week'] = _orderPending(_ManualSQLWeek);
_ManualSQLObj['Month'] = _orderPending(_ManualSQLMonth);
_ManualSQLObj['Year'] = _orderPending(_ManualSQLYTD)
