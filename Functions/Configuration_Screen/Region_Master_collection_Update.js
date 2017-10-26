//db.getCollection('JUP_DB_Region_Master_Config').find({})

// THis function is going to add get some custome data 
db.JUP_DB_Region_Master_Config.aggregate([
{$group:{
	_id:{
		"network" : "$network",
		"POS_CD" : null,
		"POS_NAME_TX" : null,
		"IS_NEW" : null,
		"COUNTRY_NAME_TX" : null,
		"COUNTRY_CD" : null,
		"POS_TYPE" : null,
		"Region" : null
	},
	
	
}},
{$project:{
		_id:0,
		"network":"$_id.network",
		"POS_CD" : "$_id.POS_CD",
		"POS_NAME_TX" : "$_id.POS_NAME_TX",
		"IS_NEW" : "$_id.IS_NEW",
		"COUNTRY_NAME_TX" : "$_id.COUNTRY_NAME_TX",
		"COUNTRY_CD" : "$_id.COUNTRY_CD",
		"POS_TYPE" : "$_id.POS_TYPE",
		"Region" : "$_id.Region",
		"POD" : "$_id.network",
		"CCRN":{$split:[{$concat:["$_id.network"]},',']}
}},
{$out:"Temp_Collection_Murugan"}

])