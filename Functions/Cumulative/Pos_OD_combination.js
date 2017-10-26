// Script for getting Pos and OD combination


db.getCollection('JUP_DB_Region_Master').find({}).forEach(function(x){
    var pos_city = x.POS_CD;
    var pos_country = x.COUNTRY_CD;
    var pos_region = x.Region;
    var pos_cluster = x.Cluster;
    var pos_network = 'Network';
    var pos = {};
    pos['City'] = x.POS_CD;
    pos['Country'] = x.COUNTRY_CD;
    pos['Region'] = x.Region;
    pos['Cluster'] = x.Cluster;
	pos['Network'] = pos_network;
    db.JUP_DB_Airline_OD.find({'Airline':'FZ'}).forEach(function(od){
        var origin = od.OD.substring(0,3);
        var destination = od.OD.substring(3,6);
        var origin_list = db.getCollection('JUP_DB_Region_Master').findOne({POS_CD:origin})
		var destination_list = db.getCollection('JUP_DB_Region_Master').findOne({POS_CD:destination})
        print(pos_city+"  "+origin +" "+destination+"  "+ origin_list.Cluster+"  "+destination_list.Cluster  );
        var Origin_ = {};
        var destination_ = {};
        Origin_['City'] = origin_list.POS_CD;
        Origin_['Country'] = origin_list.COUNTRY_CD;
	    Origin_['Region'] = origin_list.Region;
   		Origin_['Cluster'] = origin_list.Cluster;
		Origin_['Network'] = "Network";
		destination_['City'] = destination_list.POS_CD;
        destination_['Country'] = destination_list.COUNTRY_CD;
	    destination_['Region'] = destination_list.Region;
   		destination_['Cluster'] = destination_list.Cluster;
		destination_['Network'] = "Network";
		compartment = {}
		compartment['all'] = 'all';
		compartment['compartment'] = 'Y';
        db.JUP_DB_Pos_OD_Mapping.insert({
        	'pos':pos,'origin':Origin_,'destination':destination_,'compartment':compartment
        });

		compartment['compartment'] = 'J';        
        db.JUP_DB_Pos_OD_Mapping.insert({
        	'pos':pos,'origin':Origin_,'destination':destination_,'compartment':compartment
        });

        })
        
    })
    
    