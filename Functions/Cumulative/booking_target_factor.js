//db.getCollection('JUP_DB_Booking_DepDate').find({})


db.JUP_DB_Booking_DepDate.aggregate([
                { "$lookup": {
                    "from": "JUP_DB_Target_OD",
                    "localField": "combine_month",
                    "foreignField": "combine_month",
                    "as": "collection4_doc"
                }},
				{ "$lookup": {
                    "from": "JUP_DB_Sales",
                    "localField": "combine_month",
                    "foreignField": "combine_month",
                    "as": "sale"
                }},
                {"$group":{
                        "_id":{combine_month:"$combine_month",
				//				dep_date:"$dep_date",
								region:"$region",
								country:"$country",
								pos:"$pos",
								origin:"$origin",
								destination:"$destination",
								compartment:"$compartment",
								target_pax: "$collection4_doc.pax",
								sale_pax : "$sale.pax",
								sale_pax_1 : "$sale.pax_1",
                            },
								pax:{ $sum: "$pax" },
								pax_1:{ $sum: "$pax_1" },
								ticket:{ $sum: "$ticket" },
                      }
                   },
                { $unwind: { path: "$_id.target_pax", preserveNullAndEmptyArrays: true } },
				{ $unwind: { path: "$_id.sale_pax", preserveNullAndEmptyArrays: true } },
                { $unwind: { path: "$_id.sale_pax_1", preserveNullAndEmptyArrays: true } },
                { "$project": {
             //       "combine_month":1,
                 //   "_id":"$_id",
                    _id:0,
                    "combine_month":"$_id.combine_month",
                    "region" : "$_id.region",
                    "country" : "$_id.country",
                    "pos" : "$_id.pos",
                    "origin" : "$_id.origin",
                    "destination" : "$_id.destination",
                    "compartment" : "$_id.compartment",  
                    "pax" : "$pax",
                    "ticket" : "$ticket",
                    "pax_1":"$pax_1",
                    "target_pax": "$_id.target_pax",
                    "sale_pax":"$_id.sale_pax",
					"sale_pax_1":"$_id.sale_pax_1"
                }},
				{"$group":{
                        "_id":{combine_month:"$combine_month",
				//				dep_date:"$dep_date",
								region:"$region",
								country:"$country",
								pos:"$pos",
								origin:"$origin",
								destination:"$destination",
								compartment:"$compartment",
                            },
								target_pax:{ $sum: "$target_pax"},
								sale_pax :{ $sum: "$sale_pax"},
								sale_pax_1 :{ $sum: "$sale_pax_1"},
								pax:{ $sum: "$pax" },
								pax_1:{ $sum: "$pax_1" },
								ticket:{ $sum: "$ticket" },
                      }
                   },
				   { "$project": {
             //       "combine_month":1,
                 //   "_id":"$_id",
                    _id:0,
                    "combine_month":"$_id.combine_month",
                    "region" : "$_id.region",
                    "country" : "$_id.country",
                    "pos" : "$_id.pos",
                    "origin" : "$_id.origin",
                    "destination" : "$_id.destination",
                    "compartment" : "$_id.compartment",  
                    "pax" : "$pax",
                    "ticket" : "$ticket",
                    "pax_1":"$pax_1",
                    "target_pax": "$target_pax",
                    "sale_pax":"$sale_pax",
					"sale_pax_1":"$sale_pax_1",
					"factor":{
                            '$cond':
                                {
                                    'if': {'$gt': ['$sale_pax_1', 0]},
                                    'then':
                                        {
                                            '$cond':
                                                {
                                                    'if': {'$gt': ['$pax_1', 0]},
                                                    'then': {'$divide': ['$pax_1','$sale_pax_1']},
                                                    'else':0
                                                }
                                        },
                                    'else':0
                                }
                        }
                }},
				{ "$project": {
             //       "combine_month":1,
                 //   "_id":"$_id",
                    _id:0,
                    "combine_month":"$combine_month",
                    "region" : "$region",
                    "country" : "$country",
                    "pos" : "$pos",
                    "origin" : "$origin",
                    "destination" : "$destination",
                    "compartment" : "$compartment",  
                    "pax" : "$pax",
                    "ticket" : "$ticket",
                    "pax_1":"$pax_1",
                    "target_pax": "$target_pax",
                    "sale_pax":"$sale_pax",
					"sale_pax_1":"$sale_pax_1",
					"factor":"$factor",
					"forecast_booking":{
                            '$cond':
                                {
                                    'if': {'$gt': ['$target_pax', 0]},
                                    'then':
                                        {
                                            '$cond':
                                                {
                                                    'if': {'$gt': ['$factor', 0]},
                                                    'then': {'$multiply': ['$factor','$factor']},
                                                    'else':0
                                                }
                                        },
                                    'else':0
                                }
                        },
                }},
            ])