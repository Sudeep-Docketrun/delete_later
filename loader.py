import requests
from flask import *  
import pymongo, yaml
from ast import literal_eval
import json, bisect, datetime
from bson import ObjectId


app = Flask(__name__)  

myclient = pymongo.MongoClient("mongodb://localhost:27017/")

with open('config.yaml') as config:
    config = yaml.safe_load(config)

collection_db = config["re_id_db"]

mydb = myclient["re_id"]
mycol = mydb[collection_db]

@app.route('/analytics/dump_data/object',methods = ['POST', 'GET'])  
def print_data(): 
    ret = {"result": False,"output":"An unexpected error has occurred, please try again later"}
    count = 0
    if request.method == 'POST':  
        data = request.json
        insert_data = {}
        run_time = data['appruntime']
        device_location = data['device_location']
        camera_id = data['cameraid']
        tracking_id = data['trackingid']

        if isinstance(data["timestamp"],str):
            data["timestamp"] = datetime.datetime.strptime(data["timestamp"], '%Y-%m-%d %H:%M:%S')

        try:
            existing_data = mycol.find_one({"appruntime":run_time, "device_location":device_location, "cameraid":camera_id, "trackingid":tracking_id})
            if bool(existing_data):
                ticket_nos = existing_data["ticketno"]
                id = existing_data['_id']

                if data["ticketno"] not in existing_data["ticketno"]:
                    idx_newdata = bisect.bisect(existing_data["timestamp"],data["timestamp"]) # Get index where the element has to be placed in that array (insert data in sorted form by time) 
                    for key, val in data.items():
                        if key not in ["appruntime","cameraid","analyticstypeid","analyticstype","ip_address","objectname","deviceid","device_location","trackingid"]: #values not to make changes are ignored here
                            if isinstance(val,dict):   #classification {type: employee,gender :male} change it to {type: [employee],gender :[male]}
                                for subkey, subval in val.items():
                                    val[subkey].insert(idx_newdata,subval)  #convert sub values to array
                                existing_data[key] = val
                            else:
                                existing_data[key].insert(idx_newdata,val)
                                #print("appended", existing_data[key])

                #print(existing_data, "\n") 

                insert_data["data_moved"] = False
                insert_data["re_id_status"] = False            
        
                x = mycol.update_one({"_id": ObjectId(id)}, {'$set':existing_data})

                ret["output"] = "Already dumped.!!"
                ret["result"] = True

            else:
                for key, val in data.items():
                    if key not in ["appruntime","cameraid","analyticstypeid","analyticstype","ip_address","objectname","deviceid","device_location","trackingid"]: #values not to make changes are ignored here
                        if isinstance(val,dict):   #classification {type: employee,gender :male} change it to {type: [employee],gender :[male]}
                            for subkey, subval in val.items():
                                val[subkey] = [subval]  #convert sub values to array
                            insert_data[key] = val
                        else:
                            insert_data[key] = [val]
                    else:
                        insert_data[key] = val

                insert_data["data_moved_status"] = False
                insert_data["re_id_status"] = False

                x = mycol.insert_one(insert_data)

                ret["output"] = "successfully dumped.!"
                ret["result"] = True

        except Exception as e:
            print(e)
            ret["output"] = "Oops something went wrong.!"
            ret["result"] = False

        return jsonify(ret) 
   
if __name__ == '__main__':  
   app.run(debug = True, port=8080)  

"""def start_app():
    app.run(debug = False, port=8080)"""