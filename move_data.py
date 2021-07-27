import datetime, pymongo, os, shutil, yaml
from bson import ObjectId
#from segaggregate import start_re_id


def watch():
    if not os.path.exists('to_be_analyzed'):
        os.makedirs('to_be_analyzed')

    with open('config.yaml') as config:
        config = yaml.safe_load(config)

    collection_db = config["re_id_db"]

    myclient = pymongo.MongoClient("mongodb://localhost:27017/")

    mydb = myclient["re_id"]
    mycol = mydb[collection_db]

    #while True:
    for data in mycol.find({"data_moved_status":False}):
        image_dir = str(data["_id"])
        #print(image_dir)
        
        path = os.path.join("to_be_analyzed", image_dir)
        #print(path)

        if not os.path.exists(path):
            os.makedirs(path)
        
        image_list = data["imagename"]

        for image in image_list:
            img_path = os.path.join(path, image)
            if not os.path.exists(img_path):
                image_path = os.path.join("object", image)
                shutil.copy(image_path, path)
                print("moved image ", image)
            else:
                print("already exists ", image)
                continue

        mycol.update_one({"_id": ObjectId(image_dir)}, { "$set" : { "data_moved_status" : True} })

watch()