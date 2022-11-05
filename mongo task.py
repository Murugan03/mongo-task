import pymongo
import json

'''client = pymongo.MongoClient('mongodb://localhost:27017')
db = client["Directory"]
information = db["data"]
data = [
    {
        "_id": 0,
        "First_Name": "Radhika",
        "Last_Name": "Sharma",
        "Age": 26,
        "e_mail": "radhika_sharma.123@gmail.com",
        "phone": "9000012345",
        "place": "Bangalore"
    },
    {
        "_id": 1,
        "First_Name": "Rachel",
        "Last_Name": "Christopher",
        "Age": 27,
        "e_mail": "Rachel_Christopher.123@gmail.com",
        "phone": "9000054321",
        "place": "Chennai"
    },
    {
        "_id": 2,
        "First_Name": "Fathima",
        "Last_Name": "Sheik",
        "Age": 24,
        "e_mail": "Fathima_Sheik.123@gmail.com",
        "phone": "9000054321",
        "place": "Mumbai"
    },
    {
        "_id": 3,
        "First_Name": "Murugan",
        "Last_Name": "Sandy",
        "Age": 25,
        "e_mail": "murugans2802@gmail.com",
        "phone": "9025009932",
        "place": "Tiruvannamalai"
    },
    {
        "_id": 4,
        "First_Name": "Dhayalan",
        "Last_Name": "Kumar",
        "Age": 26,
        "e_mail": "dhayalan1996@gmail.com",
        "phone": "9344346177",
        "place": "Delhi"
    }
]

# inserting the data into collection
information.insert_many(data)

# query to find the records
for i in information.find():
    print(i)

# Modifies the records, use the update_one() method
information.update_one({"First_Name": "Murugan"}, {"$set": {"Last_Name": "Siva", "Age": 26}})

# deleting the record using delete_one() method
information.delete_one({"place": "Mumbai"})'''

# student database

client = pymongo.MongoClient('mongodb://localhost:27017/')
db = client["guvi"]
information = db["data"]
f = open("C:/Users/ELCOT/Desktop/Guvi/students.json")
data = json.load(f)

# student who scored maximum mark in all categories
for i in information.aggregate([{"$project": {"name": 1, "total_marks": {"$max": "$scores.score"}}},
                                {"$sort": {"total_marks": -1}}, {"$limit": 1}]):
    print(i)

# students who scored below 40 in exam
for i in information.aggregate([{"$unwind": "$scores"}, {"$match": {"scores.type": "exam"}},
                                {
                                    "$match": {"scores.score": {"$lt": 40}}
                                }]):
    print(i)
