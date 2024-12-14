from fastapi import FastAPI, HTTPException, Query
from typing import Optional
#from motor.motor_asyncio import AsyncIOMotorClient
from collections import Counter
from textblob import TextBlob as tb
import os
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

connection  = os.getenv("MONGO_SECRET")
client = MongoClient(connection, server_api=ServerApi('1'))
# client = AsyncIOMotorClient("mongodb://localhost:27017")
db = client.mydatabase
collection = db.messages

app = FastAPI()



# MongoDB connection setup here
# Your code to connect to MongoDB
@app.get("/")
def root():
    return {"message": "Welcome to the FastAPI REST API!"}

@app.get("/add_message")
def add_message(message: str, subject: Optional[str] = None,
class_name: Optional[str] = None):
# Your code to store the message in MongoDB
    analysis  = tb(message)
    sentiment = analysis.sentiment.polarity
    sentiment = "positive" if sentiment>=0 else "negative"
    new_message = {"message": message, 
                   "subject": subject, 
                   "class_name":class_name,
                   "sentiment": sentiment
                   }
    collection.insert_one(new_message)
    return {"status": "Message added successfully!"}

@app.get("/messages")
def get_messages():
# Your code to fetch all messages from MongoDB
    messages =  collection.find().to_list(None)
    for message in messages:
        message["_id"] = str(message["_id"]) 
    return messages

@app.get("/calcS")
def apply_sentiment():
    messages =  collection.find().to_list(None)
    
    for message in messages:
        #message["_id"] = str(message["_id"]) 
        analysis  = tb(message['message'])
        sentiment = analysis.sentiment.polarity
        sentiment = "positive" if sentiment>=0 else "negative"
        collection.update_one({"_id":message["_id"]}, {"$set":{"sentiment": sentiment}})
    return {"Status": "Updated successfully"}

@app.get("/analyze")
def analyze(group_by: Optional[str] = None):
# Your code to analyze data and group by parameter
    if group_by:
        # Aggregate messages grouped by the specified parameter (class or subject)
        pipeline = [
            {"$group": {
                "_id": f"${group_by}",
                "sentiment_count": {"$push": "$sentiment"}
            }}
        ]
        grouped_data =  collection.aggregate(pipeline).to_list(None)

        # Calculate the mode sentiment for each group
        result = []
        for group in grouped_data:
            sentiment_counter = Counter(group["sentiment_count"])
            mode_sentiment = sentiment_counter.most_common(1)[0][0]
            result.append({
                group_by: group["_id"],
                "mode_sentiment": mode_sentiment
            })

        return result
    else:
        # Fetch all sentiments and calculate the mode sentiment
        messages =  collection.find().to_list(None)
        sentiments = [message["sentiment"] for message in messages]
        if sentiments:
            sentiment_counter = Counter(sentiments)
            mode_sentiment = sentiment_counter.most_common(1)[0][0]
            return {"mode_sentiment": mode_sentiment}
        else:
            return {"error": "No messages available for analysis"}

