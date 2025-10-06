import os
from pymongo import MongoClient, ReturnDocument
from datetime import datetime

MONGO_URI = os.environ.get("MONGO_URI")
client = MongoClient(MONGO_URI)
db = client['frenzy_bot_db']

# Collections
jobs_collection = db['jobs']
users_collection = db['users']
stats_collection = db['stats']
settings_collection = db['settings']

# --- Settings ---
def get_settings():
    return settings_collection.find_one({"_id": "config"})

def set_supergroup(chat_id, topic_ids):
    settings_collection.update_one(
        {"_id": "config"},
        {"$set": {"supergroup_id": chat_id, "topic_ids": topic_ids}},
        upsert=True
    )

# --- Job Management ---
def add_job(user_id, media_url, referer_url):
    jobs_collection.insert_one({"user_id": user_id, "media_url": media_url, "referer_url": referer_url, "status": "pending", "created_at": datetime.utcnow()})

def get_job():
    return jobs_collection.find_one_and_update({"status": "pending"}, {"$set": {"status": "processing"}}, return_document=ReturnDocument.AFTER)

def complete_job(job_id):
    jobs_collection.delete_one({"_id": job_id})

def get_pending_jobs(user_id):
    return list(jobs_collection.find({"user_id": user_id, "status": "pending"}))

def clear_pending_jobs(user_id):
    return jobs_collection.delete_many({"user_id": user_id, "status": "pending"}).deleted_count

# --- User Config ---
def get_user_config(user_id):
    user = users_collection.find_one({"user_id": user_id})
    return user if user else {}

def set_frenzy_mode(user_id, status: bool):
    users_collection.update_one({"user_id": user_id}, {"$set": {"frenzy_mode_active": status}}, upsert=True)

def set_user_target(user_id, target_chat_id):
    users_collection.update_one({"user_id": user_id}, {"$set": {"target_chat_id": target_chat_id}}, upsert=True)

def add_worker_token(user_id, token):
    users_collection.update_one({"user_id": user_id}, {"$push": {"worker_tokens": token}}, upsert=True)

# --- Stats ---
def update_stats(stat_type, count=1):
    stats_collection.update_one({"_id": "global_stats"}, {"$inc": {stat_type: count}}, upsert=True)

def get_stats():
    return stats_collection.find_one({"_id": "global_stats"})
  
