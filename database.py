import os
from pymongo import MongoClient, ReturnDocument, operations
from datetime import datetime

MONGO_URI = os.environ.get("MONGO_URI")
client = MongoClient(MONGO_URI)
db = client['frenzy_bot_db']

jobs_collection = db['jobs']
users_collection = db['users']
stats_collection = db['stats']
settings_collection = db['settings']
file_details_collection = db['file_details']
authorized_users_collection = db['authorized_users']

# --- Settings ---
def get_settings():
    return settings_collection.find_one({"_id": "config"})

def set_supergroup(chat_id, topic_ids):
    settings_collection.update_one({"_id": "config"}, {"$set": {"supergroup_id": chat_id, "topic_ids": topic_ids}}, upsert=True)

# --- NEW: User & Broadcast Management ---
def add_user(user_id):
    """Adds a new user if they don't exist, for broadcasting and stats."""
    # Using update_one with upsert is an efficient way to add a user only if they are new.
    result = users_collection.update_one({"_id": user_id}, {"$setOnInsert": {"_id": user_id, "started_at": datetime.utcnow()}}, upsert=True)
    if result.upserted_id:
        # If a new user was inserted, increment the users_started stat.
        update_stats("users_started")

def get_all_user_ids():
    """Gets a list of all user IDs that have ever started the bot."""
    return [user["_id"] for user in users_collection.find({}, {"_id": 1})]


# --- Authorization Management ---
def add_authorized_user(user_id):
    authorized_users_collection.update_one({"_id": user_id}, {"$set": {"authorized": True}}, upsert=True)
    return True

def remove_authorized_user(user_id):
    result = authorized_users_collection.delete_one({"_id": user_id})
    return result.deleted_count > 0

def is_user_authorized(user_id):
    return authorized_users_collection.find_one({"_id": user_id}) is not None

# --- Job Management ---
def add_job(user_id, chat_id, media_url, referer_url, task_id):
    jobs_collection.insert_one({"user_id": user_id, "chat_id": chat_id, "media_url": media_url, "referer_url": referer_url, "task_id": task_id, "status": "pending", "created_at": datetime.utcnow()})

def get_job():
    job = jobs_collection.find_one_and_update({"status": "pending"}, {"$set": {"status": "processing"}}, sort=[("created_at", 1)], return_document=ReturnDocument.AFTER)
    if job:
        job['_id'] = str(job['_id'])
    return job

def complete_job(job_id_str):
    from bson.objectid import ObjectId
    jobs_collection.delete_one({"_id": ObjectId(job_id_str)})

def get_pending_jobs_for_user(user_id):
    return list(jobs_collection.find({"user_id": user_id, "status": "pending"}))

def clear_pending_jobs_for_user(user_id):
    return jobs_collection.delete_many({"user_id": user_id, "status": "pending"}).deleted_count

def count_pending_jobs_for_task(task_id):
    return jobs_collection.count_documents({"task_id": task_id, "status": "pending"})

# --- User Config ---
def get_user_config(user_id):
    user = users_collection.find_one({"_id": user_id})
    return user if user else {}

def set_frenzy_mode(user_id, status: bool):
    users_collection.update_one({"_id": user_id}, {"$set": {"frenzy_mode_active": status}}, upsert=True)

def set_user_target(user_id, target_chat_id):
    users_collection.update_one({"_id": user_id}, {"$set": {"target_chat_id": target_chat_id}}, upsert=True)

def clear_user_target(user_id):
    users_collection.update_one({"_id": user_id}, {"$unset": {"target_chat_id": ""}})

# --- Stats ---
def update_stats(stat_type, count=1):
    stats_collection.update_one({"_id": "global_stats"}, {"$inc": {stat_type: count}}, upsert=True)

def get_stats():
    return stats_collection.find_one({"_id": "global_stats"})

# --- File Fetching Logic ---
def add_file_detail(task_id, file_id, media_type):
    file_details_collection.update_one({"_id": task_id}, {"$push": {"files": {"file_id": file_id, "media_type": media_type}}}, upsert=True)

def get_file_details(task_id):
    return file_details_collection.find_one({"_id": task_id})

