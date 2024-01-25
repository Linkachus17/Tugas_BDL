from pymongo import MongoClient

# Replace the connection string with your own
client = MongoClient("mongodb://localhost:1010/")

# Select the database and collection
db = client["testcrud"]
collection = db["crud"]

# Insert a document
document = {"name": "Test", "age": 50}
collection.insert_one(document)

# Find a document
query = {"name": "John"}
result = collection.find_one(query)

# # Update a document
# query = {"name": "John"}
# new_values = {"$set": {"age": 35}}
# collection.update_one(query, new_values)

# # Delete a document
# query = {"name": "John"}
# collection.delete_one(query)

