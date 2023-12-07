import redis
 
r = redis.Redis(host='localhost', port=6379, decode_responses=True)
queue_input = r.lpush("Queue1", "Task1", "Task2", "Task3", "Task4", "Task5")
queue_input
print(queue_input)
 
for x in range(5):
    queue_output = r.rpoplpush("Queue1", "tempQueue")
    print(queue_output)