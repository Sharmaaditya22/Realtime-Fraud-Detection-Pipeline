import redis
import json

# 1. Connect
r = redis.Redis(host='localhost', port=6379, db=0)

# 2. Get all fraud keys
keys = r.keys("fraud:*")
print(f"Found {len(keys)} fraud alerts in Redis.")

# 3. Print the first 5 records
for key in keys:
    # Redis returns bytes, so we decode to string
    val_bytes = r.get(key)
    val_str = val_bytes.decode("utf-8")
    
    # Parse JSON to make it readable
    val_json = json.loads(val_str)
    
    print(f"🔑 Key: {key.decode('utf-8')}")
    print(f"📄 Data: {val_json}")
    print("-" * 30)

r.close()