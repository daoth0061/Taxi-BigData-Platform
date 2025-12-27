import requests
import json

# Login
r = requests.post('http://localhost:8088/api/v1/security/login', 
                  json={'username':'admin','password':'admin','provider':'db','refresh':True})
token = r.json()['access_token']
headers = {'Authorization': f'Bearer {token}'}

# Get existing datasets
ds = requests.get('http://localhost:8088/api/v1/dataset/', headers=headers)
print('Existing datasets:')
for d in ds.json().get('result', []):
    print(f"  - {d.get('table_name')} (id={d.get('id')})")

# Get databases
dbs = requests.get('http://localhost:8088/api/v1/database/', headers=headers)
print('\nExisting databases:')
for db in dbs.json().get('result', []):
    print(f"  - {db.get('database_name')} (id={db.get('id')})")
    
# Try to test connection to database 1
print('\nTesting database connection...')
test_r = requests.get('http://localhost:8088/api/v1/database/1/select_star/default/gold_fact_trip/', headers=headers)
print(f"Test query status: {test_r.status_code}")
if test_r.status_code == 200:
    print("Tables are accessible!")
else:
    print(f"Error: {test_r.text[:500]}")
