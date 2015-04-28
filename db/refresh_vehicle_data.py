
from urllib2 import Request, urlopen
import json
import psycopg2

# Initialize makes JSON link
api_key = 'jna7baxz2upgcue46td4k32f'
api_url = 'https://api.edmunds.com/api/vehicle/v2/makes?fmt=json&api_key='
req = Request(api_url + api_key)
res = urlopen(req)
data = json.loads(res.read())

# Prepare PostgreSQL connection
try:
    conn = psycopg2.connect("dbname='auto' \
                             user='mtmasset' \
                             host='localhost' \
                             password='tHera92esa'")
except:
    print "Error connecting to database"

cur = conn.cursor()

# Prepare the tables for data load
cur.execute("TRUNCATE auto.makes CASCADE;")
for r in data['makes']:
    # Store make information
    cur.execute("INSERT INTO auto.makes(make_id, make_ref_name, make_name) \
                 VALUES (%s, %s, %s)",
                (r['id'], r['niceName'], r['name']))
    makeID, makeName = r['id'], r['niceName']
    for s in r['models']:
        # Store model information
        cur.execute("INSERT INTO auto.models(make_id, model_id, model_ref_name, model_name) \
                     VALUES (%s, %s, %s, %s)",
                    (r['id'], s['id'],
                     s['niceName'], s['name']))
        for t in s['years']:
            # Store model-year mappings
            cur.execute("INSERT INTO auto.model_years (make_id, model_id, year_num) \
                         VALUES (%s, %s, %s)", (r['id'], s['id'], t['year']))

# Commit and close the connection
conn.commit()
conn.close()
