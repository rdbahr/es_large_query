# Written by Robert Bahr
# 09/23/2024

import requests
import json
import pandas as pd
import time

script_start = time.time()

f_name = "es_large_query_result"
f_name_ext = ".csv"
pit_window = "5m"
index_pattern = "<index_name_pattern-*>"
url_root = "https://<es_endpoint>/"
url = url_root + "_search"

# api keys are preferred, but depends on configuration of existing elasticsearch cluster
es_user = "<user>"
es_pass = "<pass>"

# query example
query = json.dumps({
  "_source": [
    "@timestamp",
    "message",
    "fields",
    "operationId"
  ],
  "track_total_hits": true,
  "size": 10000,
  "query": {
    "bool": {
      "must": [
        "range": {
          "@timestamp": {
            "gte": "now-30d",
            "lte": "now"
          }
        }
      ]
    }
  },
  "sort": [{ 
    "@timestamp": { 
      "order": "asc", 
      "format": "strict_date_optional_time_nanos", 
      "numeric_type": "date_nanos" 
    }},
  "_doc"
  ]
})

headers = {
  'Content-Type': "application/json"
}

# --- def functions
def extractKVFromJson(response):
  global i
  res_dict = []
  for hit in response['hits']['hits']:
    row = {}
    row['@timestamp'] = hit['_source']['@timestamp']
    row['message'] = hit['_source']['message']
    try:
      json_fields = json.loads(hit['_source']['fields'])
      if field_to_parse in json_fields['path_to_field']:
        row['field_to_parse'] = json_fields['path_to_field']['field_to_parse']
      else:
        row['field_to_parse'] = 'null'
    except valueError:
      print("Decode JSON failed for hit.")
      continue
    re_dict.append(row)
  df = pd.DataFrame(res_dict)
  this_name = f_name + "_" + str(i) + f_name_ext
  df.to_csv(this_name, sep=",", index=False, header=True)
  i += 1
  print("counter: " + str(i))

def run_query(query_to_run):
  start = time.time()
  res = requests.request("GET", url, auth=(es_user, es_pass), headers=headers, data=json.dumps(query_to_run)).json()
  end = time.time()
  print("Hits: " + str(res['hits']['total']['value']))
  print("Took: " + str(end - start))
  if res['hits']['hits']:
    extractKVFromJson(res)
  else:
    print("results empty.")
    delete_pit = { "id": pit['id'] }
    delete_pit_res = requests.request("DELETE", url_root + "_pit", auth=(es_user, es_pass), headers=headers, data=json.dumps(delete_pit))
    print("Delete Point-in-Time with result: " + delete_pit_res.text)
    script_end = time.time()
    print("Script duration: " + str(script_end - script_start))
    print("Exiting...")
    exit(0)
  return res

# Get point-in-time token to ensure correct sort-order and limit "now" to when query is first executed without refreshes
pit = requests.request("POST", url_root + index_pattern + "/_pit?keep_alive=" + pit_window, auth=(es_user, es_pass), headers=headers).json()
with open('pit.txt', 'w') as f:
  f.write(repr(pit))

print("Setting Point-in-Time: " + pit['pit'][:10] + "...")

i = 0
print("counter: " + str(i))

# add pit to query
json_query = json.loads(query)
json_query['pit'] = { "id": pit['id'], "keep_alive": pit_window }

result = run_query(json_query)

# Get last value of current payload for search_after
while result['hits']['hits'][-1]['sort']:
  print("Searching after: " + str(result['hits']['hits'][-1]['sort']))
  json_query['search_after'] = result['hits']['hits'][-1]['sort']
  # ensure pit is set
  json_query['pit'] = { "id": pit['id'], "keep_alive": pit_window }
  result = run_query(json_query)
  
