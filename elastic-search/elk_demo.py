from elasticsearch import Elasticsearch

#connection to els
client = Elasticsearch(
    "http://localhost:9200"
)
var = "demo"
client.indices.create(index=f"{var}")
for i in range(10):
    client.index(index=f"{var}",id=i,document={'value':i+1})
