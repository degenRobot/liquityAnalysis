from gql import gql, Client
from gql.transport.aiohttp import AIOHTTPTransport
import pandas as pd
import json
import sys
import os 


# Select your transport with a defined url endpoint
transport = AIOHTTPTransport(url="https://data.staging.arkiver.net/robolabs/liquityredemptions/graphql")

# Create a GraphQL client using the defined transport
client = Client(transport=transport, fetch_schema_from_transport=True)

# Provide a GraphQL query
queryStr = """
        query MyQuery($limit: Int!, $skip: Int!) {
        Redemptions(
            sort: BLOCK_DESC
            filter: {_operators: {block: {gt: 15000000}, lusdAmount: {gt: 50000}}}
            limit: $limit
            skip: $skip
        ) {
            account
            lusdAmount
            ethSent
            ethFee
            timestamp
            block
        }
        }
  """
query = gql(queryStr)
params = {"limit": 100, "skip": 0}

# Execute the query on the transport
result = client.execute(query, variable_values=params)
endResult = []
i = 0 
while(len(result['Redemptions']) > 0):
  #print("appending: " + str(result['Transfers']))
  endResult.append(result['Redemptions'])
  params["skip"] = params["skip"]+params["limit"]
  result = client.execute(query, variable_values=params)
  print("Getting Data " + str(i) + " - " + str(i+len(result['Redemptions'])))
  i += len(result['Redemptions'])
    
del endResult[-1] #one blank result for some reason
dfList = []
for itemBatch in endResult:
    df = pd.DataFrame.from_dict(itemBatch)
    dfList.append(df)

df = pd.concat(dfList)
df['datetime'] = pd.to_datetime(df['timestamp'], unit='s')


df.to_csv('Redemptions.csv', index = None)