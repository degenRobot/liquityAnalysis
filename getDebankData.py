from config import DEBANK_API
import pandas as pd 
import requests
import json

redemptions = pd.read_csv('Redemptions.csv')

insightUsers = redemptions['account'].unique()

headers = {'AccessKey': DEBANK_API}
base_url = 'https://pro-openapi.debank.com/v1'
total_balance = {}

for user in insightUsers:
    url = f'{base_url}/user/all_complex_protocol_list?id={user}'
    #print(url)
    res = requests.get(url, headers=headers)
    data = res.json()
    total_balance[user] = data
    #print(json.dumps(data, indent=4))

with open('./data.json', 'w') as f:
    json.dump(total_balance, f)
    
    
n = len(insightUsers)

cleanData = []

for user in insightUsers : 
    user_data = total_balance[user]
    address = user
    items = len(user_data)
    for j in range(items) : 
        item = user_data[j]
        
        portfolioInfo = item['portfolio_item_list']
        pItems = len(portfolioInfo)
        value = 0
        for p in range(pItems) : 
            value += item['portfolio_item_list'][p]['stats']['net_usd_value']
            #asset = 
        appendItem = {
            "User" : address,
            "Name" : item['name'],
            "Chain" : item['chain'],
            "Value" : value
        }
        
        cleanData.append(appendItem)
        
        
userDf = pd.DataFrame(cleanData)
userDf.to_csv("UserInfo.csv")