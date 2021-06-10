import requests 
import json
import pandas as pd

# token=

base='https://bonus.ly/api/v1/'




def get_all_bonus(token, skip=0, all_data=[]):
    headers={'Authorization': 'Bearer {}'.format(token)}
    if skip==0: all_data=[]
    url=base+'bonuses?limit={}&skip={}&include_children=true'.format(100, skip)
    res=requests.get(url, headers=headers).json()
    print('Staring from {}... Scraped {} records'.format(skip, len(all_data)))
    if len(res['result'])==0:
        return all_data
    else:
        all_data.extend(res['result'])
        return get_all_bonus(token=token, skip=skip+100, all_data=all_data)

# +
# data[4]
# -

def parse_record(row, all_df=pd.DataFrame()):
    amount=row['amount']
    giver=row['giver']['display_name']
    receiver=row['receiver']['display_name']
    datetime=row['created_at']
    reason=row['reason']
    hashtag=row['hashtag']
    
    edge_df= pd.DataFrame([{'from':giver, 'to':receiver, 'amount':amount,
                                         'datetime':datetime, 'reason':reason, 'hashtags':hashtag}])
    node_df=parse_person(row['giver'])
    
    if 'child_bonuses' in row.keys():
#         print(len(row['child_bonuses']))
        if len(row['child_bonuses'])>0:
            for i in row['child_bonuses']:
                child_temp_edge, child_temp_node=parse_record(i)
                edge_df=pd.concat([edge_df, child_temp_edge])
                node_df=pd.concat([node_df,child_temp_node])
                
                
    return edge_df, node_df



def parse_person(row):
    return pd.DataFrame.from_dict(row,orient='index').T



def generate_network_data(token):
    data=get_all_bonus(token)
    edge_list=pd.DataFrame()
    node_list=pd.DataFrame()
    
    all_parsed=[parse_record(row) for row in data]
    all_edges=pd.concat([row[0] for row in all_parsed]).drop_duplicates()
    all_nodes=pd.concat([row[-1] for row in all_parsed]).drop_duplicates(subset=['id'])
    
    print('Finished with {} edges and {} nodes'.format(len(all_edges), len(all_nodes)))
    return all_edges, all_nodes

# +
# edges, nodes= generate_network_data()
# -


