#!/usr/bin/env python
# coding: utf-8

# In[15]:


import tableauserverclient as TSC
import pandas as pd
from pandas import json_normalize 
import json
import requests
import csv
from tableau_api_lib.utils import flatten_dict_column, flatten_dict_list_column


# In[16]:


from tableau_api_lib import TableauServerConnection
tableau_server_config = {
        'tableau_prod': {
                'server': 'https://tableau.host',
                'api_version': '3.10',
                'username': 'user.name',
                'password': 'password',
                'site_name': 'sitename',
                'site_url': ''
        }
}
conn = TableauServerConnection(tableau_server_config)
conn.sign_in()


# In[17]:


basic_query = """
{
    workbooks
    {
    name
    id
    vizportalUrlId
    projectName
    embeddedDatasources 
    {
        name
        id
        downstreamOwners 
        {
            username
        }
        upstreamTables 
        {
          name
          fullName
          id
          columns 
          {
            name
          } 
        }
    }
    }
}
"""


# In[18]:


query_results = conn.metadata_graphql_query(basic_query)
workbooks_df = pd.DataFrame(query_results.json()['data']['workbooks'])

#print(workbooks_df.head())
#print(json_normalize(query_results.json()['data']['tables']).head())


# In[19]:


workbooks_df.to_json("tableau_workbooks_data.json",
           orient="records",
           lines=True)


# In[20]:


workbooks_json = query_results.json()['data']['workbooks']
workbooks_df = pd.DataFrame(workbooks_json)


# In[22]:


query_results = conn.metadata_graphql_query(basic_query)


# In[ ]:


workbooks_df


# In[23]:


workbooks_json = query_results.json()['data']['workbooks']
workbooks_df = pd.DataFrame(workbooks_json)


# In[25]:


workbooks_df = json_normalize(workbooks_json)


# In[ ]:


workbooks_df


# In[27]:


compression_opts = dict(method='zip',
                        archive_name='tableau_workbooks_data.csv') 


# In[ ]:


json_normalize(query_results.json()['data']['workbooks'])


# In[28]:


workbooks_df.to_csv('tableau_workbooks_data.zip', index=False,
          compression=compression_opts) 

