# coding: utf-8

# In[40]:


# import librerie
import os
import tweepy
import facebook
import requests
import datetime
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import json

# In[65]:


# configuration file
config = {}
config_path = os.path.join(os.path.abspath('../../'))
config_name = 'config.py'
config_file = os.path.join(config_path,config_name)
exec(open(config_file).read(),config)
fb_app_id=config['FB_APP_ID']
fb_app_secret=config['FB_APP_SECRET']
fb_app_userid=config['FB_APP_USERID']

dir_out = os.path.join(os.path.abspath(''),'output')


# In[12]:


# get access token
payload = {'grant_type': 'client_credentials', 'client_id': fb_app_id, 'client_secret': fb_app_secret}
response = requests.post('https://graph.facebook.com/oauth/access_token?', params = payload)
json_data = json.loads(response.text)
access_token = json_data['access_token']


# In[13]:


# get graph Facebook
graph = facebook.GraphAPI(access_token=access_token, version='2.7')


# In[31]:


# Candidati Elezioni
users = [
    {'user':'Renzi','user_id':'113335124914'},
    {'user':'Di Maio','user_id':'522391027797448'},
    {'user':'Salvini','user_id':'252306033154'},
    {'user':'Pisapia','user_id':'112352038802143'},
    {'user':'Meloni','user_id':'38919827644'},
    {'user':'Berlusconi','user_id':'116716651695782'}
]


# In[32]:


# get today's date
todays_date = datetime.datetime.now().date()


# In[33]:


# get fans
def get_fans(fb_user, fb_id, graph, date):
    info = graph.get_object(id=fb_id, fields='name,fan_count,posts')
    fan = info['fan_count']
    fan_d = {'dt_rif':todays_date,'user':fb_user,'fb_fans':fan}
    return fan_d


# In[34]:


fb_fans = []
for user in users:
    fb_fans.append(get_fans(user['user'],user['user_id'],graph,todays_date))


# In[35]:


df = pd.DataFrame(fb_fans)


# In[36]:


df


# In[37]:


# get database connection
db=config['DATABASE_ELE']
schema=config['SCHEMA_ELE']
engine = create_engine(db)


# In[38]:


# write on db
df.to_sql('fb_fans', engine, schema=schema, if_exists='append')
