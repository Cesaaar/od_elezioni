
# coding: utf-8

# In[1]:


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


# In[2]:


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


# In[3]:


# get access token
payload = {'grant_type': 'client_credentials', 'client_id': fb_app_id, 'client_secret': fb_app_secret}
response = requests.post('https://graph.facebook.com/oauth/access_token?', params = payload)
json_data = json.loads(response.text)
access_token = json_data['access_token']


# In[4]:


# get graph Facebook
graph = facebook.GraphAPI(access_token=access_token, version='2.7')


# In[5]:


# Candidati Elezioni
users = [
    {'user':config['USER1'],'user_id':config['USER1_ID_FB']},
    {'user':config['USER2'],'user_id':config['USER2_ID_FB']},
    {'user':config['USER3'],'user_id':config['USER3_ID_FB']},
    {'user':config['USER4'],'user_id':config['USER4_ID_FB']},
    {'user':config['USER5'],'user_id':config['USER5_ID_FB']},
    {'user':config['USER6'],'user_id':config['USER6_ID_FB']},
    {'user':config['USER7'],'user_id':config['USER7_ID_FB']}
]


# In[6]:


# get today's date
todays_date = datetime.datetime.now()


# In[27]:


# get last post
def get_last_post(fb_user, fb_id, graph, date):
    info = graph.get_object(id=fb_id, fields='posts')
    
    dt = info['posts']['data'][0]['created_time']
    id_post = info['posts']['data'][0]['id']
    try:
        msg = info['posts']['data'][0]['message']
    except:
        msg = info['posts']['data'][1]['message']
    likes = graph.get_connections(id=id_post, connection_name='likes', summary='true')['summary']['total_count']
    
    last_post = {'user':fb_user,'dt_rif':todays_date,'dt_post':dt,'id_post':id_post, 'msg':msg, 'likes':likes}
    
    return last_post


# In[28]:


fb_posts = []
for user in users:
    fb_posts.append(get_last_post(user['user'],user['user_id'],graph,todays_date))


# In[29]:


df = pd.DataFrame(fb_posts)


# In[30]:


df


# In[31]:


# get database connection
db=config['DATABASE_ELE']
schema=config['SCHEMA_ELE']
engine = create_engine(db)


# In[32]:


# write to db
df.to_sql('fb_posts', engine, schema=schema, if_exists='append')

