
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
import requests


# In[2]:


# configuration file
config = {}
config_path = os.path.join(os.path.abspath('../../'))
config_name = 'config.py'
config_file = os.path.join(config_path,config_name)
exec(open(config_file).read(),config)
nw_key=config['TOKEN_NW']


# In[3]:


# Candidati Elezioni
users = [
    {'user':config['USER1'],'user_id':config['USER1_ID_TW']},
    {'user':config['USER2'],'user_id':config['USER2_ID_TW']},
    {'user':config['USER3'],'user_id':config['USER3_ID_TW']}
]


# In[14]:


# get today's date
todays_date = datetime.datetime.now()
yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
str_dt = str(yesterday.date())


# In[15]:


def get_user_news(user,dt,today,key):
    url = ('https://newsapi.org/v2/everything?'
       'q='+user+'&'
       'from='+dt+'&'
       'sortBy=publishedAt&'
       'language=it&'
       'apiKey='+key)
    response = requests.get(url)
    json_data = json.loads(response.text)
    articles = json_data['articles']
    l_article = []
    for art in articles[0:10]:
        d_article = {
            'user':user,
            'autore':art['author'],
            'desc':art['description'],
            'pubAt':art['publishedAt'],
            'fonte':art['source']['name'],
            'titolo':art['title'],
            'url':art['url'],
            'img':art['urlToImage']
        }
        l_article.append(d_article)
    
    return l_article


# In[42]:


l = []
for user in users:
    l.append(get_user_news(user['user'],str_dt,todays_date,nw_key))


# In[53]:


df0 = pd.DataFrame(l[0])
df1 = pd.DataFrame(l[1])
df2 = pd.DataFrame(l[2])


# In[55]:


df = df0.append(df1).append(df2)
df.head(2)


# In[56]:


# get database connection
db=config['DATABASE_ELE']
schema=config['SCHEMA_ELE']
engine = create_engine(db)


# In[57]:


# write on db
df.to_sql('news', engine, schema=schema, if_exists='append')

