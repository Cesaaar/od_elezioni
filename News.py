
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


# In[4]:


# get today's date
todays_date = datetime.datetime.now()
yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
str_dt = str(yesterday.date())


# In[12]:


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
    for art in articles[0:5]:
        d_article = {
            'user':user,
            'autore':art['author'],
            'desc':art['description'],
            'pubAt':art['publishedAt'],
            'fonte':art['source']['name'],
            'titolo':art['title'],
            'url':art['url'],
            'img':art['urlToImage'],
            'dt_rif':today
        }
        l_article.append(d_article)
    
    return l_article


# In[13]:


l = []
for user in users:
    l.append(get_user_news(user['user'],str_dt,todays_date,nw_key))


# In[21]:


df0_user = pd.DataFrame(l[0])
df1_user = pd.DataFrame(l[1])
df2_user = pd.DataFrame(l[2])


# In[24]:


df_user = df0_user.append(df1_user).append(df2_user)


# In[23]:


df0_nouser = df0_user.drop('user',1)
df1_nouser = df1_user.drop('user',1)
df2_nouser = df2_user.drop('user',1)


# In[25]:


df_nouser = df0_nouser.append(df1_nouser).append(df2_nouser)


# In[26]:


df = df_nouser.drop_duplicates()


# In[30]:


df_user.drop(['autore','desc','dt_rif','fonte','img','pubAt','url'],1,inplace=True)


# In[41]:


df_user.drop_duplicates(inplace=True)


# In[43]:


df = df.merge(df_user, left_on='titolo', right_on='titolo', how='inner')


# In[44]:


df.count()


# In[47]:


df.head(2)


# In[48]:


# get database connection
db=config['DATABASE_ELE']
schema=config['SCHEMA_ELE']
engine = create_engine(db)


# In[49]:


# write on db
df.to_sql('news', engine, schema=schema, if_exists='append')

