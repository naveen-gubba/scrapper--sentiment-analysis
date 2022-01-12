## Write your Code Segments here, 
##  because your VM will get deleted
import json
import random
import feedparser
from bs4 import BeautifulSoup
from bs4.element import Comment
from google.cloud import language_v1
import pandas as pd
import numpy as np
import psycopg2
from io import StringIO
import warnings
warnings.filterwarnings("ignore", category=FutureWarning)
import final_storage as stcode

PROJECT='UMC DSA 8420 FS2021'
my_bucket_name = 'dsa_mini_project_nkg7b3'

def tag_visible(element):
    if element.parent.name in ['style', 'script', 'head', 'title', 'meta', '[document]']:
        return False
    if isinstance(element, Comment):
        return False
    return True

def text_from_html(body):
    soup = BeautifulSoup(body, 'html.parser')
    texts = soup.findAll(text=True)
    visible_texts = filter(tag_visible, texts)  
    return u" ".join(t.strip() for t in visible_texts)

# Delete any blobs/reddits already present
prefix_name = 'my'
my_sample_list = stcode.list_blobs_with_prefix(my_bucket_name,prefix_name, delimiter=None)
for my_blob in my_sample_list:
    stcode.delete_blob(my_blob)
stcode.list_blobs(my_bucket_name)

# scrape/download 10 reddits as json files and load to storage
count = 1
while count <= 10:
    a_reddit_rss_url = 'http://www.reddit.com/new/.rss?sort=new'
    feed = feedparser.parse( a_reddit_rss_url )
    if (feed['bozo'] == 1):
        print("Error Reading/Parsing Feed XML Data")    
    else:  
        dict_keys = ['dttm','title','summary_text','link']
        temp_dict = dict()
        for key in dict_keys:
            temp_dict[key] = []
        for item in feed[ "items" ]:
            temp_dict['dttm'].append(item[ "date" ])
            temp_dict['title'].append(item[ "title" ])
            temp_dict['summary_text'].append(text_from_html(item[ "summary" ]))
            temp_dict['link'].append(item[ "link" ])
        filename = 'my_sample_json'+str(random.randint(1,1000))+'.json' 
        with open(filename,'w') as f:
            json.dump(temp_dict,f)
        stcode.upload_as_blob(my_bucket_name, json.dumps(temp_dict), filename)
        
    count += 1

stcode.list_blobs(my_bucket_name)

#aggregate into all json files into a python dictionary and load to dataframe
my_sample_dict = stcode.list_blobs_with_prefix(my_bucket_name,prefix_name, delimiter=None)

dict_keys = ['dttm','title','summary_text','link']
dict_to_df = {}

for key in dict_keys:
    dict_to_df[key] = []

for blob_to_dict in my_sample_dict:
    json_data = json.loads(stcode.read_blob_staging(blob_to_dict))
    print(type(json_data))
    for key, value in json_data.items():
        for i in range(len(json_data[key])):
            dict_to_df[key].append(json_data[key][i])

df_agg = pd.DataFrame(dict_to_df)
print(df_agg.shape)
print(df_agg.head(n=5).T)
print(df_agg.dtypes)



def sample_analyze_entity_sentiment(text_content):
    """
    Analyzing Entity Sentiment in a String
    Args:
      text_content The text content to analyze
    """
    client = language_v1.LanguageServiceClient()
    language = "EN"
    type_ = language_v1.types.Document.Type.PLAIN_TEXT
    
    document = {"content":text_content, "type_" : type_ ,"language":language}
    response = client.analyze_entity_sentiment(request = {'document': document})
    for entity in response.entities:
        sentiment = entity.sentiment
        return sentiment
        # print("Score: {}".format(sentiment.score))
        # print("Magnitude: {}".format(sentiment.magnitude))
        # print('*'*20)
        # # print('response', response)

# run google nlp api
df_agg['score'] = 0 
df_agg['magnitude'] = 0 
for col in range (df_agg.shape[0]):
    val = sample_analyze_entity_sentiment(df_agg['summary_text'][col])
    df_agg.iloc[col,-2] =  val.score
    df_agg.iloc[col,-1] =  val.magnitude
   
print(df_agg['score'].tolist())
print(df_agg['magnitude'].tolist())


#load to dataframe as  csv file amd upload the serialized/json  to storage
filename = 'df_toload' 
stcode.upload_as_blob(my_bucket_name, json.dumps(df_agg.to_csv()), filename)


#pull the csv file , deserialize it and load to dataframe inorder to load it into postgres
prefix_name = 'df'
load_csv_staging = stcode.list_blobs_with_prefix(my_bucket_name,prefix_name, delimiter=None)

for csv in load_csv_staging:
    csv_data = json.loads(stcode.read_blob_staging(csv))
    # print(type(csv_data))


df_csv = pd.read_csv(StringIO(csv_data))
df_csv.rename({'Unnamed: 0': 'id'}, axis=1, inplace=True)
df_csv.iloc[:,-1] = df_csv.iloc[:,-1].apply(lambda x : round(x,3))
df_csv.iloc[:,-2] = df_csv.iloc[:,-2].apply(lambda x : round(x,3))
df_csv.iloc[:,0] = df_csv.iloc[:,0].astype(np.int32)
print(df_csv.dtypes)
print(df_csv.head(n=5).T)


col0,col1,col2,col3,col4,col5,col6 = tuple(df_csv.columns.to_list()) 


#establish postgres connection and drop if the scrapersentiment table if exists and create the  table again.

try:
    conn = psycopg2.connect(database='scraper',
                            user='postgres',
                            host= '00.000.000.000',
                            password='')
    print("I am able to connect to the database")
except:
    print("I am unable to connect to the database")   

with conn, conn.cursor() as curs:
    sql1 = "DROP TABLE  IF EXISTS scrapersentiment;"
    sql2 = "CREATE TABLE IF NOT EXISTS scrapersentiment ({0} integer PRIMARY KEY,{1} text, {2} text, {3} text , {4} text, {5} real, {6} real);".format(col0,col1,col2,col3,col4,col5,col6)
    curs.execute(sql1)
    curs.execute(sql2)
    conn.commit()

    
  

#iterate through the dataframe and injest data into the table scraper sentiment

columns_headers =   ','.join(tuple(df_csv.columns.to_list())) 
print(columns_headers)

with conn, conn.cursor() as curs:
    v_str_list = ["%s"] * df_csv.shape[1]
    v_str = ",".join(v_str_list)
    sql = "INSERT INTO scrapersentiment({column}) VALUES ({v_str});".format(v_str= v_str,column= columns_headers )  
    vals = []
    for i in range(df_csv.shape[0]):
        df_temp = df_csv.iloc[i,:].to_list()
        vals.append(df_temp)
        vals  = [item for subitem in vals for item in subitem ]
        curs.execute(sql,(vals[0].item(),vals[1],vals[2],vals[3],vals[4],vals[5],vals[6]))
        conn.commit()
        vals = []
