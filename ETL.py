from airflow import DAG
from databox import Client
from google.cloud import storage
from gcsfs import GCSFileSystem
import pandas as pd
import logging as log
import pendulum
import requests
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable
from airflow.models import TaskInstance
import os


def get_auth_header():
    my_bearer_token = Variable.get("TWITTER_BEARER_TOKEN")
    return {"Authorization": f"Bearer {my_bearer_token}"}


def get_twitter_api_data_func(ti: TaskInstance, **kwargs):
    get_users = Variable.get(f"TWITTER_USER_IDS", [], deserialize_json=True)
    get_tweets = Variable.get(f"TWITTER_TWEET_IDS", [], deserialize_json=True)
    all_users = []
    for user in get_users:
        api_url = f"https://api.twitter.com/2/users/{user}"
        user_request = requests.get(api_url, headers=get_auth_header(), params={"user.fields":"public_metrics,profile_image_url,username,description,id"})
        log.info(user_request.json())
        all_users.append(user_request.json())
    ti.xcom_push("user_requests", all_users)
    log.info(all_users)

    all_tweets = []

    for tweet in get_tweets:
        api_url = f"https://api.twitter.com/2/tweets/{tweet}"
        tweet_request = requests.get(api_url, headers=get_auth_header(), params={"tweet.fields":"public_metrics,author_id,text"})
        log.info(tweet_request.json())
        all_tweets.append(tweet_request.json())

    ti.xcom_push("tweet_requests", all_tweets)
    log.info(all_tweets)

def transform_twitter_api_data_fun(ti: TaskInstance, **kwargs):
    user_info = ti.xcom_pull(key="user_requests", task_ids="get_twitter_api_data_task")
    log.info(user_info)
    tweet_info = ti.xcom_pull(key="tweet_requests", task_ids="get_twitter_api_data_task")
    log.info(tweet_info)
    user_lib = {"user_id":[],"username":[],"name":[],"followers_count":[], "following_count":[], "tweet_count":[],"listed_count":[]}
    tweet_lib = {"tweet_id":[],"text":[],"retweet_count":[], "reply_count":[], "like_count":[], "quote_count":[],"impression_count":[]}
    for r in user_info:
        user_lib["user_id"].append(r['data']['id'])
        user_lib["username"].append(r['data']['username'])
        user_lib["name"].append(r['data']['name'])
        user_lib["followers_count"].append(r['data']['public_metrics']['followers_count'])
        user_lib["following_count"].append(r['data']['public_metrics']['following_count'])
        user_lib["tweet_count"].append(r['data']['public_metrics']['tweet_count'])
        user_lib["listed_count"].append(r['data']['public_metrics']['listed_count'])

    for t in tweet_info:
        tweet_lib["tweet_id"].append(t['data']['id'])
        tweet_lib["text"].append(t['data']['text'])
        tweet_lib["retweet_count"].append(t['data']['public_metrics']['retweet_count'])
        tweet_lib["reply_count"].append(t['data']['public_metrics']['reply_count'])
        tweet_lib["like_count"].append(t['data']['public_metrics']['like_count'])
        tweet_lib["quote_count"].append(t['data']['public_metrics']['quote_count'])
        tweet_lib["impression_count"].append(t['data']['public_metrics']['impression_count'])

    user_data = pd.DataFrame(user_lib)
    tweet_data = pd.DataFrame(tweet_lib)
    log.info(user_data)
    log.info(tweet_lib)
    #ti.xcom_push("tweet_data", tweet_data)
    #ti.xcom_push("user_data", user_data)
    c = storage.Client()
    bucket = c.get_bucket("a-j-apache-airflow-cs280")
    bucket.blob("data/users.csv").upload_from_string(user_data.to_csv(index=False), "text/csv")
    bucket.blob("data/tweets.csv").upload_from_string(tweet_data.to_csv(index=False), "text/csv")


def load_twitter_api_data_fun(ti: TaskInstance, **kwargs):
    user_token = Variable.get("DATABOX_TOKEN")

    #tweet_table = ti.xcom_pull(key="tweet_data", task_ids="transform_twitter_api_data_task")
    #user_table = ti.xcom_pull(key="user_data", task_ids="transform_twitter_api_data_task")
    # Intialize the databox client
    dbox = Client(user_token)
    fs = GCSFileSystem(project="Adrienne-Johnson-CS-280")
    with fs.open('gs://a-j-apache-airflow-cs280/data/tweets.csv', 'rb') as f:
        tweet_info = pd.read_csv(f)
    with fs.open('gs://a-j-apache-airflow-cs280/data/users.csv', 'rb') as f:
        user_info = pd.read_csv(f)

    #user_info.set_index("user_id")
    #user_set = set(user_info["user_id"])
    user_info['followers_count'].astype('int32')
    user_info.astype({'following_count': 'int32'}).dtypes
    user_info.astype({'tweet_count': 'int32'}).dtypes
    user_info.astype({'listed_count':'int32'}).dtypes
    log.info(user_info.dtypes)
    #for user in user_set
    EMfollowing = int(user_info.loc[0,'following_count'])
    EMfollowers = int(user_info.loc[0,'followers_count'])
    EMtweet = int(user_info.loc[0,'tweet_count'])
    EMlisted = int(user_info.loc[0,'listed_count'])
    dbox.push("Elon Musk Following Count", EMfollowing)
    dbox.push("Elon Musk Follower Count", EMfollowers)
    dbox.push("Elon Musk Tweet Count", EMtweet)
    dbox.push("Elon Musk Listed Count", EMlisted)

    JKRfollowing = int(user_info.loc[1,'following_count'])
    JKRfollowers = int(user_info.loc[1,'followers_count'])
    JKRtweet = int(user_info.loc[1,'tweet_count'])
    JKRlisted = int(user_info.loc[1,'listed_count'])
    dbox.push("JK Rowling Following Count", JKRfollowing)
    dbox.push("JK Rowling Follower Count", JKRfollowers)
    dbox.push("JK Rowling Tweet Count", JKRtweet)
    dbox.push("JK Rowling Listed Count", JKRlisted)

    NASAfollowing = int(user_info.loc[2,'following_count'])
    NASAfollowers = int(user_info.loc[2,'followers_count'])
    NASAtweet = int(user_info.loc[2,'tweet_count'])
    NASAlisted = int(user_info.loc[2,'listed_count'])
    dbox.push("NASA Following Count", NASAfollowing)
    dbox.push("NASA Follower Count", NASAfollowers)
    dbox.push("NASA Tweet Count", NASAtweet)
    dbox.push("NASA Listed Count", NASAlisted)

    SMEFfollowing = int(user_info.loc[3,'following_count'])
    SMEFfollowers = int(user_info.loc[3,'followers_count'])
    SMEFtweet = int(user_info.loc[3,'tweet_count'])
    SMEFlisted = int(user_info.loc[3,'listed_count'])
    dbox.push("SMEF Following Count", SMEFfollowing)
    dbox.push("SMEF Follower Count", SMEFfollowers)
    dbox.push("SMEF Tweet Count", SMEFtweet)
    dbox.push("SMEF Listed Count", SMEFlisted)

    SMSDfollowing = int(user_info.loc[4,'following_count'])
    SMSDfollowers = int(user_info.loc[4,'followers_count'])
    SMSDtweet = int(user_info.loc[4,'tweet_count'])
    SMSDlisted = int(user_info.loc[4,'listed_count'])
    dbox.push("SMSD Following Count", SMSDfollowing)
    dbox.push("SMSD Follower Count", SMSDfollowers)
    dbox.push("SMSD Tweet Count", SMSDtweet)
    dbox.push("SMSD Listed Count", SMSDlisted)


    tweet_info['reply_count'].astype("int32")
    tweet_info['like_count'].astype("int32")
    tweet_info['impression_count'].astype("int32")
    tweet_info['retweet_count'].astype("int32")

    T1reply = int(tweet_info.loc[0,'reply_count'])
    T1like = int(tweet_info.loc[0,'like_count'])
    T1impression = int(tweet_info.loc[0,'impression_count'])
    T1retweet = int(tweet_info.loc[0,'retweet_count'])
    dbox.push("Tweet1 Reply Count", T1reply)
    dbox.push("Tweet1 Like Count", T1like)
    dbox.push("Tweet1 Impression Count", T1impression)
    dbox.push("Tweet1 Retweet Count", T1retweet)

    T2reply = int(tweet_info.loc[1,'reply_count'])
    T2like = int(tweet_info.loc[1,'like_count'])
    T2impression = int(tweet_info.loc[1,'impression_count'])
    T2retweet = int(tweet_info.loc[1,'retweet_count'])
    dbox.push("Tweet2 Reply Count", T2reply)
    dbox.push("Tweet2 Like Count", T2like)
    dbox.push("Tweet2 Impression Count", T2impression)
    dbox.push("Tweet2 Retweet Count", T2retweet)

    T3reply = int(tweet_info.loc[2,'reply_count'])
    T3like = int(tweet_info.loc[2,'like_count'])
    T3impression = int(tweet_info.loc[2,'impression_count'])
    T3retweet = int(tweet_info.loc[2,'retweet_count'])
    dbox.push("Tweet3 Reply Count", T3reply)
    dbox.push("Tweet3 Like Count", T3like)
    dbox.push("Tweet3 Impression Count", T3impression)
    dbox.push("Tweet3 Retweet Count", T3retweet)

    T4reply = int(tweet_info.loc[3,'reply_count'])
    T4like = int(tweet_info.loc[3,'like_count'])
    T4impression = int(tweet_info.loc[3,'impression_count'])
    T4retweet = int(tweet_info.loc[3,'retweet_count'])
    dbox.push("Tweet4 Reply Count", T4reply)
    dbox.push("Tweet4 Like Count", T4like)
    dbox.push("Tweet4 Impression Count", T4impression)
    dbox.push("Tweet4 Retweet Count", T4retweet)

    T5reply = int(tweet_info.loc[4,'reply_count'])
    T5like = int(tweet_info.loc[4,'like_count'])
    T5impression = int(tweet_info.loc[4,'impression_count'])
    T5retweet = int(tweet_info.loc[4,'retweet_count'])
    dbox.push("Tweet5 Reply Count", T5reply)
    dbox.push("Tweet5 Like Count", T5like)
    dbox.push("Tweet5 Impression Count", T5impression)
    dbox.push("Tweet5 Retweet Count", T5retweet)
    #dbox.push("Disney", 2)
    #dbox.push("Apple", 200)


with DAG(
    dag_id="ETL",
    schedule_interval="0 9 * * *",
    start_date=pendulum.datetime(2023, 1, 1, tz="US/Pacific"),
    catchup=True,
) as dag:
    start_task = DummyOperator(task_id="start_task")
    get_twitter_api_data_task = PythonOperator(
        task_id="get_twitter_api_data_task",
        python_callable=get_twitter_api_data_func,
    )

    transform_twitter_api_data_task = PythonOperator(
        task_id="transform_twitter_api_data_task",
        python_callable=transform_twitter_api_data_fun,
	provide_context=True
    )

    load_twitter_api_data_task = PythonOperator(
        task_id="load_twitter_api_data_task",
        python_callable=load_twitter_api_data_fun,
    )
start_task >> get_twitter_api_data_task >> transform_twitter_api_data_task >> load_twitter_api_data_task
