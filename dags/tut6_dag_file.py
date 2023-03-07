from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import pandas as pd
import xml.etree.ElementTree as ET 
import wget
import requests

# [START default_args]
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'fxing',
    'depends_on_past': False,
    'email': ['fxing@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}
# [END default_args]

def extract_from_xml(file_to_process):
    pub_list = []
    tree = ET.parse(file_to_process) 
    root = tree.getroot() 
    for item in root:
        if item.tag == "r":
            for one_pub in item:
                num_of_authors = 0
                pub_attribs = {}
                pub_attribs["type"] = one_pub.tag
                pub_attribs["paper_key"] = one_pub.attrib['key']
                pub_attribs["mdate"] = one_pub.attrib['mdate']  
                for feature in pub_features:
                    if feature == "author":
                            all_authors = {}
                            for i, one_author in enumerate(one_pub.findall(feature)):
                                all_authors[i+1] = one_author.text
                            pub_attribs[feature] = all_authors
                            pub_attribs['num_authors'] = len(list(all_authors.keys()))
                    elif feature == "category":
                        pub_attribs[feature] = pub_attribs["paper_key"].split('/')[0].rstrip('s')
                    elif feature == "publisher":
                        if one_pub.find('booktitle') == None:
                            if one_pub.find('journal') == None:
                                pub_attribs[feature] = one_pub.find('publisher').text
                            else:
                                pub_attribs[feature] = one_pub.find('journal').text
                        else:
                            pub_attribs[feature] = one_pub.find('booktitle').text
                    elif feature == 'position':
                        position = {}
                        for sf in ['number', 'volume', 'pages']:
                            if one_pub.find(sf) != None:
                                position[sf] = one_pub.find(sf).text
                            else:
                                position[sf] = None
                        pub_attribs[feature] = (position['number'], position['volume'], position['pages'])
                    elif feature == "ee":
                        ees = set()
                        for one_ee in one_pub.findall(feature):
                            ees.add(one_ee.text)
                        pub_attribs[feature] = ees
                    elif feature in ["title", "year", "url", "crossref"]:
                        if one_pub.find(feature) != None:
                            pub_attribs[feature] = int(one_pub.find(feature).text) if feature == 'year' else one_pub.find(feature).text
                        else:
                            pub_attribs[feature] = None     
                pub_list.append(pub_attribs)
    return pub_list

def drop_table(session):
    drop_tab = '''
    DROP TABLE IF EXISTS publication;
    '''
    session.execute(drop_tab)

def create_table_with_data(session, primary_key_setting, raw_xml_path):
    create_tab = f'''
    CREATE TABLE IF NOT EXISTS publication (
        paper_key text,
        title text,
        year int, 
        type text, 
        authors map<int, text>,
        num_authors int,
        category text,
        publisher text,
        position frozen<position>,
        ee set<text>,
        url text, 
        crossref text,
        mdate date,
        PRIMARY KEY {primary_key_setting}
    ) WITH comment='All publication records of Prof. Ooi Beng Chin';'''
    session.execute(create_tab)
    
    insert_data = '''
    INSERT INTO publication (
        paper_key,
        title,
        year,
        type, 
        authors,
        num_authors,
        category,
        publisher,
        position,
        ee,
        url, 
        crossref,
        mdate
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    '''
    insert_stmt = session.prepare(insert_data)

    pub_ls = extract_from_xml(raw_xml_path)    
    for pub in pub_ls:
        session.execute(insert_stmt, 
        [pub['paper_key'], pub['title'], pub['year'], pub['type'], pub['author'], pub['num_authors'],
        pub['category'], pub['publisher'], pub['position'], pub['ee'], pub['url'],
        pub['crossref'], pub['mdate']])
    return pub_ls

pub_features = ["title", "year", "author", "category", "publisher", "position", "ee", "url", "crossref"]

cloud_config= {'secure_connect_bundle': '/Users/fxing/Desktop/secure-connect-researcher.zip'}
auth_provider = PlainTextAuthProvider('kXlFgKDLMXzMGlZqcFFZpJOn', 'McgqFZp.0UOi-eZjQxcL.SHQG+l68I9H9iIYaMSTN-6H0rf771E7LvgZov+WohFrNg6-nTNO7sQzan0-z-ZKnDebBFrlahkzU0xSmD9DDLMxaZnaQlndFNkJWIr7Oe2H')

#############

def checkdbconnection():  
    cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
    session = cluster.connect()   
    row = session.execute("select release_version from system.local").one()
    if row:
          print('Cassandra version '+row[0])
    else:
          print("An error occurred.")

def updateclouddbdata():
    cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
    session = cluster.connect()
    session.set_keyspace('mykeyspace')  

    drop_table(session)
    URL = "https://dblp.org/pid/o/BengChinOoi.xml"
    #response = wget.download(URL, 'xmlfile.xml')
    response = requests.get(URL)
    #print(response.text)
    with open('xmlfile.xml', 'wb+') as f:
        f.write(response.content)
        f.close()
    pub_ls = create_table_with_data(session, "(category, year, paper_key)", 'xmlfile.xml')

def queryclouddbdata():
    cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
    session = cluster.connect()
    session.set_keyspace('mykeyspace')  
    result = session.execute('select count(*) from publication;').one()[0]
    print(result)

# [START instantiate_dag]
#...........
with DAG(
    'update_cassandra_with_web_data',
    default_args=default_args,
    description='regularly update a cloud based cassandra db (astradb) with dblp data of a single person(author)\'s updated publication record, and make it easy for debugging, maintainance, and restructuring.',
    schedule_interval=timedelta(seconds=60),
    start_date=datetime(2022, 10, 5),
    catchup=False,
    tags=['tutorial-6'],
) as dag:
    # [END instantiate_dag]


    check_db_connection = PythonOperator(
        task_id ='check_connection',
        python_callable = checkdbconnection)

    update_cloud_db_data = PythonOperator(
        task_id ='update_data',
        python_callable = updateclouddbdata)

    query_cloud_db_data = PythonOperator(
        task_id ='query_data',
        python_callable = queryclouddbdata)

    check_db_connection >> update_cloud_db_data >>  query_cloud_db_data
    # [END documentation]