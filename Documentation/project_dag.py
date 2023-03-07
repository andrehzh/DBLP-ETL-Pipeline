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

#append files accordingly
cloud_config= {
         'secure_connect_bundle': '/Users/andre/Plan3 Design & Build Dropbox/Andre Heng/Mac/Documents/NUS Y2S1/IS3107/IS3107 Mini Project/secure-connect-is3107astradb.zip'
}
auth_provider = PlainTextAuthProvider('XZGMaIyaQCJBSOZOXBxQHGrX', '7KhXw2ywLrDsJDNryIP1mTohNE5lDgxccpqjKjl17EPDIAhY,-WbqTKv8,k6IZ.hrKxr-g5xZ2eL39QBywTAJZCyO3j5euY2mWS85dpQxh9qYdLZ7QwsaUsh,hrTbATT')
cs_researchers = pd.read_csv("/Users/andre/Plan3 Design & Build Dropbox/Andre Heng/Mac/Documents/NUS Y2S1/IS3107/IS3107 Mini Project/cs_researchers.csv")

# [START default_args]
default_args = {
    'owner': 'Andre',
    'depends_on_past': False,
    'email': ['e0725806@u.nus.edu'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}
 
def start_dag():
    print("Starting DAG")

def check_localdb_connection():
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect()
    session.set_keyspace('localdb')
    row = session.execute("select release_version from system.local").one()
    if row:
          print('Cassandra version '+row[0])
    else:
          print("An error occurred.")

def check_astradb_connection():
    cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
    session = cluster.connect()
    session.set_keyspace('astradb') 
    row = session.execute("select release_version from system.local").one()
    if row:
          print('Cassandra version '+row[0])
    else:
          print("An error occurred.")

#help with checking of data set
def check_if_pub_exists(session, paper_key):

    create_idx_q1 = '''CREATE INDEX IF NOT EXISTS ON pubs_by_pid (paper_key);'''

    probe_query = f'''
        SELECT title FROM pubs_by_pid WHERE paper_key = '{paper_key}';
    '''
    session.execute(create_idx_q1)
    rows = session.execute(probe_query)
    check = []
    for (title) in rows:    
        check.append({'title':title})
    check = pd.DataFrame(check)
    if check.empty:
        return True
    else:
        return False

def localdb_load_pub_ls(session, pub_ls):
    #index
    insert_data_pubs_by_pid = '''
        INSERT INTO localdb.pubs_by_pid (
            pid,
            paper_key,
            authorEntryIndex,
            category,
            year,
            author_name,
            authors,
            title,
            mdate
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        '''
    insert_stmt1 = session.prepare(insert_data_pubs_by_pid)

    insert_data_pubs_by_pid_index_year = '''
        INSERT INTO localdb.pubs_by_pid_index_year (
            pid,
            paper_key,
            authorEntryIndex,
            category,
            year,
            author_name,
            authors,
            title,
            mdate
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        '''
    insert_stmt2 = session.prepare(insert_data_pubs_by_pid_index_year)

    insert_data_pubs_by_pid_index_cat_year = '''
        INSERT INTO localdb.pubs_by_pid_index_cat_year (
            pid,
            paper_key,
            authorEntryIndex,
            category,
            year,
            author_name,
            authors,
            title,
            mdate
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        '''
    insert_stmt3 = session.prepare(insert_data_pubs_by_pid_index_cat_year)

    insert_data_pubs_by_pid_cat_year_index = '''
        INSERT INTO localdb.pubs_by_pid_cat_year_index (
            pid,
            paper_key,
            authorEntryIndex,
            category,
            year,
            author_name,
            authors,
            title,
            mdate
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        '''
    insert_stmt4 = session.prepare(insert_data_pubs_by_pid_cat_year_index)

    #coauthor_pid
    insert_data_pubs_by_name_coauthors_pid = '''
        INSERT INTO localdb.pubs_by_name_coauthors_pid (
            pid,
            author_name,
            coauthor_pid,
            paper_key,
            year,
            title,
            mdate
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        '''
    insert_stmt5 = session.prepare(insert_data_pubs_by_name_coauthors_pid)

    insert_data_pubs_by_pid_year_coauthors_pid = '''
        INSERT INTO localdb.pubs_by_pid_year_coauthors_pid (
            pid,
            author_name,
            coauthor_pid,
            paper_key,
            year,
            title,
            mdate
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        '''
    insert_stmt6 = session.prepare(insert_data_pubs_by_pid_year_coauthors_pid)

    insert_data_pubs_by_name_year_coauthors_pid = '''
        INSERT INTO localdb.pubs_by_name_year_coauthors_pid (
            pid,
            author_name,
            coauthor_pid,
            paper_key,
            year,
            title,
            mdate
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        '''
    insert_stmt7 = session.prepare(insert_data_pubs_by_name_year_coauthors_pid)
    
    for pub in pub_ls:
        session.execute(insert_stmt1,
                        [pub['pid'], pub['paper_key'], pub['authorEntryIndex'], pub['category'], pub['year'], pub['author_name'], pub['authors'], pub['title'], pub['mdate']])

        session.execute(insert_stmt2,
                        [pub['pid'], pub['paper_key'], pub['authorEntryIndex'], pub['category'], pub['year'], pub['author_name'], pub['authors'], pub['title'], pub['mdate']])

        session.execute(insert_stmt3,
                        [pub['pid'], pub['paper_key'], pub['authorEntryIndex'], pub['category'], pub['year'], pub['author_name'], pub['authors'], pub['title'], pub['mdate']])

        session.execute(insert_stmt4,
                        [pub['pid'], pub['paper_key'], pub['authorEntryIndex'], pub['category'], pub['year'], pub['author_name'], pub['authors'], pub['title'], pub['mdate']])

        for coauthor_pid in pub['coauthors_pid']:
                session.execute(insert_stmt5,
                                [pub['pid'], pub['author_name'], coauthor_pid, pub['paper_key'], pub['year'], pub['title'], pub['mdate']])

                session.execute(insert_stmt6,
                                [pub['pid'], pub['author_name'], coauthor_pid, pub['paper_key'], pub['year'], pub['title'], pub['mdate']])

                session.execute(insert_stmt7,
                                [pub['pid'], pub['author_name'], coauthor_pid, pub['paper_key'], pub['year'], pub['title'], pub['mdate']])
    return pub_ls

def extract_from_xml(session, file_to_process):
    pub_list = []
    tree = ET.parse(file_to_process)
    root = tree.getroot()
    type_of_pub_arr = ("article", "inproceedings", "proceedings", "book", "incollection", "phdthesis", "masterthesis", "www", "person", "data") 
    type_of_author_arr = ("author", "editor")

    for dblpperson in root.iter('dblpperson'):
        pid = dblpperson.attrib['pid']
        author_name = dblpperson.attrib['name']

    for item in root.findall('r'):

        for child in item:

            pub_attribs = {}
            pub_attribs["pid"] = pid
            pub_attribs["author_name"] = author_name

            for i in type_of_pub_arr:
                type = item.find(i)
                if type is not None:
                    paper_key = type.attrib['key']
                    pub_attribs["paper_key"] = paper_key
                    pub_attribs["mdate"] = type.attrib['mdate']
                    break

            coauthors_pid = []
            authors = {}
            authors_researchers = {}
            count = 1
            for i in type_of_author_arr:
                for auth in child.findall(i):
                    name = auth.text
                    if auth.get('orcid'):
                        orcid = auth.attrib['orcid']
                    else:
                        orcid = None
                    diff_pid = auth.attrib['pid']
                    if diff_pid == pub_attribs["pid"]:
                        pub_attribs["authorEntryIndex"] = count
                    else:
                        coauthors_pid.append(diff_pid)
                    
                    authors[count] = (name, orcid, diff_pid)
                    count += 1

                pub_attribs["authors"] = authors
                pub_attribs["coauthors_pid"] = coauthors_pid
            
            count = 1
            for i in type_of_author_arr: 
                for auth in child.findall(i):
                    name = auth.text
                    if auth.get('orcid'):
                        orcid = auth.attrib['orcid']
                    else:
                        orcid = None
                    diff_pid = auth.attrib['pid']
                    if diff_pid in cs_researchers.values:
                        authors_researchers[count] = (name, orcid, diff_pid)
                        count += 1
                    
                pub_attribs['authors_researchers'] = authors_researchers
                # if pub_attribs['authorEntryIndex'] is None:
                #     pub_attribs['authorEntryIndex'] = "HAHAHAHAHAHBABABABABABABA~!!!"

            pub_attribs["category"] = pub_attribs["paper_key"].split('/')[0].rstrip('s')

            year = child.find('year')
            if year is not None:
                year = int(year.text)
                pub_attribs["year"] = year

            title = child.find("title")
            if title is not None:
                title = title.text
                pub_attribs["title"] = title

            ees = set()
            for one_ee in child.findall("ee"):
                ees.add(one_ee.text)            
            pub_attribs["ee"] = ees
            
            #do check here before appending
            if check_if_pub_exists(session, paper_key) == True:
                pub_list.append(pub_attribs)
                # print(pub_list)
            # print(pub_list)
    # print(pub_list)
    return pub_list

def get_data_load_localdata(session):
    total_new_pubs_dic = {}
    total_num_new_pubs = 0
    all_pubs_ls = []

    api = "https://dblp.org/pid/"
    for index, row in cs_researchers.iterrows():
        pid = row['PID']
        name = row['Name']
        current_api = api + pid
        current_api += ".xml"

        response = requests.get(current_api)
        if response.status_code == 200:
            with open('xmlfile.xml', 'wb+') as f:
                f.write(response.content)
                f.close()
            
            pub_ls = extract_from_xml(session, 'xmlfile.xml')
            localdb_load_pub_ls(session, pub_ls)
            total_num_new_pubs += len(pub_ls)
            total_new_pubs_dic[f'{pid}'] = pid
            all_pubs_ls.append(pub_ls)

    total_new_pubs_dic["all_pubs"] = all_pubs_ls
    total_new_pubs_dic["total_new_num"] = total_num_new_pubs
    return total_new_pubs_dic

def query_num_unique_pubs():
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect()
    session.set_keyspace('localdb')

    query = []
    rows = session.execute('SELECT count(*) as total FROM localdb.pubs_by_pid;')
    for (total) in rows:
        query.append({'':total})
    query = pd.DataFrame(query)
    query = query.reset_index(drop=True).iloc[0].to_string()
    return query

def load_astradata(today, total_new_pubs_dic):
    cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
    session = cluster.connect()
    session.set_keyspace('astradb') 

    all_pubs_ls = total_new_pubs_dic["all_pubs"]

    insert_tab_1 = '''
    INSERT INTO astradb.volume_update (
        timestamp,
        num_new_pubs,
        num_unique_pubs
    ) VALUES (?, ?, ?)
    '''
    insert_stmt1 = session.prepare(insert_tab_1)

    insert_tab_2 = '''
    INSERT INTO astradb.author_pub_update (
        timestamp,
        paper_key,
        title,
        authors,
        ee
    ) VALUES (?, ?, ?, ?, ?)
    '''
    insert_stmt2 = session.prepare(insert_tab_2)
    
    query_result = query_num_unique_pubs()

    for pub_ls in all_pubs_ls:
        for pub in pub_ls:

            session.execute(insert_stmt2,
                            [today, pub['paper_key'], pub['title'], pub['authors_researchers'], pub['ee']])
    session.execute(insert_stmt1,
                    [today, total_new_pubs_dic['total_new_num'], query_result])

#num of new pubs
#total pubs
#list of new pubs
def update_db():
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect()
    session.set_keyspace('localdb')

    from datetime import date 

    today = date.today()

    today_form = today.strftime("%d/%m/%Y")

    total_new_pubs_dic = get_data_load_localdata(session)

    load_astradata(today_form, total_new_pubs_dic)

def query_answers():
    from datetime import date 
    today = date.today()
    today_form = today.strftime("%d/%m/%Y")

    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect()
    session.set_keyspace('localdb')
    query_results = []

    # Q1
    rows = session.execute(
        "SELECT authorEntryIndex, COUNT(*) as total, category FROM pubs_by_pid_index_cat_year WHERE pid = '40/2499' AND authorEntryIndex = 3 AND category = 'conf' AND year >= 2012 AND year <= 2022;")
    query_entry_1 = {}
    query_entry_1["query_num"] = today_form + ' Q1'
    q1 = []
    for (authorEntryIndex, total, category) in rows:
        q1.append({'authorEntryIndex': authorEntryIndex,
                  'total': total, 'category': category})
    q1 = pd.DataFrame(q1)
    q1 = q1.reset_index(drop=True).iloc[0].to_string()
    query_entry_1["query_outcome"] = q1
    query_results.append(query_entry_1)

    # Q2
    rows = session.execute(
        "SELECT authorEntryIndex, COUNT(*) as total, pid FROM pubs_by_pid_index_year WHERE pid = 'o/BengChinOoi' AND authorEntryIndex = 2 AND year >= 2017 AND year <= 2022;")
    query_entry_1 = {}
    query_entry_1["query_num"] = today_form + ' Q2'
    q2 = []
    for (authorEntryIndex, total, pid) in rows:
        q2.append({'authorEntryIndex': authorEntryIndex,
                  'total': total, 'pid': pid})
    q2 = pd.DataFrame(q2)
    q2 = q2.reset_index(drop=True).iloc[0].to_string()
    query_entry_1["query_outcome"] = q2
    query_results.append(query_entry_1)

    #Q3
    rows = session.execute("SELECT COUNT(*) as total, author_name FROM pubs_by_name_coauthors_pid WHERE author_name='Lihua Xie' GROUP BY coauthor_pid;")
    query_entry_1 = {}
    query_entry_1["query_num"] = today_form + ' Q3'
    q3 = []
    input_total = 0
    for (total, author_name) in rows:
        input_total += total
    q3.append({'total':input_total, 'author_name':author_name})
    q3 = pd.DataFrame(q3)
    q3 = q3.reset_index(drop=True).iloc[0].to_string()
    query_entry_1["query_outcome"] = q3
    query_results.append(query_entry_1)

    #Q4
    rows = session.execute("SELECT coauthor_pid, count(*) as total, year FROM pubs_by_name_year_coauthors_pid WHERE author_name='Beng Chin Ooi' AND year = 2020 GROUP BY coauthor_pid;")
    query_entry_1 = {}
    query_entry_1["query_num"] = today_form + ' Q4'
    q4 = []
    for (coauthor_pid, total, year) in rows:    
        q4.append({'coauthor_pid':coauthor_pid, 'year':year, 'total':total})
    q4 = pd.DataFrame(q4)
    q4 = q4.sort_values(by=['total'],ascending=False).reset_index(drop=True).iloc[0].to_string()
    query_entry_1["query_outcome"] = q4
    query_results.append(query_entry_1)

    return query_results
    
def insert_query_answers():
    cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
    session = cluster.connect()
    session.set_keyspace('astradb')

    #Insert Query Results
    insert_query_data = '''
        INSERT INTO astradb.query_outcomes (
            query_num,
            query_outcome
        ) VALUES (?, ?)
    '''
    insert_stmt_query = session.prepare(insert_query_data)

    query_results = query_answers()

    for query in query_results:
        session.execute(insert_stmt_query,
                        [query['query_num'], query['query_outcome']])
    
with DAG(
    "project_dag",
    default_args=default_args, 
    description='ETL data pipeline which extracts API from DBLP and inserts into local database',
    schedule_interval="@weekly", 
    # schedule_interval=timedelta(seconds=60),
    start_date=datetime(2022, 11, 20),
    catchup=False,
) as dag:

        test_dag_run = PythonOperator(
            task_id="start_dag",
            python_callable=start_dag
        )

        localdb_connection = PythonOperator(
            task_id = "localdb_connection",
            python_callable=check_localdb_connection
        )

        astradb_connection = PythonOperator(
            task_id="astradb_connection",
            python_callable=check_astradb_connection
        )

        update_data = PythonOperator(
            task_id = "update_data",
            python_callable=update_db
        )

        query_data = PythonOperator(
            task_id="query_data",
            python_callable=insert_query_answers
        )

        test_dag_run >> [localdb_connection, astradb_connection] >> update_data >> query_data
    

