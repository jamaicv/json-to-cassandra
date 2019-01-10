""" Reuters' json file import
"""

"""
    Reuters' keyspace and table must be created prior to running this script.
    We will also create a 'text' table to host data contained in 'text' field.

    cqlsh> create keyspace reuters;
    cqlsh> create table reuters (id int primary key, companies text, date text, exchanges text, orgs text, people text, places text, text_id int, topics text);
    cqlsh> create table texts (id int primary key, body text, dateline text, title text);
"""

from pprint import pprint
import json
import os
import cassandra
from cassandra.cluster import Cluster

# Connection to reuters keyspace
cluster = Cluster()
session = cluster.connect('reuters')

session.execute("""
truncate table reuters;
""")

text_id = 1

with open("reuters.json") as data_file:
    data = json.load(data_file)

    for v in data:
	print('Uploading object ' + str(text_id))
        r_id = v['_id']
        r_companies = "'" + v['companies'] + "'"
        r_date = "'" + v['date'] + "'"
        r_exchanges = "'" + v['exchanges'] + "'"
        r_orgs = "'" + v['orgs'] + "'"
        r_people = "'" + v['people'] + "'"
        r_places = "'" + v['places'] + "'"
        r_text_id = text_id

        t_id = text_id
        t_body = "'" + v['text']['body'] + "'"
        t_dateline = "'" + v['text']['dateline'] + "'"
        t_title = "'" + v['text']['title'] + "'"

        r_topics = "'" + v['topics'] + "'"

        text_id += 1

        query = "insert into reuters (id, companies, date, exchanges, orgs, people, places, text_id, topics) values ("
        query += str(r_id) + ", "
        query += r_companies + ", "
        query += r_date + ", "
        query += r_exchanges + ", "
        query += r_orgs + ", "
        query += r_people + ", "
        query += r_places + ", "
        query += str(r_text_id) + ", "
        query += r_topics + ")"

        prepared_stmt = session.prepare(query)
        session.execute(prepared_stmt)

        query = "insert into texts (id, body, dateline, title) values ("
        query += str(t_id) + ", "
        query += t_body + ", "
        query += t_dateline + ", "
        query += t_title + ")"

        prepared_stmt = session.prepare(query)
        session.execute(prepared_stmt)


