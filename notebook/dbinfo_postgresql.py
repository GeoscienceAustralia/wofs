#!/usr/bin/python
"""
Ref: Origined from ubuntu@ip-172-31-15-21:~/github$ cat simple_test_v2.py 
agdc-v2 db

db_hostname: 130.56.244.227
db_database: demo
db_username: cube_user
schema_name: agdc
"""

import sys
import psycopg2

dbhost='130.56.244.227'     #'54.79.91.53'
dbname='demo' # 'gdf_landsat'        #'gdf_test_ls'
dbuser='cube_user'
passwd='GAcube0'

def schema_tables(schname):
    con = None

    try:
        #con = psycopg2.connect(host='10.10.19.65', database='gdf_landsat', user='cube_user', password='GAcube0')
        #con = psycopg2.connect(host='10.10.19.65', database='gdfdev', user='cube_user', password='GAcube0')
        #NCI con = psycopg2.connect(host='130.56.244.228', database='gdf_landsat', user='cube_user', password='GAcube0')
        #con = psycopg2.connect(host='54.79.91.53', database='gdf_test_ls', user='cube_user', password='GAcube0')  #AWS

        con = psycopg2.connect(host=dbhost, database=dbname, user=dbuser, password=passwd)  # connection parametrised

        cur = con.cursor()
        cur.execute('SELECT version()')
        ver = cur.fetchone()
        print "Postgresql server version is:" + str(ver)

        cur.execute('SELECT CURRENT_USER')
        user = cur.fetchone()
        print "Postgresql server login user is:" + str(user)


        #cur.execute("SELECT * FROM platform ")
        #res = cur.fetchall()
        #print "Rows:" + str(res)


#################################################################
        print "Check tables rows in (schema, database, host)=: (%s, %s, %s) *********** " %(schname, dbname,dbhost)
        table_list = get_tables(schname, cur)
        for atable in table_list:
            sqlq="SELECT count(*) FROM %s.%s"%(schname, atable)
            cur.execute( sqlq )
            rows = cur.fetchone()

            # print sqlq, rows
            print atable +" table has rows: %s" % str(rows[0])


    except psycopg2.DatabaseError, e:
        print 'Error %s' % e
        sys.exit(1)

    finally:

        if con:
            con.close()


def get_tables(sname, curs):
    """
    schema public or agdc?
    """
    curs.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='%s'"%(sname))
    result = curs.fetchall()

    table_names=[]

    for item in result:
        table_names.append(item[0])

    table_names.sort()
    return table_names

###################################################################################
# How2un> $ python Zupyternotez/dbinfo_postgresql.py agdc
#
###################################################################################
if __name__ == "__main__":
    
    schname= sys.argv[1]

    schema_tables(schname)
