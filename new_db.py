import psycopg2
import sys, os

class MyDB(object):
    _db_connection = None
    _db_cur = None

    # upon init establish connection and cursor for connection
    def __init__(self):
        # try:
        # conn = psycopg2.connect("port='5432' dbname='rwillard' user='rwillard' host='localhost'")
        # except:
            # print "I am unable to connect to the database"
        try:
            self._db_connection = psycopg2.connect("port='5432' dbname='rwillard' user='rwillard' host='localhost'")
            print "connected to the database" 
        except:
            print "I am unable to connect to the database" 

        try:
            self._db_cur = self._db_connection.cursor()
        except:
            print "Unable to get cursor"
        try:
            self._db_cur.execute("SELECT * FROM AUTH_USER;")
            print self._db_cur.description
        except:
            print "Unexpected error:", sys.exc_info()[0]
            raise
            return
        # print 'hello'
        # for row in self._db_cur:
            # print row

    def run(self):
        return self._db_cur.execute("SELECT * FROM AUTH_USER;")

    def query(self, query, params):
        print query, params

        tup_param=( params, )
        # rows=self._db_cur.execute(query, tup_param )

        try:
            self._db_cur.execute( query, tup_param )
            rows=self._db_cur
            # print type( self._db_cur )
        except:
            print "Error getting rows"

        if rows is None:
            print "Nothing returned XXXXXXXXXXXXX "
            return None
        else:
            # print type( rows ), len(rows )
            return rows

    def __del__(self):
        self._db_connection.close()

if __name__ == '__main__':
    in_name=str(raw_input("Please enter a name: "))
    db = MyDB()

    rows = db.query('select * from auth_user where last_name = %s',  in_name )
    
    for row in rows:
        print row[0], row[1]
