import cherrypy
import psycopg2


class Person(object):
    def __init__(self):
        self.albums = Names()

    def _cp_dispatch(self, vpath):
        cherrypy.log( str(vpath) )
        if len(vpath) == 1:
            cherrypy.request.params['lname'] = vpath.pop()
            return self

        if len(vpath) == 3:
            cherrypy.request.params['lname'] = vpath.pop(0)  # /band name/
            vpath.pop(0) # /albums/
            cherrypy.request.params['fname'] = vpath.pop(0) # /album title/
            return self.albums

        return vpath

    @cherrypy.expose
    def fp(self, lname):

        db = MyDB()
        ret_str = "Person: "

        # rows = db.query( "SELECT first_name, last_name, email from auth_user where last_name = %(lname)s ", { 'lname': lname } )
        rows = db.query( "SELECT first_name, last_name, email from auth_user where last_name = %s ", lname )

        cherrypy.log( str(type( rows )) )
        # no rows returned
        if rows == None:
            return "No one by that name in our registry"
        ctr=0
        for row in rows:
            p = '{} {}'.format( row[0], row[1] )
            ret_str = ret_str + str(p) + '</br>'
            ctr+=1
        return ret_str

class Names(object):
    @cherrypy.expose
    def index(self, lname, fname):
        return 'About %s by %s...' % (fname, lname)

class MyDB(object):
    _db_connection = None
    _db_cur = None

    def __init__(self):
        try:
            self._db_connection = psycopg2.connect("port='5432' dbname='rwillard' user='rwillard' host='localhost'")
            cherrypy.log( "connected to the database"  )
        except:
            cherrypy.log("I am unable to connect to the database")

        self._db_cur = self._db_connection.cursor()

    def query(self, query, params):
        tup_param=( params, )
        # return query, params
        cherrypy.log( query )
        # return self._db_cur.execute(query, params)
        self._db_cur.execute( query, tup_param )
        rows=self._db_cur
        if type(rows) is None:
            cherrypy.log("Nothing returned XXXXXXXXXXXXX ")
            return None
        else:
            cherrypy.log( 'query worked' )
            return rows

    def __del__(self):
        self._db_connection.close()

if __name__ == '__main__':
    conf = {
        '/': {
            'tools.sessions.on': True,
            'log.access_file': '/tmp/cherry_access',
            'log.error_file': '/tmp/cherry_error',
        }
    }
    cherrypy.quickstart(Person(), '/', conf )
