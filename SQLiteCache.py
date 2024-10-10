#!/usr/bin/python

import os
import errno
import sqlite3
import sys
from time import time
import _pickle as cPickle
from _pickle import loads, dumps, PickleBuffer

import logging
import json
from datetime import datetime

logger = logging.getLogger(__name__)


class SqliteCache:
    """
        SqliteCache

        Ripped heavily from: http://flask.pocoo.org/snippets/87/
        This implementation is a simple Sqlite based cache that
        supports cache timers too. Not specifying a timeout will
        mean that the TITLEue will exist forever.
    """

    # prepared queries for cache operations
    _create_sql = (
        'CREATE TABLE IF NOT EXISTS entries '
        '( KEY TEXT PRIMARY KEY, val BLOB, exp BLOB )'
    )
    _create_sql_reviews = (
        'CREATE TABLE IF NOT EXISTS reviews '
        '( ID TEXT PRIMARY KEY, NAME BLOB, SCORE BLOB, description BLOB, CAT BLOB, VOTES BLOB, PROVIDERKEY BLOB )'
    )
    _create_index = 'CREATE INDEX IF NOT EXISTS keyname_index ON entries (key)'
    _create_index_reviews = 'CREATE INDEX IF NOT EXISTS keyname_index ON reviews (key)'

    _get_sql = 'SELECT val, exp FROM entries WHERE key = ?'
    _get_sql_exp = 'SELECT exp FROM entries WHERE key = ?'
    _del_sql = 'DELETE FROM entries WHERE key = ?'
    _set_sql = 'REPLACE INTO entries (key, val, exp) VALUES (?, ?, ?)'
    _add_sql = 'INSERT INTO entries (key, val, exp) VALUES (?, ?, ?)'
    _clear_sql = "DELETE FROM cache"  # Corrected SQL statement

    _create_sql_stats = '''
    CREATE TABLE IF NOT EXISTS stats (
        key TEXT PRIMARY KEY,
        value TEXT
    )
    '''

    _create_sql_logs = '''
    CREATE TABLE IF NOT EXISTS logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT,
        level TEXT,
        message TEXT
    )
    '''

    _create_sql_omdb = '''
    CREATE TABLE IF NOT EXISTS omdb_cache (
        key TEXT PRIMARY KEY,
        value BLOB,
        expires REAL
    )
    '''

    # other properties
    connection = None

    def __init__(self, db_path):
        self.db_path = db_path
        self._create_tables()

    def _create_tables(self):
        with self._get_conn() as conn:
            # Create entries table if it doesn't exist
            conn.execute('''CREATE TABLE IF NOT EXISTS entries
                            (key TEXT PRIMARY KEY, val BLOB, exp BLOB)''')
            
            # Create logs table if it doesn't exist
            conn.execute('''CREATE TABLE IF NOT EXISTS logs
                            (id INTEGER PRIMARY KEY, timestamp TEXT, level TEXT, message TEXT)''')
            
            # Create stats table if it doesn't exist
            conn.execute('''CREATE TABLE IF NOT EXISTS stats
                            (key TEXT PRIMARY KEY, value TEXT)''')
            
            # Create omdb_cache table if it doesn't exist
            conn.execute('''CREATE TABLE IF NOT EXISTS omdb_cache
                            (key TEXT PRIMARY KEY, value BLOB, expires REAL)''')

    def _get_conn(self):
        conn = sqlite3.connect(self.db_path, timeout=60, check_same_thread=False)
        return conn

    def _create_table(self):
        create_table_sql = '''
        CREATE TABLE IF NOT EXISTS cache (
            key TEXT PRIMARY KEY,
            value BLOB,
            expires REAL
        )
        '''
        self.conn.execute(create_table_sql)
        self.conn.commit()

    def get(self, key):

        """ Retreive a value from the Cache """

        return_value = None
        key = key.lower()

        # get a connection to run the lookup query with
        with self._get_conn() as conn:

            # loop the response rows looking for a result
            # that is not expired
            for row in conn.execute(self._get_sql, (key,)):
                #return_value = loads(row[0])

                expire = loads(row[1])

                #xbmc.executebuiltin('Notification(%s,%s,3000,%s)' % ('expiry of {k} {kk}'.format(k=key, kk=provider), expire , ADDON.getAddonInfo('icon')))

                if expire == 0 or expire > time():
                    return_value = loads(row[0])
                    #xbmc.executebuiltin('Notification(%s,%s,3000,%s)' % ('Cach for %s' % key, return_value , ADDON.getAddonInfo('icon')))
                    # TODO: Delete the value that is expired?
                else:
                    self.delete(key)
                    return_value = None
                break

        return return_value

    def get_exp(self, key):
        return_value = None
        key = key.lower()

        with self._get_conn() as conn:
            #for row in conn.execute(self._get_sql_exp, (key,)):
                try:
                    expire = loads(conn.execute(self._get_sql_exp, (key,)))
                except:
                    expire = "No Result"

        return expire

    def delete(self, key):

        """ Delete a cache entry """

        with self._get_conn() as conn:
            conn.execute(self._del_sql, (key,))

    def update(self, key, show_info, timeout=None):
        """ Sets a k,v pair with an optional timeout """

        Default_caching_period = 30*24*60*60  # 30 days
        expire = time() + Default_caching_period if timeout is None else time() + float(timeout)

        # Serialize the value
        val = PickleBuffer(dumps(show_info))
        expire = PickleBuffer(dumps(expire))

        # Write the updated value to the db
        with self._get_conn() as conn:
            try:
                conn.execute(self._set_sql, (key, val, expire))
                if isinstance(show_info, dict):
                    logger.info(f"Successfully updated results in cache for [{show_info.get('title', 'Unknown')}] [{show_info.get('provider', 'Unknown')}]")
                else:
                    logger.info(f"Successfully updated results in cache for key: {key}")
            except:
                logger.info(f"Failed to update results in cache for key: {key}")

    def set(self, key, show_info, timeout=None):
        """ Adds a k,v pair with an optional timeout """

        try:
            if isinstance(show_info, dict):
                logger.info(f"Trying to save results to cache for [{show_info.get('title', 'Unknown')}] [{show_info.get('provider', 'Unknown')}]")
            else:
                logger.info(f"Trying to save results to cache for key: {key}")
        except:
            logger.info("Failed to log save attempt details")

        Default_caching_period = 30*24*60*60  # 30 days

        # Check if timeout is a dictionary and extract the value if it is
        if isinstance(timeout, dict):
            timeout = timeout.get('timeout', None)
        
        expire = time() + Default_caching_period if timeout is None else time() + float(timeout)

        # Serialize the value
        val = PickleBuffer(dumps(show_info))
        expire2 = PickleBuffer(dumps(expire))

        # Adding a new entry that may cause a duplicate key error if the key already exists.
        # In this case, we will fall back to the update method.
        with self._get_conn() as conn:
            try:
                conn.execute(self._add_sql, (key, val, expire2))
            except sqlite3.IntegrityError:
                # Call the update method as fallback
                logger.info(f'Attempting to set an existing key {key}. Falling back to update method.')
                self.update(key, show_info, timeout)

    def clear(self):
        try:
            conn = self._get_conn()
            with conn:
                conn.execute("DELETE FROM entries")
                conn.execute("DELETE FROM stats")
                conn.execute("DELETE FROM logs")
                conn.execute("DELETE FROM omdb_cache")
            logger.info('Cache cleared successfully')
        except Exception as e:
            logger.error(f"Failed to clear cache: {e}")
            raise

    def __del__(self):

        """ Cleans up the object by destroying the sqlite connection """

        if self.connection:
            self.connection.close()

    def get_cached_records_count(self):
        with self._get_conn() as conn:
            count = conn.execute("SELECT COUNT(*) FROM entries").fetchone()[0]
            logger.info(f"Cached records count: {count}")
            return count

    # Stats methods
    def set_stat(self, key, value):
        with self._get_conn() as conn:
            conn.execute("INSERT OR REPLACE INTO stats (key, value) VALUES (?, ?)", (key, json.dumps(value)))

    def get_stat(self, key):
        with self._get_conn() as conn:
            cursor = conn.execute("SELECT value FROM stats WHERE key = ?", (key,))
            result = cursor.fetchone()
            return json.loads(result[0]) if result else None

    def get_all_stats(self):
        try:
            with self._get_conn() as conn:
                cursor = conn.execute("SELECT key, value FROM stats")
                return {key: json.loads(value) for key, value in cursor.fetchall()}
        except sqlite3.Error as e:
            logger.error(f"Error getting all stats: {e}")
            return {}  # Return an empty dict if there's an error

    # Logs methods
    def add_log(self, level, message):
        timestamp = datetime.now().isoformat()
        with self._get_conn() as conn:
            conn.execute("INSERT INTO logs (timestamp, level, message) VALUES (?, ?, ?)", (timestamp, level, message))

    def get_logs(self, limit=100, offset=0):
        with self._get_conn() as conn:
            cursor = conn.execute("SELECT * FROM logs ORDER BY timestamp DESC LIMIT ? OFFSET ?", (limit, offset))
            return cursor.fetchall()

    def get_omdb_cache(self, key):
        with self._get_conn() as conn:
            cursor = conn.execute("SELECT value, expires FROM omdb_cache WHERE key = ?", (key,))
            result = cursor.fetchone()
            if result:
                value, expires = result
                if expires == 0 or expires > time():
                    return loads(value)
        return None

    def set_omdb_cache(self, key, value):
        expire = time() + 365 * 24 * 60 * 60  # 1 year expiry
        val = PickleBuffer(dumps(value))
        with self._get_conn() as conn:
            conn.execute("INSERT OR REPLACE INTO omdb_cache (key, value, expires) VALUES (?, ?, ?)", 
                         (key, val, expire))

    def clear_logs(self):
        with self._get_conn() as conn:
            conn.execute("DELETE FROM logs")

    def clear_stats(self):
        with self._get_conn() as conn:
            conn.execute("DELETE FROM stats")

    def get_logs_count(self):
        with self._get_conn() as conn:
            return conn.execute("SELECT COUNT(*) FROM logs").fetchone()[0]

    def get_stats_count(self):
        with self._get_conn() as conn:
            return conn.execute("SELECT COUNT(*) FROM stats").fetchone()[0]

    def get_cached_records_count(self):
        with self._get_conn() as conn:
            return conn.execute("SELECT COUNT(*) FROM entries").fetchone()[0]

    def ensure_omdb_cache_table(self):
        with self._get_conn() as conn:
            conn.execute('''CREATE TABLE IF NOT EXISTS omdb_cache
                            (key TEXT PRIMARY KEY, value BLOB, expires REAL)''')

# allow this module to be used to clear the cache
if __name__ == '__main__':
    logger.info('ParentalGuide Cache Initiated')
    
    # Clear cache if no arguments or if 'clear' is specified
    if len(sys.argv) == 1 or (len(sys.argv) == 2 and sys.argv[1] == 'clear'):
        # Use a default path or get it from an environment variable
        default_db_path = os.environ.get('SQLITE_DB_PATH', 'cache.sqlite')
        c = SqliteCache(default_db_path)
        c.clear()
        print(f' * Cache cleared (Database: {default_db_path})')
    else:
        print('[!] Usage: python %s [clear]' % sys.argv[0])
        print('    Running without arguments or with "clear" will clear the cache.')
        sys.exit(1)