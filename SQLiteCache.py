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

    # other properties
    connection = None

    def __init__(self):
        temp_dir = os.getcwd()
        logger.info('temp dir is ' + temp_dir)
        cache_dir = os.path.join(temp_dir, 'cache')

        isExist = os.path.exists(cache_dir)

        if not isExist:
            os.makedirs(cache_dir)
        logger.info('the cache dir is ' + cache_dir)

        self.path = cache_dir


    def _get_conn(self):

        """ Returns a Sqlite connection """

        if self.connection:
            return self.connection

        # specify where we want the cache db to live
        cache_db_path = os.path.join(self.path, 'cache.sqlite')
        self.cache_db_path = cache_db_path
    
        # setup the connection
        conn = sqlite3.Connection(cache_db_path, timeout=60, check_same_thread=False)
        logger.debug('Connected to {path}'.format(path=cache_db_path))

        # ensure that the table schema is available. The
        # 'IF NOT EXISTS' in the create_sql should be
        # pretty self explanitory
        with conn:
            conn.execute(self._create_sql)
            conn.execute(self._create_index)
            logger.debug('Ran the create table && index SQL.')

        # set the connection property
        self.connection = conn

        # return the connection
        return self.connection

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

    def _get_conn(self):

        """ Returns a Sqlite connection """

        if self.connection:
            return self.connection

        # specify where we want the cache db to live
        cache_db_path = os.path.join(self.path, 'cache.sqlite')

        # setup the connection
        conn = sqlite3.Connection(cache_db_path, timeout=60, check_same_thread=False)
        logger.debug('Connected to {path}'.format(path=cache_db_path))

        # ensure that the table schema is available. The
        # 'IF NOT EXISTS' in the create_sql should be
        # pretty self explanitory
        with conn:
            conn.execute(self._create_sql)
            conn.execute(self._create_index)
            logger.debug('Ran the create table && index SQL.')

        # set the connection property
        self.connection = conn

        # return the connection
        return self.connection

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

        """ Clear a cache """

        try:
            # Check if the directory exists
            
            db_dir = os.path.dirname(self.path)
            if not os.path.exists(db_dir):
                os.makedirs(db_dir)
                logging.info(f"Created directory: {db_dir}")

            # Try to open the database
            conn = sqlite3.Connection(self.path, timeout=60, check_same_thread=False)
            # ... rest of the method ...
        except sqlite3.OperationalError as e:
            logging.error(f"Failed to open database: {e}")
            logging.error(f"Database path: {self.path}")
            logging.error(f"Current working directory: {os.getcwd()}")
            raise  # Re-raise the exception after logging

        self._clear_sql = "DELETE FROM cache"

        conn = sqlite3.Connection(cache_db_path, timeout=60, check_same_thread=False)
        
        with conn:
            conn.execute(self._create_sql)          
            logger.info('Cache cleared sucessfully')

    def __del__(self):

        """ Cleans up the object by destroying the sqlite connection """

        if self.connection:
            self.connection.close()

    def get_cached_records_count(self):
        """Returns the total number of cached records"""
        try:
            with self._get_conn() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM entries")
                count = cursor.fetchone()[0]
                return count
        except sqlite3.Error as e:
            logger.error(f"Error getting cached records count: {e}")
            return 0

# allow this module to be used to clear the cache
if __name__ == '__main__':
    logger.info('ParentalGuide Cache Initiated')
    
    # Clear cache if no arguments or if 'clear' is specified
    if len(sys.argv) == 1 or (len(sys.argv) == 2 and sys.argv[1] == 'clear'):
        c = SqliteCache()  # Use current directory as base path
        c.clear()
        print(' * Cache cleared')
    else:
        print('[!] Usage: python %s [clear]' % sys.argv[0])
        print('    Running without arguments or with "clear" will clear the cache.')
        sys.exit(1)
