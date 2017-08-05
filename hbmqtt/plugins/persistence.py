# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
import asyncio
import sqlite3
import pickle


class SQLitePlugin:
    def __init__(self, context):
        self.context = context
        self.conn = None
        self.cursor = None
        self.db_file = None
        try:
            self.persistence_config = self.context.config['persistence']
            self.init_db()
        except KeyError:
            self.context.logger.warning("'persistence' section not found in context configuration")

    def init_db(self):
        self.db_file = self.persistence_config.get('file', None)
        if not self.db_file:
            self.context.logger.warning("'file' persistence parameter not found")
        else:
            try:
                self.conn = sqlite3.connect(self.db_file)
                self.cursor = self.conn.cursor()
                self.context.logger.info("Database file '%s' opened" % self.db_file)
            except Exception as e:
                self.context.logger.error("Error while initializing database '%s' : %s" % (self.db_file, e))
        if self.cursor:
            self.cursor.execute("CREATE TABLE IF NOT EXISTS session(client_id TEXT PRIMARY KEY, data BLOB)")

    @asyncio.coroutine
    def save_session(self, session):
        if self.cursor:
            dump = pickle.dumps(session)
            try:
                self.cursor.execute(
                    "INSERT OR REPLACE INTO session (client_id, data) VALUES (?,?)", (session.client_id, dump))
                self.conn.commit()
            except Exception as e:
                self.context.logger.error("Failed saving session '%s': %s" % (session, e))

    @asyncio.coroutine
    def find_session(self, client_id):
        if self.cursor:
            row = self.cursor.execute("SELECT data FROM session where client_id=?", (client_id,)).fetchone()
            if row:
                return pickle.loads(row[0])
            else:
                return None

    @asyncio.coroutine
    def del_session(self, client_id):
        if self.cursor:
            self.cursor.execute("DELETE FROM session where client_id=?", (client_id,))
            self.conn.commit()

    @asyncio.coroutine
    def on_broker_post_shutdown(self):
        if self.conn:
            self.conn.close()
            self.context.logger.info("Database file '%s' closed" % self.db_file)
