from dags.python_packages.test_connect_to_db.src.query_loader import QueryLoader
from dags.python_packages.test_connect_to_db.src.config.path_config import Path
from dags.python_packages.test_connect_to_db.src.log.logger import Logger
from dags.python_packages.test_connect_to_db.src.connector.db_connector import DBConnector

import logging

class ConnectTest:
    def __init__(self, tibero_dbs, query_dir):
        self.tibero_dbs = tibero_dbs
        self.query_dir = query_dir
        self.load_queries()
        self.log = Logger(name="Phis_to_Aichatbot-UserInfoSyncManager", log_file=f"{Path.log_path}/Phis_to_Aichatbot.log", level=logging.INFO).get_logger()

    def load_queries(self):
        query_loader = QueryLoader(self.query_dir)
        self.select_database_name = query_loader.load_query('select_database_name')

    def select_db_name(self):
        for tibero_dsn in self.tibero_dbs:
            self.log.info(f"[INFO] START select_database_name DB : {tibero_dsn}")
            tibero_connector = DBConnector(tibero_dsn)
            rows, columns = tibero_connector.select_fetchall(self.select_database_name)

            self.log.info(f"{columns}")
            for row in rows:
                self.log.info(f"{row}")

            tibero_connector.close()
            self.log.info(f"[INFO] END select_database_name DB: {tibero_dsn}")

