import jaydebeapi as jdbc
import json
import logging
from python_packages.test_connect_to_db.src.config.path_config import Path
from python_packages.test_connect_to_db.src.log.logger import Logger

class DBConnector :
    def __init__(self, db_type, config_path=f"{Path.config_path}/config.json"):
        with open(config_path, 'r') as f:
            config = json.load(f)[db_type]

        self.driver = config["driver"]
        self.url = config["url"]
        self.user = config["user"]
        self.password = config["password"]
        self.jdbc_driver = config["jdbcDriver"]
        self.log = Logger(name="Phis_to_Aichatbot-DBConnector", log_file=f"{Path.log_path}/Phis_to_Aichatbot.log",level=logging.INFO).get_logger()

        self.connect()

    def connect(self):
        try:
           self.con = jdbc.connect(
               self.driver,
               self.url,
               [self.user, self.password],
               jars=self.jdbc_driver
           )
           self.con.jconn.setAutoCommit(False)

        except Exception as e:
            self.log.warning(f"Connection error for {self.driver} : {e}")
            raise

    def select_fetchall(self, sql_text, bind_params=None):
        cursor = None
        try:
            cursor = self.con.cursor()
            if bind_params:
                cursor.execute(sql_text, bind_params)
            else:
                cursor.execute(sql_text)
            rows = cursor.fetchall()
            column_names = [desc[0] for desc in cursor.description]
            return rows, column_names
        except Exception as e:
            self.log.warning(f"Error during select : {e}")
            return None, None
        finally:
            if cursor:
                cursor.close()

    def execute_many(self, sql_text, rows, batch_size=1000):
        cursor = None
        try:
            cursor = self.con.cursor()
            for i in range(0, len(rows), batch_size):
                # batch_size 단위로 데이터를 나눠서 처리
                batch = rows[i:i + batch_size]
                self.log.info(f"Executing batch {i // batch_size + 1} with {len(batch)} records.")
                cursor.executemany(sql_text, batch)
                # 각 배치 처리 후 커밋
                self.con.commit()
                self.log.info(f"Batch {i // batch_size + 1} committed successfully.")

        except jdbc.Error as e:
            self.log.warning(f"Database error during batch execution: {e}")
            self.con.rollback()  # 오류 발생 시 롤백
        except TypeError as e:
            self.log.warning(f"Type error during batch execution: {e}")
        finally:
            if cursor:
                cursor.close()

    def execute(self, sql_text):
        cursor = None
        try:
            cursor = self.con.cursor()
            cursor.execute(sql_text)
            self.con.commit()
        except TypeError as e:
            self.log.warning(f"Error while run execute to DBMS: {e}")
        finally:
            if cursor:
                cursor.close()

    def close(self):
        if self.con:
            self.con.close()