from dags.python_packages.test_connect_to_db.src.function.connect_test import ConnectTest
from dags.python_packages.test_connect_to_db.src.config.path_config import Path


def main():
    tibero_dbs = ("TIBERO_CMAS_RODB_CONFIG", "TIBERO_CMCA_RODB_CONFIG", "TIBERO_SYMC_RODB_CONFIG")
    query_dir = Path.query_path + "/"

    ct = ConnectTest(tibero_dbs, query_dir)
    ct.select_db_name()

if __name__ == "__main__":
    main()
