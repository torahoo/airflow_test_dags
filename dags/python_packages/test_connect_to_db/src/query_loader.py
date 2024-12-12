import os


class QueryLoader:
    def __init__(self, query_dir="queries"):
        self.query_dir = query_dir

    def load_query(self, query_name):
        cwd = os.getcwd()
        print(cwd)
        file_path = os.path.join(self.query_dir, f"{query_name}.sql")
        # file_path = os.path.join("./query/", f"{query_name}.sql")
        with open(file_path, "r", encoding="utf-8") as file:
            return file.read()


