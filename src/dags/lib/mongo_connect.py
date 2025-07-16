from urllib.parse import quote_plus as quote

from pymongo.mongo_client import MongoClient


# class MongoConnect:
#     def __init__(self,
#                  cert_path: str,
#                  user: str,
#                  pw: str,
#                  host: str,
#                  rs: str,
#                  auth_db: str,
#                  main_db: str
#                  ) -> None:

#         self.user = user
#         self.pw = pw
#         self.host = host
#         self.replica_set = rs
#         self.auth_db = auth_db
#         self.main_db = main_db
#         self.cert_path = cert_path

#     def url(self) -> str:
#         return 'mongodb://{user}:{pw}@{hosts}/?replicaSet={rs}&authSource={auth_src}'.format(
#             user=quote(self.user),
#             pw=quote(self.pw),
#             hosts=self.host,
#             rs=self.replica_set,
#             auth_src=self.auth_db)

#     def client(self):
#         return MongoClient(self.url(), tlsCAFile=self.cert_path)[self.main_db]

# from urllib.parse import quote_plus as quote
# from pymongo.mongo_client import MongoClient


class MongoConnect:
    def __init__(self,
                 cert_path: str,
                 user: str,
                 pw: str,
                 host: str,
                 rs: str,
                 auth_db: str,
                 main_db: str
                 ) -> None:

        self.user = user
        self.pw = pw
        self.host = host
        self.replica_set = rs
        self.auth_db = auth_db
        self.main_db = main_db
        self.cert_path = cert_path
        self._client = None  # Кэш клиента

    def url(self) -> str:
        return 'mongodb://{user}:{pw}@{hosts}/?replicaSet={rs}&authSource={auth_src}'.format(
            user=quote(self.user),
            pw=quote(self.pw),
            hosts=self.host,
            rs=self.replica_set,
            auth_src=self.auth_db)

    def client(self) -> MongoClient:
        if self._client is None:
            self._client = MongoClient(self.url(), tlsCAFile=self.cert_path)
        return self._client

    def get_db(self):
        """Возвращает объект базы данных."""
        return self.client()[self.main_db]

    def get_collection(self, collection_name: str):
        """Возвращает объект коллекции."""
        return self.get_db()[collection_name]

    def close(self):
        """Закрывает соединение с MongoDB."""
        if self._client:
            self._client.close()
            self._client = None
