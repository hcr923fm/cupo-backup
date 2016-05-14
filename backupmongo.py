from pymongo import MongoClient


# Archive records are organised by the path of the origin directory, with sub-entries for each individual version
# of the uploaded archive.
# Use the format:
#
# db.archives.insert_one({
#     "path": "/path/to/archived/subdir",
#     "vault":
#     "versions":
#         [
#             {
#                 "_id": "AWS-ARCHIVE-ID-GOES-HERE-ABCDEFHGIJKLMNOPQRSTUVWXYZ0123456789",
#                 "treehash": "SHA256-TREEHASH-GOES-HERE-ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789",
#                 "size": 123456789,
#                 "uploaded_time": 147258369,
#                 "aws_URI": "aws://AWS-ARCHIVE-URI-GOES-HERE-ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789",
#                 "to_delete": False
#             },
#             {
#                 "_id": "AWS-ARCHIVE-ID-GOES-HERE-ABCDEFHGIJKLMNOPQRSTUVWXYZ0123456789",
#                 "treehash": "SHA256-TREEHASH-GOES-HERE-ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789",
#                 "size": 987654321,
#                 "uploaded_time": 369258147,
#                 "aws_URI": "aws://AWS-ARCHIVE-URI-GOES-HERE-ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789",
#                 "to_delete": False
#             }
#         ]
# })
#
#
# db.vaults.insert_one({
#     "arn": "aws://AWS-VAULT-ARN-123456789",
#     "name": "vault name",
#     "region": "eu-west-1"
# })


def create_backup_database(database_name, db_client, drop_existing=True):
    """
    Creates a MongoDB database `database_name` that is ready to be used as a backup tracking
    database for this software.
    :param database_name The name of the database we'll use
    :param host The hostname of the server running MongoDB
    :param port The port number to connect to `host` on
    :param drop_existing If true, drop any existing database with this name
    """

    if drop_existing and (database_name in db_client.database_names()):
        db_client.drop_database(database_name)

    db = db_client[database_name]
    db.create_collection('archives')
    db.create_collection('vaults')

    return db

def create_vault_entry(path, )


def connect(database_name, host="localhost", port=27017):
    mongodb_uri = "mongo://{host}:{port}".format(host=host, port=port)
    client = MongoClient(mongodb_uri)

    if database_name in client.database_names():
        db = client[database_name]
    else:
        db = create_backup_database(database_name, client)

    return client, db


def disconnect(client):
    client.close()
