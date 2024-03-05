import os
import base64
from bson import STANDARD, CodecOptions
from pymongo import *
from pymongo.encryption import ClientEncryption

import credentials


def gen_cmk(path):
    file_bytes = os.urandom(96)
    with open(path, "wb") as f:
        f.write(file_bytes)


def get_key_provider(path):
    with open(path, "rb") as f:
        local_master_key = f.read()
    return {
        "local": {
            "key": local_master_key  # local_master_key variable from the previous step
        },
    }


def initialize_key_vault():
    key_vault_coll = "__keyVault"
    key_vault_db = "encryption"
    key_vault_client = MongoClient(credentials.get_credentials()["MONGODB_URI"])

    key_vault_client.drop_database(key_vault_db)

    key_vault_client["test"].drop_collection("people")
    key_vault_client[key_vault_db][key_vault_coll].create_index(
        [("keyAltNames", ASCENDING)],
        unique=True,
        partialFilterExpression={"keyAltNames": {"$exists": True}},
    )


def register_dek_from_cmk(altname):
    client = MongoClient(credentials.get_credentials()["MONGODB_URI"])
    key_vault_coll = "__keyVault"
    key_vault_db = "encryption"
    key_vault_namespace = f"{key_vault_db}.{key_vault_coll}"

    client_encryption = ClientEncryption(
        get_key_provider("./cmk.key"),  # pass in the kms_providers variable from the previous step
        key_vault_namespace,
        client,
        CodecOptions(uuid_representation=STANDARD),
    )

    data_key_id = client_encryption.create_data_key(
        "local", key_alt_names=[altname]
    )

    data_key_id = client_encryption.get_key_by_alt_name(altname).get('_id')

    base_64_data_key_id = base64.b64encode(data_key_id)
    print("DataKeyId [base64]: ", base_64_data_key_id)


def main():
    gen_cmk("./cmk.key")
    initialize_key_vault()
    register_dek_from_cmk("demo-data-key")

main()