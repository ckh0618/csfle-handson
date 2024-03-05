import bson
from bson import CodecOptions, STANDARD
from mongoengine import *
from pymongo.encryption import ClientEncryption
from pymongo.encryption_options import AutoEncryptionOpts

from credentials import get_credentials
from pymongo import *

from pprint import pprint

_key_vault_database = "encryption"
_key_value_collection = "__keyVault"


def get_kms_provider(local_key_path):
    with open(local_key_path, "rb") as f:
        local_master_key = f.read()

    return {
        "local": {
            "key": local_master_key  # local_master_key variable from the previous step
        },
    }


def get_key_id(keyVaultDB, keyVaultCol):
    client = MongoClient(get_credentials()["MONGODB_URI"])

    client_encryption = ClientEncryption(
        get_kms_provider("./cmk.key"),  # pass in the kms_providers variable from the previous step
        keyVaultDB + "." + keyVaultCol,
        client,
        CodecOptions(uuid_representation=STANDARD),
    )

    key_id = client_encryption.get_key_by_alt_name("demo-data-key").get("_id")

    return key_id


def define_encryption_schema(keyid):
    return {
        "bsonType": "object",
        "encryptMetadata": {"keyId": "/dek_alt_name"},
        "properties": {
            "name": {
                "encrypt": {
                    "bsonType": "string",
                    "algorithm": "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                }
            },
            "ssn": {
                "encrypt": {
                    "bsonType": "string",
                    "algorithm": "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                }
            },
            "salary": {
                "encrypt": {
                    "bsonType": "int",
                    "algorithm": "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                }
            },
        },
    }


def build_csfle_options():
    kms_providers = get_kms_provider("./cmk.key")
    key_vault_namespace = _key_vault_database + "." + _key_value_collection
    keyid = get_key_id(_key_vault_database, _key_value_collection)
    people_schema = define_encryption_schema(keyid)

    schema = {
        "test.automatic_encryption": people_schema
    }

    extra_options = {"crypt_shared_lib_path": get_credentials()["SHARED_LIB_PATH"]}

    fle_opts = AutoEncryptionOpts(
        kms_providers, key_vault_namespace, schema_map=schema, **extra_options
    )
    return fle_opts


def insert(client: MongoClient):
    coll = client.get_database("test").get_collection("automatic_encryption")
    coll.drop()
    dict = {}
    dict["name"] = "John"
    dict["ssn"] = "!232-21312223"
    dict["salary"] = 10000
    dict["dek_alt_name"] = "demo-data-key"

    coll.insert_one(dict)

def findAll ( client: MongoClient) :
    coll = client.get_database("test").get_collection("automatic_encryption")
    cur = coll.find()

    for doc in cur :
        pprint ( doc )



def main():
    fle_opts = build_csfle_options()
    secureClient = MongoClient(get_credentials()["MONGODB_URI"], auto_encryption_opts=fle_opts)
    normalClient = MongoClient(get_credentials()["MONGODB_URI"])

    insert(secureClient)
    findAll(secureClient)


    findAll(normalClient)


main()
