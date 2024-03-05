import bson
from bson import CodecOptions, STANDARD
from mongoengine import *
from pymongo.encryption import ClientEncryption
from pymongo.encryption_options import AutoEncryptionOpts

from credentials import _credentials
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
    client = MongoClient(_credentials["MONGODB_URI"])

    client_encryption = ClientEncryption(
        get_kms_provider("./cmk.key"),  # pass in the kms_providers variable from the previous step
        keyVaultDB + "." + keyVaultCol,
        client,
        CodecOptions(uuid_representation=STANDARD),
    )

    key_id = client_encryption.get_key_by_alt_name("demo-data-key").get("_id")

    return key_id


def define_encryption_schema(keyid):
    key1 = get_key_id(_key_vault_database, _key_value_collection)
    print(key1)
    return {
        "bsonType": "object",
        "encryptMetadata": {"keyId": [key1]},
        "properties": {
            "name": {
                "encrypt": {
                    "bsonType": "string",
                    "algorithm": "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic",
                }
            },
            "ssn": {
                "encrypt": {
                    "bsonType": "string",
                    "algorithm": "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic",
                }
            },
            "salary": {
                "encrypt": {
                    "bsonType": "long",
                    "algorithm": "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic",
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
        "test.person": people_schema
    }

    extra_options = {"crypt_shared_lib_path": _credentials["SHARED_LIB_PATH"]}

    # start-client
    fle_opts = AutoEncryptionOpts(
        kms_providers, key_vault_namespace, schema_map=schema, **extra_options
    )

    connect("test", DEFAULT_CONNECTION_NAME, host="mongodb+srv://csfle:csfle00@csfle.ewcj8.mongodb.net",
            auto_encryption_opts=fle_opts)


class Person(Document):
    name = StringField(required=True)
    ssn = StringField(required=True)
    mobile = StringField(required=True)
    salary = LongField(required=True)


def insert_new_person(name, ssn, mobile, salary):
    p = Person(name=name, ssn=ssn, mobile=mobile, salary=salary)
    p.save()


def get_all_persons():
    for p2 in Person.objects:
        print(p2.name)


def update_one_person():
    for p2 in Person.objects(name="Almond")[:1]:
        print(p2.name)

        p2.name = "Albert"
        p2.save()

    for p3 in Person.objects(name="Albert")[:1]:
        print(p2.name)


def remove_all_persons():
    Person.drop_collection()


def main():
    build_csfle_options()

    remove_all_persons()

    insert_new_person(name="Almond", ssn="111-222-333", mobile="000-111-222-444", salary=100000)
    insert_new_person(name="Jane", ssn="222-222-333", mobile="000-111-222-442", salary=200000)
    get_all_persons()
    update_one_person()


main()
