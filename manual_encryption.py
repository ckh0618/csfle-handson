from bson import CodecOptions, STANDARD
from pymongo import MongoClient
from pymongo.encryption import ClientEncryption, Algorithm
from credentials import get_credentials
from pprint import pprint

client = MongoClient(get_credentials()["MONGODB_URI"])


def get_kms_provider(local_key_path):
    with open(local_key_path, "rb") as f:
        local_master_key = f.read()

    return {
        "local": {
            "key": local_master_key  # local_master_key variable from the previous step
        },
    }


def get_client_encryption(key_database, key_collection):
    client_encryption = ClientEncryption(
        get_kms_provider("./cmk.key"),  # pass in the kms_providers variable from the previous step
        key_database + "." + key_collection,
        client,
        CodecOptions(uuid_representation=STANDARD),
    )

    return client_encryption


def insert(db_name, col_name):
    col = client.get_database(db_name).get_collection(col_name)

    ## Initilaize Client Encryption
    client_encryption = get_client_encryption("encryption", "__keyVault")
    dek_key_id = client_encryption.get_key_by_alt_name("demo-data-key").get('_id')

    encrypted_name = client_encryption.encrypt(
        "Ivan",
        Algorithm.AEAD_AES_256_CBC_HMAC_SHA_512_Deterministic,
        key_id=dek_key_id,  # DEK UUID
    )
    encrypted_foods = client_encryption.encrypt(
        ["Pizza", "Beer", "Salad"],
        Algorithm.AEAD_AES_256_CBC_HMAC_SHA_512_Random,
        key_id=dek_key_id,  # DEK UUID
    )

    col.insert_one( { "name" : encrypted_name, "foods" : encrypted_foods})


def find(db_name, col_name) :
    col = client.get_database(db_name).get_collection(col_name)

    ## Initilaize Client Encryption
    client_encryption = get_client_encryption("encryption", "__keyVault")
    dek_key_id = client_encryption.get_key_by_alt_name("demo-data-key").get('_id')

    encrypted_name = client_encryption.encrypt(
        "Ivan",
        Algorithm.AEAD_AES_256_CBC_HMAC_SHA_512_Deterministic,
        key_id=dek_key_id,  # DEK UUID
    )

    encrypted_doc = col.find_one( {"name" : encrypted_name })
    pprint(encrypted_doc)

    plain_doc = {"name": client_encryption.decrypt(encrypted_doc["name"]),
                    "foods": client_encryption.decrypt(encrypted_doc["foods"])}

    pprint ( plain_doc )


def main () :
    #insert("test", "manual_encryption")
    find("test", "manual_encryption")



main()