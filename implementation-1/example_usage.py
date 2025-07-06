from transactional_db.core import TransactionalDBBuilder


def example_usage():
    db = (
        TransactionalDBBuilder()
        .with_initial_data({"key1": "value1", "key2": "value2"})
        .build()
    )

    print(f"Value for 'key1': {db.get(key='key1')}")
    print(f"Value for 'key2': {db.get(key='key2')}")
    try:
        print(f"Value for 'key3': {db.get(key='key3')}")
    except KeyError as e:
        print(f"KeyError encountered: {e}")
    print("----------------------")
    # starting a transaction using the context manager
    with db.transaction() as txn_id:
        print(f"Transaction started with id: {txn_id}")
        db.set("key", "valval")
        print(f"Value for 'key' inside transaction: {db.get('key')}")

    print(f"Value for 'key' after transaction: {db.get('key')}")
    print("----------------------")
    # Starting another transaction manually
    txn_id = db.begin()
    print(f"Transaction started with id: {txn_id}")
    db.set("key1", "new_value1")
    print(f"Value for 'key1' after begin: {db.get('key1')}")
    db.commit()
    print(f"Value for 'key1' after commit: {db.get('key1')}")


if __name__ == "__main__":
    example_usage()
