# Transactional DB Implementations

This repository has two implementations of a Thread-Safe Key-Value Store with Transaction support, I have highlighted the differences below.

---

## Implementation-1: Strict Serializable

This implementation follows a **`strict serializable isolation level`**. It guarantees that concurrent transactions behave as if they were executed one after another in a specific order that also respects real-time.

This is achieved by **locking the entire database store** as soon as a transaction begins and releasing the lock only when the transaction is committed or rolled back. This approach ensures that only one transaction can run at a time, preventing race conditions.

---

## Implementation-2: Non-locking

This implementation is identical to the first but **does not lock the database store** when a transaction starts. This allows multiple transactions to run concurrently, which can improve performance but introduces the risk of race conditions, as explained I have explained later in this readme. The implementation mostly differ in begin, commit and roll back methods of the TransactionalDB component. Implementation-1 acquires db store lock at begin, and releases it at commit or rollback

---

## Components

The components are identical for both implementations.

### `TransactionalDB`
An interface for all database operations.

### `TransactionalDBBuilder`
A builder for `TransactionalDB` that uses the **Builder Pattern**. It's extensible and ideal for configuring database creation, including support for initial data population.

### `ThreadLocalTransactionStack`
This component maintains a separate transaction stack for each thread using `threading.local()`. It provides helper methods like `push`, `pop`, and `peek` to simplify transaction management within `TransactionalDB`.

This approach is cleaner than using a shared dictionary for transaction stacks, as it avoids potential race conditions and simplifies memory management.

### `Transaction`
A dataclass representing a single transaction, holding all its changes and metadata.

### `KeyDeleteMarker`
A singleton object that acts as a sentinel value to mark a key for deletion within a transaction.

---

## The Problem with Implementation-2: Race Conditions

Allowing concurrent transactions without locking can lead to issues like lost updates.



### Scenario for Race Condition


The database starts with a single key-value pair:
* `{'key1': 'valueA'}`

1.  **Thread A starts a transaction.**
    * It updates `'key1'` to `'newValueA'`.
    * It adds a new key `'key2'` with the value `'value2'`.

2.  **Thread B starts a transaction.**
    * It reads `'key1'`. Since Thread A's transaction has not committed, Thread B reads the original value, `'valueA'`, from the main store.

3.  **Thread A commits.**
    * It acquires the store lock.
    * It writes its changes: `self.store['key1'] = 'newValueA'` and `self.store['key2'] = 'value2'`.
    * It releases the lock.
    * The store is now `{'key1': 'newValueA', 'key2': 'value2'}`.

4.  **Thread B commits.**
    * Its logic, based on the stale value `'valueA'`, determines that `'key1'` should be deleted.
    * It acquires the store lock.
    * It deletes `'key1'`.
    * It releases the lock.

### Final State & Lost Update
The final state of the store is `{'key2': 'value2'}`. Thread A's update to `'key1'` has been lost because Thread B operated on outdated information.

---

## Solutions to Race Conditions

* **Pessimistic Locking (Implementation-1):** Lock the database store as soon as a transaction starts. A more fine-grained approach could involve locking only the specific keys a transaction intends to modify.

* **Optimistic Locking:** Do not acquire locks initially. When a transaction is ready to commit, it first checks if any data it read has been modified by another committed transaction. If so, the current transaction is rolled back. This approach is more performant in low-contention environments.

---

## 🚀 Running the Code

### Running tests
To run the included tests:
```bash
python run_tests.py
```

### Example Usage
To see an example of the database in action:
```bash
python example_usage.py