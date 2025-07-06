import threading
from .constants import KeyDeleteMarker
from .transaction import ThreadLocalTransactionStack
from typing import Dict, Any
from contextlib import contextmanager


class TransactionalDB:
    def __init__(self):
        self.store = {}
        self.store_lock = threading.RLock()
        self.thread_local_stack = ThreadLocalTransactionStack()
        self.DELETED = KeyDeleteMarker()

    """Gets the transaction stack for the current thread"""

    def _get_transaction_stack(self):
        return self.thread_local_stack.get_stack()

    def get(self, key):
        stack = self._get_transaction_stack()
        # Look for the key in the current thread's transactions, from newest to oldest
        for txn in reversed(stack):
            if key in txn.changes:
                value = txn.changes[key]
                if value is self.DELETED:
                    raise KeyError(f"Key '{key}' not found (deleted in transaction)")
                return value
        # If not in a transaction, check the main store
        with self.store_lock:
            if key in self.store:
                return self.store[key]
            raise KeyError(f"Key '{key}' not found")

    def set(self, key, val):
        # If a transaction is active, the change is recorded in the current transaction.
        if self.thread_local_stack.is_any_current_transaction_present():
            self.thread_local_stack.peek_transaction().changes[key] = val
        else:
            # No active transaction, modify the main store directly.
            with self.store_lock:
                self.store[key] = val

    def delete(self, key):
        # If a transaction is active, mark the key as deleted in the current transaction.
        if self.thread_local_stack.is_any_current_transaction_present():
            # If the key exists, we mark it as deleted in the current transaction.
            self.get(key)
            self.thread_local_stack.peek_transaction().changes[key] = self.DELETED
        else:
            # No active transaction, delete the key from the main store.
            with self.store_lock:
                if key not in self.store:
                    raise KeyError(f"Key '{key}' not found")
                del self.store[key]

    def begin(self) -> str:
        return self.thread_local_stack.push_transaction()

    def commit(self):
        # If there are no active transactions, raise an error.
        stack = self._get_transaction_stack()
        if not stack:
            raise ValueError("Nothing to commit; no active transaction")
        # Pop the current transaction from the stack.
        curr_txn = self.thread_local_stack.pop_transaction()

        # If there are no other transactions in the stack, apply changes to the main store.
        # Otherwise, update the parent transaction with the changes.
        if not self.thread_local_stack.is_any_current_transaction_present():
            with self.store_lock:
                for key, value in curr_txn.changes.items():
                    if value is self.DELETED:
                        self.store.pop(key, None)
                    else:
                        self.store[key] = value
        else:
            parent_txn = self.thread_local_stack.peek_transaction()
            parent_txn.update_changes(curr_txn.changes)

    def rollback(self):
        # If there are no active transactions, raise an error.
        if not self.thread_local_stack.is_any_current_transaction_present():
            raise ValueError("Nothing to roll back; no active transaction")
        # Pop the current transaction from the stack, effetively discarding its changes.
        self.thread_local_stack.pop_transaction()

    @contextmanager
    def transaction(self, auto_commit: bool = True):
        txn_id = self.begin()
        try:
            yield txn_id
            if auto_commit:
                self.commit()
        except Exception:
            self.rollback()


class TransactionalDBBuilder:
    def with_initial_data(self, data: Dict[str, Any]) -> "TransactionalDBBuilder":
        self.initial_data = data
        return self

    def build(self) -> TransactionalDB:
        db = TransactionalDB()
        if hasattr(self, "initial_data") and self.initial_data:
            for key, value in self.initial_data.items():
                db.set(key=key, val=value)
        return db
