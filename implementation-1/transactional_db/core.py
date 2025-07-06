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

    def _get_transaction_stack(self):
        return self.thread_local_stack.get_stack()

    def get(self, key):
        """
        Gets the value of a key.

        Checks active transactions from newest to oldest first, then the main store.
        Raises a KeyError if the key doesn't exist or was deleted in a transaction.
        """
        stack = self._get_transaction_stack()
        for txn in reversed(stack):
            if key in txn.changes:
                value = txn.changes[key]
                if value is self.DELETED:
                    raise KeyError(f"Key '{key}' not found (deleted in transaction)")
                return value
        with self.store_lock:
            if key in self.store:
                return self.store[key]
            raise KeyError(f"Key '{key}' not found")

    def set(self, key, val):
        """
        Sets a key to a value.

        If in a transaction, the change is logged to the transaction.
        Otherwise, it writes directly to the main store.
        """
        if self.thread_local_stack.is_any_current_transaction_present():
            self.thread_local_stack.peek_transaction().changes[key] = val
        else:
            with self.store_lock:
                self.store[key] = val

    def delete(self, key):
        """
        Deletes a key.

        Checks if the key exists first. If in a transaction, it's marked
        for deletion. Otherwise, it's removed directly from the main store.
        """
        if self.thread_local_stack.is_any_current_transaction_present():
            self.get(key)
            self.thread_local_stack.peek_transaction().changes[key] = self.DELETED
        else:
            with self.store_lock:
                if key not in self.store:
                    raise KeyError(f"Key '{key}' not found")
                del self.store[key]

    def begin(self) -> str:
        """
        Begins a new transaction.

        If this is the outermost transaction, a lock is acquired on the store.
        """
        is_outermost_transaction = (
            not self.thread_local_stack.is_any_current_transaction_present()
        )
        if is_outermost_transaction:
            self.store_lock.acquire()

        return self.thread_local_stack.push_transaction()

    def commit(self):
        """
        Commits the most recent transaction.

        For a nested transaction, changes are merged with the parent.
        For an outermost transaction, changes are written to the main store.
        Once the commit is done, the lock is released if this was the outermost transaction.
        """
        stack = self._get_transaction_stack()
        if not stack:
            raise ValueError("Nothing to commit; no active transaction")

        try:
            curr_txn = self.thread_local_stack.pop_transaction()
            is_outermost_transaction = (
                not self.thread_local_stack.is_any_current_transaction_present()
            )

            if is_outermost_transaction:
                # Apply changes directly to the main store
                for key, value in curr_txn.changes.items():
                    if value is self.DELETED:
                        self.store.pop(key, None)
                    else:
                        self.store[key] = value
            else:
                # Merge changes into the parent transaction
                parent_txn = self.thread_local_stack.peek_transaction()
                parent_txn.update_changes(curr_txn.changes)
        finally:
            if not self.thread_local_stack.is_any_current_transaction_present():
                self.store_lock.release()

    def rollback(self):
        """
        Rolls back the most recent transaction, discarding its changes.
        If this was the outermost transaction, the lock is released.
        """
        if not self.thread_local_stack.is_any_current_transaction_present():
            raise ValueError("Nothing to roll back; no active transaction")

        try:
            self.thread_local_stack.pop_transaction()
        finally:
            if not self.thread_local_stack.is_any_current_transaction_present():
                self.store_lock.release()

    @contextmanager
    def transaction(self, auto_commit: bool = True):
        """
        A context manager for transactions.

        Automatically begins a transaction and will commit on a clean exit if specified.
        Rolls back if an exception occurs.
        """
        txn_id = self.begin()
        try:
            yield txn_id
            if auto_commit:
                self.commit()
        except Exception:
            self.rollback()
            raise


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
