import threading
import time
from dataclasses import dataclass
from typing import Dict, Any, List


@dataclass
class Transaction:
    id: str
    changes: Dict[str, Any]

    def update_changes(self, changes: Dict[str, Any]):
        self.changes.update(changes)


"""A class to manage a stack of transactions for each thread.
This class uses thread-local storage to maintain a stack of transactions for each thread.
Each transaction is represented by a Transaction object, which contains an ID, a dictionary of changes"""


class ThreadLocalTransactionStack:
    def __init__(self):
        self._local = threading.local()
        self._thread_counter = 0
        self._counter_lock = threading.Lock()

    def get_stack(self) -> List[Transaction]:
        if not hasattr(self._local, "stack"):
            self._local.stack = []
        return self._local.stack

    def pop_transaction(self):
        stack = self.get_stack()
        if len(stack) == 0:
            raise ValueError("No current transactions found")
        return stack.pop()

    def peek_transaction(self):
        stack = self.get_stack()
        if len(stack) == 0:
            raise ValueError("No current transactions found")
        return stack[-1]

    def is_any_current_transaction_present(self):
        stack = self.get_stack()
        if len(stack) == 0:
            return False
        return True

    def push_transaction(
        self,
    ) -> str:
        stack = self.get_stack()
        with self._counter_lock:
            self._thread_counter += 1
            txn_id = self._generate_transaction_id(self._thread_counter)
            stack.append(Transaction(id=txn_id, changes={}))
            return txn_id

    def _generate_transaction_id(self, transaction_idx):
        thread_id = threading.current_thread().ident
        return f"txn_{thread_id}_{transaction_idx}"
