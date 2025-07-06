import unittest
import threading
import time
from transactional_db.core import TransactionalDBBuilder


class TestKVStore(unittest.TestCase):
    def setUp(self):
        """Set up a new KVStore for each test."""
        self.kv = TransactionalDBBuilder().build()

    def test_basic_set_get_delete(self):
        """Tests core operations outside of a transaction."""
        print("\nRunning test_basic_set_get_delete...")
        self.kv.set("a", 10)
        self.assertEqual(self.kv.get("a"), 10)
        self.kv.set("a", 20)
        self.assertEqual(self.kv.get("a"), 20)
        self.kv.delete("a")
        with self.assertRaises(KeyError):
            self.kv.get("a")
        print("PASSED")

    def test_simple_transaction_commit(self):
        """Tests a single transaction that gets committed."""
        print("\nRunning test_simple_transaction_commit...")
        self.kv.set("a", "initial")
        self.kv.begin()
        self.kv.set("a", "transaction_value")
        # Value should be updated inside the transaction
        self.assertEqual(self.kv.get("a"), "transaction_value")
        self.kv.commit()
        # Value should be persisted after commit
        self.assertEqual(self.kv.get("a"), "transaction_value")
        print("PASSED")

    def test_simple_transaction_rollback(self):
        """Tests a single transaction that gets rolled back."""
        print("\nRunning test_simple_transaction_rollback...")
        self.kv.set("a", "initial")
        self.kv.begin()
        self.kv.set("a", "new_value")
        self.assertEqual(self.kv.get("a"), "new_value")
        self.kv.rollback()
        # Value should revert to its state before the transaction
        self.assertEqual(self.kv.get("a"), "initial")
        print("PASSED")

    def test_nested_transactions(self):
        """Tests nested transactions with both commit and rollback."""
        print("\nRunning test_nested_transactions...")
        self.kv.set("a", 1)
        self.kv.set("b", 1)

        self.kv.begin()  # Outer transaction
        self.kv.set("a", 2)
        self.assertEqual(self.kv.get("a"), 2)

        self.kv.begin()  # Inner transaction
        self.kv.set("a", 3)
        self.kv.set("b", 2)
        self.assertEqual(self.kv.get("a"), 3)
        self.assertEqual(self.kv.get("b"), 2)
        self.kv.rollback()  # Rollback inner transaction

        # Should see changes from outer transaction
        self.assertEqual(self.kv.get("a"), 2)
        self.assertEqual(self.kv.get("b"), 1)  # 'b' change was rolled back

        self.kv.commit()  # Commit outer transaction

        # Final values should be from the committed outer transaction
        self.assertEqual(self.kv.get("a"), 2)
        self.assertEqual(self.kv.get("b"), 1)
        print("PASSED")

    def test_error_on_no_transaction(self):
        """Tests that commit/rollback raise errors when no transaction is active."""
        print("\nRunning test_error_on_no_transaction...")
        with self.assertRaises(ValueError):
            self.kv.commit()
        with self.assertRaises(ValueError):
            self.kv.rollback()
        print("PASSED")

    def test_multithreaded_basic_operations(self):
        """Test basic operations across multiple threads"""
        results = {}

        def worker(thread_id):
            self.kv.set(f"key_{thread_id}", f"value_{thread_id}")
            results[thread_id] = self.kv.get(f"key_{thread_id}")

        threads = []
        for i in range(5):
            thread = threading.Thread(target=worker, args=(i,))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        for i in range(5):
            self.assertEqual(results[i], f"value_{i}")

    def test_multithreaded_transactions(self):
        """Test transactions across multiple threads"""
        results = {}

        def worker(thread_id):
            self.kv.begin()
            self.kv.set(f"key_{thread_id}", f"value_{thread_id}")
            time.sleep(0.1)  # Simulate some work
            self.kv.commit()
            results[thread_id] = self.kv.get(f"key_{thread_id}")

        threads = []
        for i in range(3):
            thread = threading.Thread(target=worker, args=(i,))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        for i in range(3):
            self.assertEqual(results[i], f"value_{i}")

    def test_concurrent_transaction_isolation(self):
        """
        Tests that two threads with independent transactions do not interfere.
        """
        print("\nRunning test_concurrent_transaction_isolation...")
        self.kv.set("shared_key", "initial")
        results = {}
        barrier = threading.Barrier(2)  # To synchronize thread start

        def worker1():
            self.kv.begin()
            val = self.kv.get("shared_key")
            time.sleep(2)
            self.kv.set("shared_key", f"{val}_w1")
            self.kv.commit()
            results["w1_final"] = self.kv.get("shared_key")
            barrier.wait()

        def worker2():
            time.sleep(1)  # Ensure worker1 starts and acquires lock
            self.kv.begin()

            if self.kv.get("shared_key") == "initial":
                self.kv.set("shared_key", "initial_w2")
            time.sleep(5)
            self.kv.commit()
            results["w2_read"] = self.kv.get("shared_key")
            barrier.wait()

        thread1 = threading.Thread(target=worker1)
        thread2 = threading.Thread(target=worker2)

        thread1.start()
        thread2.start()

        thread1.join()
        thread2.join()

        # worker1 should see "initial", update it, and commit.
        self.assertEqual(results["w1_final"], "initial_w1")
        # worker2 should be blocked until worker1 finishes, so it reads the committed value.
        self.assertEqual(results["w2_read"], "initial_w1")
        # The final state of the store should be worker1's value.
        self.assertEqual(self.kv.get("shared_key"), "initial_w1")
        print("PASSED")


if __name__ == "__main__":
    unittest.main(verbosity=2)
