import unittest

if __name__ == "__main__":
    loader = unittest.TestLoader()
    suite = loader.discover("tests")
    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(suite)
    print(
        "‼👉THE FAILURE OF test_concurrent_transaction_isolation IS EXPECTED IN THIS IMPLEMENTATION. IT IS EXPLAINED IN THE README FILE.👈"
    )
