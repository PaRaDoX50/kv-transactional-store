class KeyDeleteMarker:
    _instance = None

    def __new__(cls):
        if cls._instance:
            return cls._instance
        cls._instance = super().__new__(cls)
        return cls._instance
