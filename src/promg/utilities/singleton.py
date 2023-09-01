class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            try:
                cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
            except TypeError as error:
                print(f"You should first create a {cls} yourself")
                raise
        return cls._instances[cls]