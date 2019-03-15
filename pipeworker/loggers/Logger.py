class Logger:
    def __init__(self, level):
        self.level = level

    def log(self, message, message_level):
        if message_level <= self.level:
            print(message)
