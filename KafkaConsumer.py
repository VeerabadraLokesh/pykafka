


class Consumer:

    def __init__(self) -> None:
        self.events = {}
        pass

    def createMessageStreams(self, topic, start=-1):
        while True:
            messages = {}
            for message in messages:
                bytes = message.payload
                yield bytes
            self.events[topic].wait()
            self.events[topic].clear()
        pass
