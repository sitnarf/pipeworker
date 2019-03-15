class Pipeline:
    def __init__(self, pipeline):
        self.pipeline = pipeline

    def execute(self, data=None):
        return self.pipeline.invoke(data)
