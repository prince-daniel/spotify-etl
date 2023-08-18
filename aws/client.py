import boto3

class AWS:
    def __init__(self, service, region_name):
        self.client = boto3.client(service, region_name)

    def get_client(self):
        return self.client

    def close_client(self):
        self.client.close()

class DynamoDB(AWS):
    def __init__(self, region_name):
        service = self.__class__.__name__.lower()
        super().__init__(service, region_name)

class S3(AWS):
    def __init__(self, region_name):
        service = self.__class__.__name__.lower()
        super().__init__(service, region_name)
