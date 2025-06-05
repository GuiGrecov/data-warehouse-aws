from data_source.api import APICollector
from contracts.schema import CompraSchema
from aws.client import S3Client

schema = CompraSchema
aws = S3Client()

minha_classe = APICollector(schema, aws).start(2)
print(minha_classe)
