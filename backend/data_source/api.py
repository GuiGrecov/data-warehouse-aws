import requests
from contracts.schema import CompraSchema, GenericSchema
from io import BytesIO
import pandas as pd
import datetime
from typing import List, Dict, Union

class APICollector:
    def __init__(self, schema, aws):
        self._schema = schema
        self._aws = aws
        self._buffer = None

    def start(self, param):
        response = self.getData(param)
        response = self.extractData(response)
        response = self.transformData(response)
        self.convertToParquet(response)

        if self._buffer is not None:
            file_name = self.fileName()
            print(file_name)
            self._aws.upload_file(self._buffer.getvalue(), file_name)  # usa os bytes
            return True
        return False

    def getData(self, param):
        if param > 1:
            url = f"http://127.0.0.1:8000/gerar_compra/{param}"
        else:
            url = "http://127.0.0.1:8000/gerar_compra"

        res = requests.get(url)
        res.raise_for_status()  # Garante que erros HTTP sejam lançados
        return res.json()

    def extractData(self, response):
        result: List[Dict[str, Union[str, int, float]]] = []
        for item in response:
            index = {}
            for key, value in self._schema.items():
                val = item.get(key)
                index[key] = val if isinstance(val, value) else None
            result.append(index)
        return result

    def transformData(self, response):
        return pd.DataFrame(response)

    def convertToParquet(self, df):
        try:
            self._buffer = BytesIO()
            df.to_parquet(self._buffer, index=False)
            self._buffer.seek(0)
        except Exception as e:
            print(f"Erro ao converter para parquet: {e}")
            self._buffer = None

    def fileName(self):
        date_atual = datetime.datetime.now().strftime("%Y-%m-%dT%H-%M-%S")
        return f"api/api-response-compra-{date_atual}.parquet"
