from fastapi import FastAPI
from faker import Faker
from pydantic import BaseModel
import pandas as pd
import random

app = FastAPI()
fake = Faker()

file_name = "backend/fakeapi/products.csv"
df = pd.read_csv(file_name)
df["indice"]  = range(1, len(df) + 1)
df.set_index("indice", inplace=True)

@app.get("/gerar_compra")
async def gerar_compra():
    index = random.randint(1, len(df) - 1)
    produto = df.iloc[index]

    return {
        "client": fake.name(),
        "creadit_card": fake.credit_card_number(card_type=None),
        "prduct_name": str(produto["Product Name"]),
        "ean": str(produto["EAN"]),
        "price": round(float(produto["Price"]) * 1.2, 2),
        "store": 11,
        "dateTime": fake.iso8601(),
        "clientPosition": list(fake.location_on_land()),  # transforma tupla em lista
    }

@app.get("/gerar_compra/{numero_registro}")
async def gerar_compra(numero_registro: int):
    if numero_registro < 1:
        return {"error": "Número de registro deve ser maior que 0"}
    
    respostas = []

    for _ in range(numero_registro):
        index = random.randint(1, len(df))
        produto = df.loc[index]

        compra = {
            "client": fake.name(),
            "creditcard": fake.credit_card_number(card_type=None),
            "product": str(produto["Product Name"]),
            "ean": str(produto["EAN"]),
            "price": round(float(produto["Price"]) * 1.2, 2),
            "store": 11,
            "dateTime": fake.iso8601(),
            "clientPosition": list(fake.location_on_land()),
        }

        respostas.append(compra)

    return respostas