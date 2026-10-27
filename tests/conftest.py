import pytest
from pyspark.sql import SparkSession

import bronze
import silver

# Muestra pequena con el esquema de Bronce (columnas originales de PaySim).
# Cada fila cubre un caso concreto. Ver el comentario de la derecha.
# (step, type, amount, nameOrig, oldbalanceOrg, newbalanceOrig,
#  nameDest, oldbalanceDest, newbalanceDest, isFraud, isFlaggedFraud)
SAMPLE_ROWS = [
    # destino comercio: el origen cuadra y el destino no se evalua
    (1, "PAYMENT", 100.0, "C1", 1000.0, 900.0, "M1", 0.0, 0.0, 0, 0),
    # cuadra en origen y destino; step 24 es la ultima hora del dia 1
    (24, "CASH_OUT", 500.0, "C2", 500.0, 0.0, "C9", 100.0, 600.0, 0, 0),
    # fraude; el origen cuadra pero el destino no recibe nada
    (25, "TRANSFER", 2000.0, "C3", 2000.0, 0.0, "C8", 0.0, 0.0, 1, 0),
    # saldo de origen en cero: no cuadra -> zero_origin_balance
    (26, "CASH_OUT", 300.0, "C4", 0.0, 0.0, "C7", 0.0, 300.0, 0, 0),
    # cuenta vaciada con saldo menor al importe -> account_emptied
    (27, "CASH_OUT", 400.0, "C5", 1000.0, 0.0, "C6", 0.0, 400.0, 0, 0),
    # descuadre sin causa conocida -> other
    (28, "PAYMENT", 50.0, "C10", 1000.0, 800.0, "M2", 0.0, 0.0, 0, 0),
    # caso de coma flotante: la diferencia real es exactamente 0.01 (= TOLERANCE),
    # pero en double sale 0.01000000000022 y sin redondear daba "no cuadra"
    (28, "CASH_OUT", 4237.32, "C11", 13522.99, 9285.68, "C12", 0.0, 4237.32, 0, 0),
    # fraude marcado por el sistema; step 49 es la primera hora del dia 3
    (49, "TRANSFER", 1000.0, "C13", 1000.0, 0.0, "C14", 0.0, 1000.0, 1, 1),
]


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder
        .appName("PaySimTests")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture(scope="session")
def bronze_df(spark):
    return spark.createDataFrame(SAMPLE_ROWS, schema=bronze.SCHEMA)


@pytest.fixture(scope="session")
def silver_df(bronze_df):
    return silver.transform(bronze_df)
