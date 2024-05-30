
# Mercado Pago ETL (Propuestas de Valor)
Este desafío tiene como objetivo desarrollar un proceso integral de manejo de datos a través de un flujo ETL (Extracción, Transformación y Carga). La meta es proporcionar un dataset normalizado que funcione como entrada para modelos de análisis. 

## Estructura del proyecto

```bash
MercadoPago_ETL
├─ data
│  ├─ processed
│  │  └─ result_dataset.csv
│  └─ raw
│     ├─ pays.csv
│     ├─ prints.json
│     └─ taps.json
├─ logs
│  ├─ etl.log
│  └─ expectations
│     ├─ expected_pays.json
│     ├─ expected_prints.json
│     └─ expected_taps.json
├─ src
│  ├─ config.py
│  ├─ data_validator.py
│  ├─ etl.py
│  ├─ extractor.py
│  ├─ loader.py
│  ├─ main.py
│  ├─ transformer.py
│  ├─ __init__.py
│  └─ __pycache__
├─ notebooks
│  └─ data_explorer.ipynb
├─ README.md
├─ requirements.txt
├─ Dockerfile
├─ logging.conf

```

## Instalación de dependencias

```bash
python -m venv .venv
.\venv\Scripts\activate
pip install -r requirements.txt
```

## Uso

Al ejecutar el archivo main.py comienza el proceso ETL para concluir con el guardado de datos

```bash
python src/main.py
```
