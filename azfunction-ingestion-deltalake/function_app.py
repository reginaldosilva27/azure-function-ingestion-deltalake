import datetime
import pandas as pd
import logging
import azure.functions as func
from deltalake import DeltaTable, write_deltalake
import requests

app = func.FunctionApp()
## Binding timerTrigger
@app.function_name(name="IngestionApiDeltaLake")
@app.schedule(schedule="*/10 * * * * *", arg_name="IngestionApiDeltaLake", run_on_startup=True,
              use_monitor=False) 
## Função MAIN
def IngestionApiDeltaLake(IngestionApiDeltaLake: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()
    
    logging.info(f'Iniciando requisição - {utc_timestamp}')
    try:
        storage_options = {"azure_storage_account_name": "externalstorageunity", "access_key":"xxx"}

        ## Chamando API de Bitcoins
        url = "https://api.coinext.com.br:8443/AP/GetL2Snapshot"
        payload = "{\"OMSId\": 1, \"InstrumentId\": 1, \"Depth\": 1}"
        response = requests.request("POST", url, data=payload)
        bitcoins = list(response.json())
        logging.info(f'Retorno API Bitcoins:')
        logging.info(response.content)

        schema = {
        "id": "int32",
        "actionDateTime": "string",
        "lastTradePrice": "string",
        "price": "string",
        "quantity": "string",
        "type": "string",
        "currency": "string",
        "datelog": "timestamp[us]",
        }
        dados = [[bitcoins[0][0],bitcoins[0][2],bitcoins[0][4],bitcoins[0][6],bitcoins[0][8],"purchase","bitcoin", pd.to_datetime(utc_timestamp)]]

        df = pd.DataFrame(dados, columns=schema.keys())
        write_deltalake("abfss://raw@externalstorageunity.dfs.core.windows.net/prod/tbBitcoins", df, mode = 'append', storage_options=storage_options)
        logging.info('Bitcoins gravados na DeltaTable...')

        logging.info(f'Lendo tabela delta e convertendo para pyarrow_table...')
        dt = DeltaTable("abfss://raw@externalstorageunity.dfs.core.windows.net/prod/tbBitcoins", storage_options=storage_options)
        table = dt.to_pyarrow_table()
        
        logging.info(f'Files: {len(dt.files())}')
        logging.info(f'Rows: {table.num_rows}')
        logging.info(f'Último Delta Table History:')
        logging.info(dt.history()[0])

        if len(dt.files()) > 10:
            logging.info(f'Delta Table com mais de 10 arquivos, iniciando otimização e Vaccum')
            dt.optimize()
            dt.vacuum(dry_run=False,retention_hours=0, enforce_retention_duration=False)

        utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()
        logging.info(f'Finalizando requisição - {utc_timestamp}')
    except Exception as error:
        logging.error(error)