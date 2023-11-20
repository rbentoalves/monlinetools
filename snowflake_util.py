import snowflake.connector
from time import time
import pandas as pd
import logging
import os
import requests
import json

ACCOUNT = "lt76872.west-europe.azure"
WAREHOUSE = "ANALYTICS_PRD_WH" # is it correct?
DATABASE = "DATA_LAKE_PRD"
SCHEMA = "CURATED" #changed
ROLE = "POWER_BI_GLOBAL_PERFORMANCE_AND_MONITORING" #changed

# YOU DO NOT NEED TO PUT YOUR CREDS HERE, MAKE ENV VARIABLES INSTEAD, THIS IS JUST TO SHOW YOU
os.environ["SNOWFLAKE_AUTH_CLIENT_ID"] = "THIS IS YOUR CID"
os.environ["SNOWFLAKE_AUTH_CLIENT_SECRET"] = "THIS IS YOUR CL SECRET"


def get_token():
    # Snowflake options

    # oAuth
    auth_client_id = os.environ['SNOWFLAKE_AUTH_CLIENT_ID']
    auth_client_secret = os.environ['SNOWFLAKE_AUTH_CLIENT_SECRET']

    auth_grant_type = 'client_credentials'  # "password", client_credentials

    # SCOPE_URL = "api://724695f7-8468-432e-b4ed-bc302f5c75ee/session:role-any" # mine
    scope_url = 'https://lightsourcebp.com/.default'
    token_url = "https://login.microsoftonline.com/ed5f664a-8752-4c95-8205-40c87d185116/oauth2/v2.0/token"

    payload = "client_id={clientId}&" \
              "client_secret={clientSecret}&" \
              "grant_type={grantType}&" \
              "scope={scopeUrl}".format(clientId=auth_client_id, clientSecret=auth_client_secret,
                                        grantType=auth_grant_type,
                                        scopeUrl=scope_url)

    response = requests.post(token_url, data=payload)
    json_data = json.loads(response.text)
    return json_data['access_token']


def get_query_results(query, results_location=0):
    conn = snowflake.connector.connect(
        account=ACCOUNT,
        role=ROLE,
        authenticator="oauth",
        token=get_token(),
        warehouse=WAREHOUSE,
        database=DATABASE,
        schema=SCHEMA,
        client_session_keep_alive=False,
        max_connection_pool=1
    )

    cursor_list = conn.execute_string(query)
    df = pd.DataFrame(cursor_list[results_location].fetchall())
    df.columns = [i[0] for i in cursor_list[results_location].description]
    conn.close()
    return df
