# Databricks notebook source
import json
import os
from typing import List
os.system("pip install azure-servicebus==7.11.1")
from azure.servicebus import ServiceBusClient, ServiceBusMessage

# COMMAND ----------

def message_body_test(messages_list: List[str]) -> None:
  """
    Function: Test if all of the required elements are present in the message
    Args:
        messages_list (List[str]): messages body
    Returns:
        None
  """ 
  for msg in range(len(messages_list)):
    messages_dict = json.loads(messages_list[msg])
    if messages_dict["vendor"] == "CREDIT_KARMA":
      expected_keys = ["driverID", "fileLocation", "job_id"]
    elif messages_dict["vendor"] == "ZEN_DRIVE":
      expected_keys = ["fileLocation", "driverID", "publisherID", "job_id"]
    else:
      print("vendor not valid")
    for key in expected_keys:
      assert key in messages_dict, f"{key} not present"
  return None
  
def send_messages_to_service_bus(conn_str: str, messages_list: List[str]) -> None:
  """
    Function: sends the messages in a list of dictionaries to service bus
    Args:
      conn_str (string): connection string for connecting to the service bus
      messages_list (List[str]): body of the messages
    Returns:
      None
  """
  with ServiceBusClient.from_connection_string(conn_str) as client:
    with client.get_queue_sender("post_job_queue_ep") as sender:
        for job_msg in messages_list:
            msg = ServiceBusMessage(job_msg)
            sender.send_messages(msg)
  return None

# COMMAND ----------

# Place the messages list below
messages_list = [json.dumps({"driverID": "a204346f-feb2-482e-aacd-1024cde775a7.62",
        "fileLocation": "gs://cktest-ck-geico-trips/testfiles/FL.cd24fc14-7022-41be-a961-f3ce8ead48fc.a204346f-feb2-482e-aacd-1024cde775a7.62.json.gz.gpg","vendor":"CREDIT_KARMA","job_id":"CK.1.1"}),
                 json.dumps({
        "driverID": "c430b4ac-5885-431d-87c4-2fd9654117a5.421",
        "fileLocation": "gs://cktest-ck-geico-trips/testfiles/GA.2356ce05-b1b8-4d72-bf94-25f9fc82e35f.c430b4ac-5885-431d-87c4-2fd9654117a5.421.json.gz.gpg","vendor":"CREDIT_KARMA","job_id":"CK.2.1"
    }),
                 json.dumps( {
        "driverID": "7981a53b-e3c7-427f-8620-2dc5db5c88bd.848",
        "fileLocation": "gs://cktest-ck-geico-trips/testfiles/OK.2fe5f20f-71f4-4df6-8f3c-bcb201dc5017.7981a53b-e3c7-427f-8620-2dc5db5c88bd.848.json.gz.gpg","vendor":"CREDIT_KARMA","job_id":"CK.3.1"
    }),
                 json.dumps({
        "driverID": "RandomDriverID",
        "fileLocation": "gs://cktest-ck-geico-trips/testfiles/FL.cd24fc14-7022-41be-a961-f3ce8ead48fc.a204346f-feb2-482e-aacd-1024cde775a7.62.json.gz.gpg","vendor":"CREDIT_KARMA","job_id":"CK.5.1"
    }),
                 json.dumps({
        "driverID": "6c20e1f1-ef13-4d1d-a8e8-efe0bda97ddf.208",
        "fileLocation": "gs://cktest-ck-geico-trips/testfiles/OR.9f48bad0-51e3-4abb-ba30-0da509fe8202.6c20e1f1-ef13-4d1d-a8e8-efe0bda97ddf.208.json.gz.gpg","vendor":"CREDIT_KARMA","job_id":"CK.6.1"
    }),
                 json.dumps({
        "fileLocation": "s3://geico-zendrive/test/outbound/Coin/2023-08-28/TX.04301c20-c951-4bcd-acca-706fb8b490fc.dN3B4CMJQINYPcg89coXMRvHACH2.json.gz.pgp",
        "driverID": "dN3B4CMJQINYPcg89coXMRvHACH2","vendor":"ZEN_DRIVE","job_id":"ZD.7.1",
        "publisherID": "MoneyLion",
    }),
                 json.dumps({
        "fileLocation": "s3://geico-zendrive/test/outbound/Coin/2023-08-28/TX.04301c20-c951-4bcd-acca-706fb8b490fc.dN3B4CMJQINYPcg89coXMRvHACH2.json.gz.pgp",
        "driverID": "RandomDriverID2","vendor":"ZEN_DRIVE","job_id":"ZD.8.1",
        "publisherID": "ABC",
    }),
                 json.dumps({
        "fileLocation": "s3://prod.zendrive.com/backend/testdata/iql/e2e/dep/consumer/exchange/geico/outbound/encrypted/MoneyLion/2023-12-12/GA.ff8ae326-6ccf-48d1-99be-90fa2bf9c931.64469fa4f1edbc40a1f094ea.json.gz.pgp",
        "driverID": "64469fa4f1edbc40a1f094ea","vendor":"ZEN_DRIVE","job_id":"ZD.9.1",
        "publisherID": "MoneyLion",
    }),
                 json.dumps({
        "fileLocation": "s3://geico-zendrive/prod/outbound/MoneyLion/2023-12-12/OK.0cf8d583-37d6-420a-b4b9-1a63dd37e47d.60bc5c8d435a25533d977966.json.gz.pgp",
        "driverID": "60bc5c8d435a25533d977966","vendor":"ZEN_DRIVE","job_id":"ZD.10.1",
        "publisherID": "MoneyLion",
    })]

# COMMAND ----------

# Test if the required elements are present in each message
message_body_test(messages_list)

# COMMAND ----------

messages_list

# COMMAND ----------

### For sending messages to the PP service bus uncomment the next line
#CONNECTION_STRING = dbutils.secrets.get(scope = "EDPAML_PP_Scope", key = "POST-SERVICEBUS-CONNECTION-STRING")

### For sending messages to the PD service bus uncomment the next line
#CONNECTION_STRING = dbutils.secrets.get(scope = "AML_TRN_Scope", key = "POST-SERVICEBUS-CONNECTION-STRING")

### For sending messages to the SB service bus uncomment the next line
CONNECTION_STRING = dbutils.secrets.get(scope = "AML_SB1_Scope", key = "POST-SERVICEBUS-CONNECTION-STRING") 
send_messages_to_service_bus(CONNECTION_STRING, messages_list)
