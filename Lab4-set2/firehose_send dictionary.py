import sys
import json
import time
import logging
import pandas as pd
import csv
import boto3
import backoff


# Code to connect to Firehose and batch a payload (list of dicts) to firehose. This will then get streamed to an s3 bucket
# TODO: add keys back into firehose class, modify to take payload a

class config:
    def __init__(self) -> None:
        self.region = 'us-east-2'
        self.fh_arn = 'arn:aws:firehose:us-east-2:796973478303:deliverystream/aws2-fh-data'
        self.delivery_stream_name = 'aws2-fh-data'


class FirehoseClient:
    """
    AWS Firehose client to send records and monitor metrics.

    Attributes:
        config (object): Configuration object with delivery stream name and region.
        delivery_stream_name (str): Name of the Firehose delivery stream.
        region (str): AWS region for Firehose and CloudWatch clients.
        firehose (boto3.client): Boto3 Firehose client. 
        cloudwatch (boto3.client): Boto3 CloudWatch client. - removed
    """

    def __init__(self, config):
        """
        Initialize the FirehoseClient.

        Args:
            config (object): Configuration object with delivery stream name and region.
        """
        self.config = config
        self.delivery_stream_name = config.delivery_stream_name
        self.region = config.region
        self.firehose = boto3.client("firehose", region_name=self.region,
            aws_access_key_id = '', 
            aws_secret_access_key = '')
        # self.cloudwatch = boto3.client("cloudwatch", region_name=self.region)


    # def put_record(self, record: dict):
    #     """
    #     Put individual records to Firehose with backoff and retry.

    #     Args:
    #         record (dict): The data record to be sent to Firehose.

    #     This method attempts to send an individual record to the Firehose delivery stream.
    #     It retries with exponential backoff in case of exceptions.
    #     """
    #     try:
    #         entry = self._create_record_entry(record)
    #         response = self.firehose.put_record(
    #             DeliveryStreamName=self.delivery_stream_name, Record=entry
    #         )
    #         self._log_response(response, entry)
    #     except Exception:
    #         # logger.info(f"Fail record: {record}.")
    #         raise


    @backoff.on_exception(
    backoff.expo, Exception, max_tries=5, jitter=backoff.full_jitter
    )
    
    
    def put_record_batch(self, data: list, batch_size: int = 500):
        """
        Put records in batches to Firehose with backoff and retry.

        Args:
            data (list): List of data records to be sent to Firehose.
            batch_size (int): Number of records to send in each batch. Default is 500.

        This method attempts to send records in batches to the Firehose delivery stream.
        It retries with exponential backoff in case of exceptions.
        """
        for i in range(0, len(data), batch_size):
            batch = data[i : i + batch_size]
            record_dicts = [{"Data": json.dumps(record)} for record in batch]
            try:
                response = self.firehose.put_record_batch(
                    DeliveryStreamName=self.delivery_stream_name, Records=record_dicts
                )
                # print(response)
                # self._log_batch_response(response, len(batch))
            except Exception as e:
                # logger.info(f"Failed to send batch of {len(batch)} records. Error: {e}")
                print(e)



if __name__ == '__main__':
    # create list of payloads to send to firehose
    data= []
    con = config()
    fh = FirehoseClient(con)
    # test sending data from json file
    for i in range(5):
        # with open (f'vehicle_data/vehicle{i}.csv','r')as fh:
        #     json = csv.DictReader(fh)
        
        # print (json)
        # if i ==0:
        #     df = pd.read_csv(f'vehicle_data/vehicle{i}.csv')
        # else:

        #     df_temp = pd.read_csv(f'vehicle_data/vehicle{i}.csv')
        #     df = pd.concat([df,df_temp])
        # df = pd.read_csv(f'vehicle_data/vehicle{i}.csv')
        # data = df.to_json(orient='records')
        # print(data)

        with open(f'vehicle_data/vehicle{i}.csv', encoding='utf-8') as csvf:
            jsonArray = []
            csvReader = csv.DictReader(csvf)

            for row in csvReader:
                jsonArray.append(row)

            print(jsonArray)
             # create connections and send data
            fh.put_record_batch(jsonArray)
    # print (json.dumps(df.to_dict(orient='list')))

    # data = df.to_json(orient='records')
    # print(data)
    # sample json payload
    # json_rec = json.loads('''{ 
    #     "TICKER_SYMBOL": "QXZ",
    #     "SECTOR": "HEALTHCARE",
    #     "CHANGE": -0.05,
    #     "PRICE": 84.51
    # }''')

    # create connections and send data

    # print(df.shape)
    # with open ('test.txt','w') as fle:
    #     fle.write(data)