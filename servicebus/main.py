import io
import json
from azure.servicebus import ServiceBusClient, ServiceBusMessage

CONNECTION_STR = "Endpoint=sb://ppsoboi.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=0xbO5stvtfKiGpx/qf0578e343HWNDlJXshjqOQYs2I="
QUEUE_NAME = "data-queue"

if __name__ == '__main__':
    with io.open('jobs.json', encoding='utf-8') as json_file:
        jobs_json = json.load(json_file)

    servicebus_client = ServiceBusClient.from_connection_string(conn_str=CONNECTION_STR, logging_enable=True)

    with servicebus_client:
        sender = servicebus_client.get_queue_sender(queue_name=QUEUE_NAME)

        with sender:
            for i, job_json in enumerate(jobs_json):
                print(f'sending message {i}...')
                message = ServiceBusMessage(json.dumps(job_json))
                sender.send_messages(message)
