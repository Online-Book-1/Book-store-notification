from kafka import KafkaConsumer
import json
from config import settings




email_consumer=KafkaConsumer('notify.sms',group_id="notify.sms.group",
                             bootstrap_servers=[settings.KAFKA_BOOTSTRAP_SERVERS],
                             value_deserializer=lambda x: json.loads(x.decode("utf-8")))


def send_sms(to,body):
    print(f"SMS:sent")                      


def start():
    print("SMS Started... Listening for events to route via sms.")
    
    for msg in email_consumer:
        message=msg.value
        

        send_sms(to=message["to"],body=message["body"])

        print(f"Sms: Sent to {message['to']}",flush=True)



