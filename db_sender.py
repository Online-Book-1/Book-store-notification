from kafka import KafkaConsumer
import json
from config import settings

from database import SessionLocal  
from models import Notification





db_consumer=KafkaConsumer('notify.notifications',group_id="notify.sms.group",
                             bootstrap_servers=[settings.KAFKA_BOOTSTRAP_SERVERS],
                             value_deserializer=lambda x: json.loads(x.decode("utf-8")))



def start():
    print("db service Started... Listening for events to route via Email.")
    

    for msg in db_consumer:
        data = msg.value
        
        # Open a new DB session for this specific task
        db = SessionLocal()
        
        try:
            new_note = Notification(
                user_id=data['user_id'],
                message=data['message'],
                is_read=False
            )
            db.add(new_note)
            db.commit()
            print(f"Saved notification for User {data['user_id']}")
        except Exception as e:
            db.rollback()
            print(f"Error saving to DB: {e}")
        finally:
            db.close()