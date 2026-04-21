
 
from sqlalchemy import Column,String,Integer,Text,Boolean
from sqlalchemy.orm import declarative_base


Base=declarative_base()


class Notification(Base):
    __tablename__="user_notification"
    id=Column(Integer,autoincrement=True,primary_key=True)
    user_id=Column(Integer,nullable=False),
    notification_data=Column(Text,nullable=False)
    is_read=Column(Boolean,default=False)

 