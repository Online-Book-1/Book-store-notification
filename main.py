 
from fastapi import FastAPI, Depends
from sqlalchemy.orm import Session
from database import get_db,engine
from models import Base
from models import Notification
from contextlib import asynccontextmanager
import db_sender  
import router_service 
import email_sender
import sms_sender  
import threading
from pydantic import BaseModel
from typing import List

class ResponseNotification(BaseModel):
    id:int
    user_id:int
    notification_data:str
    is_read:bool


@asynccontextmanager
async def lifespan(app: FastAPI):
    threading.Thread(target=db_sender.start, daemon=True).start()
    threading.Thread(target=router_service.start, daemon=True).start()
    threading.Thread(target=sms_sender.start, daemon=True).start()
    threading.Thread(target=email_sender.start, daemon=True).start()
    yield

app = FastAPI(lifespan=lifespan)

Base.metadata.create_all(bind=engine)
 


@app.get("/")
def get_root(db: Session = Depends(get_db)):
    return {"Status":"Connected"}
   

 

# main.py
@app.get("/notification/get_all_notifications",response_model=List[ResponseNotification])
def get_notifications(db: Session = Depends(get_db)):
    noti=db.query(Notification).all()
    return noti

# # 1. READ: Fetch notifications (Worker created these!)
@app.get("/notifications/{user_id}",response_model=List[ResponseNotification])
def get_notifications(user_id: int, db: Session = Depends(get_db)):
    notes = db.query(Notification).filter(Notification.user_id == user_id).all()
    return notes

# 2. UPDATE: User read the message
@app.put("/notifications/mark-read/{note_id}")
def mark_as_read(note_id: int, db: Session = Depends(get_db)):
    note = db.query(Notification).filter(Notification.id == note_id).first()
    if note:
        note.is_read = True
        db.commit()
        return {"status": "Updated"}
    return {"status": "Not Found"}