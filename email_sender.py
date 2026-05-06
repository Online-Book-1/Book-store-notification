from kafka import KafkaConsumer
import json
from email.message import EmailMessage
from config import settings
import smtplib



SENDER_EMAIL = "hritikchoukikarwork24@gmail.com"
SENDER_PASSWORD = settings.email


def send_email(email_data):
    msg = EmailMessage()
    
    # Extract data from the Kafka message
    recipient = email_data.get("to")
    subject = email_data.get("subject", "Verification Code")
    raw_body = email_data.get("body", "")
    otp_code = email_data.get("otp")  # Ensure your producer sends this!

    # Replace the {otp} placeholder with the actual number
    final_body = raw_body.replace("{otp}", str(otp_code))

    msg['Subject'] = subject
    msg['From'] = SENDER_EMAIL 
    msg['To'] = recipient
    msg.set_content(final_body, subtype='html')

    try:
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
            smtp.login(SENDER_EMAIL, SENDER_PASSWORD)
            smtp.send_message(msg)
            print(f"✅ Email sent successfully to {recipient}", flush=True)
    except smtplib.SMTPAuthenticationError:
        print("❌ AUTH ERROR: Check your Gmail App Password.")
        raise


def start():
    print("Email_sender Started... Listening for events to route via email.")

    email_consumer = KafkaConsumer(
    'notify.email',
    group_id="notify.email.group",
    bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
    value_deserializer=lambda x: json.loads(x.decode("utf-8"),api_version=(0, 10, 2), request_timeout_ms=60000)
)
    
    for msg in email_consumer:
        message=msg.value
        

        try:
            send_email(message)
             
        except Exception as e:
            print(f"❌ Failed to send email: {e}")

        
 