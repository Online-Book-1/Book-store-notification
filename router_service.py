# router_service.py
from kafka import KafkaConsumer, KafkaProducer
import json
from config import settings


# --- EMAIL TEMPLATES ---
TEMPLATE_MAP = {
    "USER_CREATED": {
        "EMAIL": {
            "subject": "Welcome to Folio!",
            "body": """
                <h2>Welcome, {username}! 🎉</h2>
                <p>We are thrilled to have you on board. Start reading and writing today!</p>
                <p>Best,<br>The Folio Team</p>
            """
        }
    },

    # ← renamed from CHECK_OTP to SEND_OTP to match our book system
    "SEND_OTP": {
        "EMAIL": {
            "subject": "Your Folio Verification Code",
            "body": """
                <h2>Hi {username}! 👋</h2>
                <p>Your one-time verification code is:</p>
                <p><strong style="font-size:32px; color:#c9a84c;">{otp}</strong></p>
                <p>This code expires in <strong>5 minutes</strong>.</p>
                <p style="color:red;">Do not share this code with anyone.</p>
                <p>Best,<br>The Folio Team</p>
            """
        }
    },

    "FORGOT_PASSWORD": {
        "EMAIL": {
            "subject": "Folio Password Reset",
            "body": """
                <h2>Password Reset Request</h2>
                <p>Hi {username},</p>
                <p>Your password reset OTP is:</p>
                <p><strong style="font-size:32px; color:#c9a84c;">{otp}</strong></p>
                <p>This code expires in <strong>5 minutes</strong>.</p>
                <p>If you did not request this, ignore this email.</p>
            """
        }
    },

    "NEW_PASSWORD": {
        "EMAIL": {
            "subject": "Your New Folio Password",
            "body": """
                <h2>Hello {username},</h2>
                <p>Your password has been reset. Here is your new temporary password:</p>
                <p><strong style="font-size:24px; color:#4CAF50;">{new_password}</strong></p>
                <p>Please change it after logging in.</p>
                <p>If you did not request this, contact support immediately.</p>
            """
        }
    },
}


def start():
    print("Router Service Started... Listening for events.", flush=True)

    
    producer = KafkaProducer(
    bootstrap_servers=[settings.KAFKA_BOOTSTRAP_SERVERS],
    value_serializer=lambda x: json.dumps(x).encode('utf-8'),api_version=(0, 10, 2), request_timeout_ms=60000
)

    consumer = KafkaConsumer(
    'user_events',
    group_id='notify_router_group',
    bootstrap_servers=[settings.KAFKA_BOOTSTRAP_SERVERS],
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    api_version=(0, 10, 2),
              # Don't hang for more than 2 seconds
    request_timeout_ms=30000,     # Time to wait for a response
    
)
    for msg in consumer:
        event      = msg.value
        event_type = event.get('event_type')
        raw_data   = event.get('data')

        print(f"\n📨 Received event: {event_type}", flush=True)

        # ── guard clauses ──
        if not raw_data:
            print("❌ Error: Event missing 'data' block.", flush=True)
            continue

        if event_type not in TEMPLATE_MAP:
            print(f"⚠️ No template found for event: {event_type}", flush=True)
            continue

        channel_templates = TEMPLATE_MAP[event_type]

        if "EMAIL" in channel_templates:
            email_tmpl = channel_templates["EMAIL"]
            try:
                email_payload = {
                    "to":      raw_data.get('email'),
                    "subject": email_tmpl["subject"].format(**raw_data),
                    "body":    email_tmpl["body"].format(**raw_data),  # {otp} replaced here ✅
                }
                producer.send('notify.email', value=email_payload)
                print(f"✅ Routed {event_type} → Email → {raw_data.get('email')}", flush=True)

            except KeyError as e:
                print(f"❌ Missing data field for template {event_type}: {e}", flush=True)

    # ← flush moved OUTSIDE the loop ✅
    producer.flush()