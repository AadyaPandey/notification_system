import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from dotenv import load_dotenv

load_dotenv()

EMAIL = os.getenv("EMAIL_ADDRESS")
APP_PASSWORD = os.getenv("EMAIL_APP_PASSWORD")

receiver = "swapnilsinha1912@gmail.com"  # Change this to the email you want to test

msg = MIMEMultipart("alternative")
msg["Subject"] = "Notification System Test"
msg["From"] = EMAIL
msg["To"] = receiver

html = """
<h2>Hello!</h2>
<p>This email was sent using Gmail SMTP.</p>
<p>If you received it, everything is working.</p>
"""

msg.attach(MIMEText(html, "html"))

with smtplib.SMTP("smtp.gmail.com", 587) as smtp:
    smtp.ehlo()
    smtp.starttls()
    smtp.ehlo()

    smtp.login(EMAIL, APP_PASSWORD)

    smtp.sendmail(
        EMAIL,
        receiver,
        msg.as_string()
    )

print("✅ Email sent successfully!")