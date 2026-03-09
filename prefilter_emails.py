import os
import email
import re
import shutil
from email import policy
from email.parser import BytesParser

INPUT_DIR = "emails1"
TRAVEL_DIR = "travel"
OTHER_DIR = "other"

os.makedirs(TRAVEL_DIR, exist_ok=True)
os.makedirs(OTHER_DIR, exist_ok=True)

travel_patterns = [
    r'\bflight\b',
    r'\bitinerary\b',
    r'\bboarding pass\b',
    r'\bpnr\b',
    r'\bhotel reservation\b',
    r'\bcheck[- ]?in\b',
    r'\bcheck[- ]?out\b',
    r'\btrip\b',
    r'\btravel\b',
    r'\bairlines\b',
    r'\bdeparture\b',
    r'\barrival\b',
    r'\btrain\b',
    r'\bticket\b',
    r'\btrain ticket\b',
    r'\bticket confirmation\b',   # ← fixed comma

    r'\bconfirmation\b',
    r'\bbooking\b',
    r'\bvisit\b',
    r'\bcab\b',
    r'\bhotel\b',
    r'\broom\b',

    r'\bbooking confirmation\b',
    r'\bhotel booking\b',
    r'\bcab ride\b'
]

pattern = re.compile("|".join(travel_patterns), re.IGNORECASE)

for file in os.listdir(INPUT_DIR):

    if not file.endswith(".eml"):
        continue

    path = os.path.join(INPUT_DIR, file)

    with open(path, 'rb') as f:
        msg = BytesParser(policy=policy.default).parse(f)

    subject = msg['subject'] or ""
    body = ""

    if msg.is_multipart():
        for part in msg.walk():
            if part.get_content_type() == "text/plain":
                body += part.get_content()
    else:
        body = msg.get_content()

    text = subject + " " + body

    if pattern.search(text):
        shutil.copy2(path, os.path.join(TRAVEL_DIR, file))
    else:
        shutil.copy2(path, os.path.join(OTHER_DIR, file))