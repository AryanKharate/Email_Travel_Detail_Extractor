import os
import asyncio
from email import policy
from email.parser import BytesParser
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv
from typing import Optional, List, Dict, Type
import pdfplumber
import pandas as pd
import json
import re
from datetime import datetime
from dateutil import parser as date_parser
import html2text

# Gemini Imports
from google import genai
from pydantic import BaseModel


# ==========================
# 1️⃣ Load Environment Variables
# ==========================
load_dotenv()

GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY", "").strip()
if not GOOGLE_API_KEY:
    raise ValueError("GOOGLE_API_KEY not found in .env file")


# ==========================
# 2️⃣ Define Models
# ==========================

class TravelData(BaseModel):
    passenger_name: Optional[str] = None
    airline: Optional[str] = None
    booking_partner: Optional[str] = None
    pnr: Optional[str] = None
    flight_number: Optional[str] = None
    from_location: Optional[str] = None
    to_location: Optional[str] = None
    travel_date: Optional[str] = None
    booking_date: Optional[str] = None
    expense: Optional[float] = None
    source_file: Optional[str] = None


class HotelData(BaseModel):
    guest_name: Optional[str] = None
    hotel_name: Optional[str] = None
    booking_partner: Optional[str] = None
    city: Optional[str] = None
    check_in: Optional[str] = None
    check_out: Optional[str] = None
    booking_date: Optional[str] = None
    total_amount: Optional[float] = None
    number_of_nights: Optional[int] = None
    source_file: Optional[str] = None


class CabData(BaseModel):
    guest_name: Optional[str] = None
    travel_date: Optional[str] = None
    time: Optional[str] = None
    from_location: Optional[str] = None
    to_location: Optional[str] = None
    expense: Optional[float] = None
    source_file: Optional[str] = None


class AttachmentExtraction(BaseModel):
    travels: List[TravelData] = []
    hotels: List[HotelData] = []
    cabs: List[CabData] = []


class TravelHotelExtraction(BaseModel):
    travels: List[TravelData] = []
    hotels: List[HotelData] = []


class CabExtraction(BaseModel):
    cabs: List[CabData] = []


def _extract_subject_from_email_text(email_text: str) -> str:
    match = re.search(r"^Subject:\s*(.+)$", email_text, flags=re.IGNORECASE | re.MULTILINE)
    return match.group(1).strip() if match else ""


def _is_cab_intent(email_text: str) -> bool:
    subject = _extract_subject_from_email_text(email_text).lower()
    subject_terms = ["cab", "taxi", "outstation", "sedan", "suv", "chauffeur", "car booking"]
    if any(term in subject for term in subject_terms):
        return True

    body_text = email_text.lower()
    has_cab_reference = bool(re.search(r"\b(cab|taxi|chauffeur)\b", body_text))
    has_booking_details = bool(re.search(r"\b(pickup|drop|vehicle|passenger|location|sedan|suv|outstation)\b", body_text))
    return has_cab_reference and has_booking_details


def _is_valid_cab_record(cab: Dict[str, Optional[str]]) -> bool:
    guest_name = str(cab.get("guest_name") or "").strip()
    from_location = str(cab.get("from_location") or "").strip()
    to_location = str(cab.get("to_location") or "").strip()
    travel_date = str(cab.get("travel_date") or "").strip()
    time = str(cab.get("time") or "").strip()

    has_guest = bool(guest_name)
    has_route = bool(from_location or to_location)
    has_when = bool(travel_date or time)
    return has_guest and has_route and has_when


# ==========================
# 3️⃣ Date Normalization
# ==========================

def normalize_date_value(value: Optional[str]) -> Optional[str]:
    """Normalize many date formats to dd-mm-yyyy."""
    if value is None:
        return None

    if isinstance(value, float) and pd.isna(value):
        return None

    if isinstance(value, (datetime, pd.Timestamp)):
        return value.strftime("%d-%m-%Y")

    text = str(value).strip()
    if not text or text.lower() in {"nan", "none", "nat"}:
        return None

    # Handle compact month forms like 12May2025 / 03MAR2025 / 1Aug25
    text = re.sub(r"(\d)([A-Za-z])", r"\1 \2", text)
    text = re.sub(r"([A-Za-z])(\d)", r"\1 \2", text)
    text = re.sub(r"\s+", " ", text).strip()

    try:
        parsed = date_parser.parse(text, dayfirst=True, fuzzy=True)
        return parsed.strftime("%d-%m-%Y")
    except (ValueError, TypeError, OverflowError):
        return text


def normalize_date_columns(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    for col in columns:
        if col in df.columns:
            df[col] = df[col].apply(normalize_date_value)
    return df


# ==========================
# 4️⃣ Extract Email Body + Attachments
# ==========================

def _safe_attachment_filename(filename: str) -> str:
    name = os.path.basename(filename).replace("\\", "_").replace("/", "_")
    name = re.sub(r'[:*?"<>|]', "_", name).strip(" .")
    return name or "attachment.pdf"


def extract_attachment_text(file_path: str) -> Dict[str, str]:
    with open(file_path, 'rb') as f:
        msg = BytesParser(policy=policy.default).parse(f)

    os.makedirs("attachments", exist_ok=True)

    subject = str(msg.get("subject", "") or "").strip()
    sent_date = str(msg.get("date", "") or "").strip()
    from_header = str(msg.get("from", "") or "").strip()

    body_text = ""
    attachment_text = ""

    for part in msg.walk():
        content_type = part.get_content_type()
        disposition = part.get_content_disposition()

        # ✅ Read plain text email body (cab emails live here)
        if content_type == "text/plain" and disposition != "attachment":
            try:
                body_text += part.get_content() or ""
            except Exception:
                payload = part.get_payload(decode=True)
                if payload:
                    body_text += payload.decode("utf-8", errors="ignore")

        # ✅ Read HTML body and convert to plain text
        elif content_type == "text/html" and disposition != "attachment":
            try:
                html_content = part.get_content() or ""
            except Exception:
                payload = part.get_payload(decode=True)
                html_content = payload.decode("utf-8", errors="ignore") if payload else ""
            if html_content:
                h = html2text.HTML2Text()
                h.ignore_links = True
                h.ignore_images = True
                body_text += h.handle(html_content)

        # ✅ Read PDF attachments
        elif disposition == "attachment":
            filename = part.get_filename()
            if filename and filename.lower().endswith(".pdf"):
                print(f"  📎 Found PDF attachment: {filename}")
                safe_filename = _safe_attachment_filename(filename)
                filepath = os.path.join("attachments", safe_filename)
                payload = part.get_payload(decode=True)
                if payload:
                    with open(filepath, "wb") as pdf_file:
                        pdf_file.write(payload)
                    print("  📄 Extracting text from PDF...")
                    try:
                        with pdfplumber.open(filepath) as pdf:
                            for page in pdf.pages:
                                attachment_text += page.extract_text() or ""
                    except Exception as e:
                        print(f"  ⚠ Could not read PDF {filename}: {e}")

    # Keep email text and PDF text separated so category extraction uses strict sources.
    email_text = ""
    if subject or sent_date or from_header:
        email_text += "[EMAIL META]\n"
        if subject:
            email_text += f"Subject: {subject}\n"
        if sent_date:
            email_text += f"Date: {sent_date}\n"
        if from_header:
            email_text += f"From: {from_header}\n"
        email_text += "\n"
    if body_text.strip():
        email_text += f"[EMAIL BODY]\n{body_text.strip()}\n"

    return {
        "email_text": email_text.strip(),
        "pdf_text": attachment_text.strip(),
    }


# ==========================
# 5️⃣ LLM Setup & Prompt
# ==========================

client = genai.Client(api_key=GOOGLE_API_KEY)

travel_hotel_prompt_template = """
You are a professional travel data extraction system.
Extract only FLIGHT and HOTEL booking details from the PDF text below.

Important:
- This input is PDF attachment text only.
- Do not infer from email subject/body.
- If PDF has no flight/hotel booking data, return empty arrays.

TRAVEL fields:
- passenger_name (string)
- airline (string)
- booking_partner (string)
- pnr (string)
- flight_number (string)
- from_location (string)
- to_location (string)
- travel_date (string)
- booking_date (string)
- expense (float)

HOTEL fields:
- guest_name (string)
- hotel_name (string)
- booking_partner (string)
- city (string)
- check_in (string)
- check_out (string)
- booking_date (string)
- total_amount (float)
- number_of_nights (integer)

Rules:
- Return valid JSON only with exactly keys: "travels", "hotels"
- Return [] for categories with no records
- Do not wrap output in markdown

PDF Text:
{pdf_text}
"""


cab_prompt_template = """
You are a professional travel data extraction system.
Extract only CAB booking details from the email metadata/body below.

Important:
- This input is email subject/body text only.
- Do not infer from attachments.
- If there are multiple vehicles/passengers, return one cab record per vehicle/passenger.
- Only extract a cab record when there is explicit cab evidence (cab/taxi/vehicle booking, pickup/drop details).
- Ignore generic travel planning, flight, and hotel messages that do not clearly book a cab.

CAB fields:
- guest_name (string): traveller/guest full name
- travel_date (string): date of cab travel
- time (string): pickup or reporting time
- from_location (string): pickup/from location
- to_location (string): drop/to location
- expense (float): fare or expense as a number

Rules:
- Return valid JSON only with exactly key: "cabs"
- Return [] when no cab booking exists
- Do not wrap output in markdown

Email Text:
{email_text}
"""


CLASSIFY_PROMPT = """Does this email contain a cab/taxi/vehicle booking?
Reply with exactly one word: YES or NO.

Email:
{email_text}"""


# ==========================
# 6️⃣ LLM Extraction Function
# ==========================

def _run_llm_json(formatted_prompt: str, response_schema: Type[BaseModel]) -> dict:
    response = client.models.generate_content(
        model='gemini-flash-lite-latest',
        contents=formatted_prompt,
        config={
            'response_mime_type': 'application/json',
            'response_schema': response_schema,
        }
    )

    raw_output = response.text.strip() if response.text else ""
    if not raw_output:
        raise ValueError("❌ LLM returned empty response")

    # Strip markdown fences if present (safety net)
    raw_output = re.sub(r"```json", "", raw_output).strip()
    raw_output = re.sub(r"```", "", raw_output).strip()

    try:
        return json.loads(raw_output)
    except json.JSONDecodeError as e:
        print(f"  ⚠ Raw LLM output:\n{raw_output}")
        raise ValueError(f"❌ JSON parsing failed: {e}")


def classify_email_type(email_text: str) -> bool:
    result = client.models.generate_content(
        model='gemini-flash-lite-latest',
        contents=CLASSIFY_PROMPT.format(email_text=email_text[:1500])
    )
    text = (result.text or "").strip().upper()
    return text == "YES"


def detect_cab_intent(email_text: str) -> bool:
    heuristic_intent = _is_cab_intent(email_text)
    if not email_text.strip():
        return False

    try:
        llm_intent = classify_email_type(email_text)
    except Exception as e:
        print(f"  ⚠ Cab classifier failed, falling back to heuristic: {e}")
        return heuristic_intent

    # Keep extraction conservative to reduce false positives in Cab sheet.
    return heuristic_intent and llm_intent


def run_travel_hotel_llm(pdf_text: str) -> dict:
    if not pdf_text:
        return {"travels": [], "hotels": []}
    travel_hotel_prompt = travel_hotel_prompt_template.format(pdf_text=pdf_text)
    return _run_llm_json(travel_hotel_prompt, TravelHotelExtraction)


def run_cab_llm(email_text: str) -> dict:
    if not email_text:
        return {"cabs": []}
    cab_prompt = cab_prompt_template.format(email_text=email_text)
    return _run_llm_json(cab_prompt, CabExtraction)


def _merge_extraction_data(
    travel_hotel_data: dict,
    cab_data: dict,
    email_text: str,
    source_file: str,
    cab_intent: bool,
) -> AttachmentExtraction:
    data = {
        "travels": travel_hotel_data.get("travels", []),
        "hotels": travel_hotel_data.get("hotels", []),
        "cabs": cab_data.get("cabs", []),
    }

    # Sanitize — remove any null entries inside lists
    data["travels"] = [t for t in data.get("travels", []) if t]
    data["hotels"] = [h for h in data.get("hotels", []) if h]
    data["cabs"] = [c for c in data.get("cabs", []) if c]

    subject_line = _extract_subject_from_email_text(email_text)
    route_match = re.search(
        r"\b([A-Za-z][A-Za-z .,&'-]{1,60}?)\s+to\s+([A-Za-z][A-Za-z .,&'-]{1,60}?)\b",
        subject_line,
        flags=re.IGNORECASE,
    )
    subject_from = route_match.group(1).strip() if route_match else None
    subject_to = route_match.group(2).strip() if route_match else None

    normalized_cabs = []
    for cab in data["cabs"]:
        normalized = {
            "guest_name": cab.get("guest_name") or cab.get("passenger_name") or cab.get("name"),
            "travel_date": cab.get("travel_date") or cab.get("ride_date") or cab.get("pickup_date") or cab.get("pickup"),
            "time": cab.get("time") or cab.get("travel_time") or cab.get("pickup_time") or cab.get("reporting_time"),
            "from_location": cab.get("from_location") or cab.get("pickup_location") or cab.get("location"),
            "to_location": cab.get("to_location") or cab.get("drop_location") or cab.get("destination"),
            "expense": cab.get("expense") if cab.get("expense") is not None else cab.get("total_amount"),
        }
        if cab_intent and not normalized["from_location"] and subject_from:
            normalized["from_location"] = subject_from
        if cab_intent and not normalized["to_location"] and subject_to:
            normalized["to_location"] = subject_to
        if _is_valid_cab_record(normalized):
            normalized_cabs.append(normalized)

    data["cabs"] = normalized_cabs

    # Inject source file into every record
    for t in data["travels"]:
        t["source_file"] = source_file
    for h in data["hotels"]:
        h["source_file"] = source_file
    for c in data["cabs"]:
        c["source_file"] = source_file

    return AttachmentExtraction.model_validate(data)


def extract_from_attachment(email_text: str, pdf_text: str, source_file: str) -> AttachmentExtraction:
    cab_intent = detect_cab_intent(email_text)
    travel_hotel_data = run_travel_hotel_llm(pdf_text)
    cab_data = run_cab_llm(email_text) if (email_text and cab_intent) else {"cabs": []}
    return _merge_extraction_data(travel_hotel_data, cab_data, email_text, source_file, cab_intent)


async def process_file_async(file_path: str, executor: ThreadPoolExecutor) -> Optional[AttachmentExtraction]:
    loop = asyncio.get_running_loop()
    filename = os.path.basename(file_path)
    print(f"\n📧 Processing: {filename}")

    extracted = await loop.run_in_executor(executor, extract_attachment_text, file_path)
    email_text = extracted.get("email_text", "")
    pdf_text = extracted.get("pdf_text", "")

    if not email_text and not pdf_text:
        print("  ⚠ No readable text content found. Skipping.")
        return None

    print(f"  📝 Extracted text chars → Email: {len(email_text)} | PDF: {len(pdf_text)}")

    cab_intent = detect_cab_intent(email_text)
    travel_future = None
    cab_future = None

    if pdf_text:
        travel_future = loop.run_in_executor(executor, run_travel_hotel_llm, pdf_text)
    if email_text and cab_intent:
        cab_future = loop.run_in_executor(executor, run_cab_llm, email_text)

    awaitables = [f for f in (travel_future, cab_future) if f is not None]
    outputs = await asyncio.gather(*awaitables) if awaitables else []

    travel_hotel_data = {"travels": [], "hotels": []}
    cab_data = {"cabs": []}

    output_idx = 0
    if travel_future is not None:
        travel_hotel_data = outputs[output_idx]
        output_idx += 1
    if cab_future is not None:
        cab_data = outputs[output_idx]

    result = _merge_extraction_data(travel_hotel_data, cab_data, email_text, file_path, cab_intent)

    print(
        f"  ✅ Found → Flights: {len(result.travels)} | Hotels: {len(result.hotels)} | Cabs: {len(result.cabs)}"
    )
    return result


# ==========================
# 7️⃣ Save to Excel
# ==========================

def save_to_excel(result: AttachmentExtraction, output_file: str = "travel_output.xlsx"):

    cab_columns = [
        "Guest Name",
        "Travel Date",
        "Time",
        "From Location",
        "To Location",
        "Expense",
        "Source Email",
    ]

    travel_rows = []
    for t in result.travels:
        row = {
            "Passenger Name":  t.passenger_name,
            "Airline":         t.airline,
            "Booking Partner": t.booking_partner or t.airline,
            "PNR":             t.pnr,
            "Flight Number":   t.flight_number,
            "From Location":   t.from_location,
            "To Location":     t.to_location,
            "Travel Date":     t.travel_date,
            "Booking Date":    t.booking_date,
            "Expense":         t.expense,
            "Source Email":    ""
        }
        if t.source_file:
            rel_path = os.path.relpath(t.source_file, os.path.dirname(os.path.abspath(output_file)))
            row["Source Email"] = f'=HYPERLINK("{rel_path}", "{os.path.basename(t.source_file)}")'
        travel_rows.append(row)

    hotel_rows = []
    for h in result.hotels:
        row = {
            "Guest Name":       h.guest_name,
            "Hotel Name":       h.hotel_name,
            "Booking Partner":  h.booking_partner or h.hotel_name,
            "City":             h.city,
            "Check In":         h.check_in,
            "Check Out":        h.check_out,
            "Booking Date":     h.booking_date,
            "Total Amount":     h.total_amount,
            "Number of Nights": h.number_of_nights,
            "Source Email":     ""
        }
        if h.source_file:
            rel_path = os.path.relpath(h.source_file, os.path.dirname(os.path.abspath(output_file)))
            row["Source Email"] = f'=HYPERLINK("{rel_path}", "{os.path.basename(h.source_file)}")'
        hotel_rows.append(row)

    cab_rows = []
    for c in result.cabs:
        row = {
            "Guest Name":    c.guest_name,
            "Travel Date":   c.travel_date,
            "Time":          c.time,
            "From Location": c.from_location,
            "To Location":   c.to_location,
            "Expense":       c.expense,
            "Source Email":    ""
        }
        if c.source_file:
            rel_path = os.path.relpath(c.source_file, os.path.dirname(os.path.abspath(output_file)))
            row["Source Email"] = f'=HYPERLINK("{rel_path}", "{os.path.basename(c.source_file)}")'
        cab_rows.append(row)

    new_travel_df = pd.DataFrame(travel_rows)
    new_hotel_df  = pd.DataFrame(hotel_rows)
    new_cab_df    = pd.DataFrame(cab_rows)
    for col in cab_columns:
        if col not in new_cab_df.columns:
            new_cab_df[col] = None
    new_cab_df = new_cab_df[cab_columns]

    # If output file already exists → append to existing sheets
    if os.path.exists(output_file):
        xls = pd.ExcelFile(output_file)
        existing_travel = pd.read_excel(output_file, sheet_name="Travel") if "Travel" in xls.sheet_names else pd.DataFrame()
        existing_hotel  = pd.read_excel(output_file, sheet_name="Hotel")  if "Hotel"  in xls.sheet_names else pd.DataFrame()
        existing_cab    = pd.read_excel(output_file, sheet_name="Cab")    if "Cab"    in xls.sheet_names else pd.DataFrame()

        # Handle older cab sheet schema so appends stay in one consistent format.
        existing_cab = existing_cab.rename(columns={
            "Passenger Name": "Guest Name",
            "Pickup Location": "From Location",
            "Drop Location": "To Location",
            "Ride Date": "Travel Date",
            "Total Amount": "Expense",
        })
        for col in cab_columns:
            if col not in existing_cab.columns:
                existing_cab[col] = None
        existing_cab = existing_cab[cab_columns]

        combined_travel = pd.concat([existing_travel, new_travel_df], ignore_index=True)
        combined_hotel  = pd.concat([existing_hotel,  new_hotel_df],  ignore_index=True)
        combined_cab    = pd.concat([existing_cab,    new_cab_df],    ignore_index=True)
    else:
        combined_travel = new_travel_df
        combined_hotel  = new_hotel_df
        combined_cab    = new_cab_df

    # Normalize all date columns
    combined_travel = normalize_date_columns(combined_travel, ["Travel Date", "Booking Date"])
    combined_hotel  = normalize_date_columns(combined_hotel,  ["Check In", "Check Out", "Booking Date"])
    combined_cab    = normalize_date_columns(combined_cab,    ["Travel Date"])

    with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
        combined_travel.to_excel(writer, sheet_name="Travel", index=False)
        combined_hotel.to_excel(writer,  sheet_name="Hotel",  index=False)
        combined_cab.to_excel(writer,    sheet_name="Cab",    index=False)

    print(f"\n✅ Data saved to {output_file}")


# ==========================
# 8️⃣ Main Runner
# ==========================

async def main_async():
    folder_path = "Filtered_100"
    max_workers = int(os.getenv("MAX_WORKERS", "8"))

    all_results = []

    print("📂 Scanning folder for .eml files...\n")

    eml_files = [
        os.path.join(folder_path, filename)
        for filename in os.listdir(folder_path)
        if filename.endswith(".eml")
    ]

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        tasks = [process_file_async(file_path, executor) for file_path in eml_files]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    for result in results:
        if isinstance(result, Exception):
            print(f"  ❌ Failed to extract file: {result}")
            continue
        if result:
            all_results.append(result)

    if not all_results:
        print("\n⚠ No valid travel/hotel/cab data found in any files.")
        return

    # Merge all results
    combined_travels = []
    combined_hotels  = []
    combined_cabs    = []

    for result in all_results:
        combined_travels.extend(result.travels)
        combined_hotels.extend(result.hotels)
        combined_cabs.extend(result.cabs)

    final_result = AttachmentExtraction(
        travels=combined_travels,
        hotels=combined_hotels,
        cabs=combined_cabs
    )

    print(f"\n📊 Total → Flights: {len(combined_travels)} | Hotels: {len(combined_hotels)} | Cabs: {len(combined_cabs)}")

    save_to_excel(final_result)

    print("\n🎉 All files processed successfully!")


def main():
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
