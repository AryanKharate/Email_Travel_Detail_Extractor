import os
from email import policy
from email.parser import BytesParser
from dotenv import load_dotenv
from typing import Optional, List
import pdfplumber
import pandas as pd
import json
import re

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
    passenger_name: Optional[str]
    airline: Optional[str]
    pnr: Optional[str]
    flight_number: Optional[str]
    from_location: Optional[str]
    to_location: Optional[str]
    travel_date: Optional[str]
    booking_date: Optional[str]
    expense: Optional[float]
    source_file: Optional[str] = None  # New field for hyperlink


class HotelData(BaseModel):
    guest_name: Optional[str]
    hotel_name: Optional[str]
    city: Optional[str]
    check_in: Optional[str]
    check_out: Optional[str]
    booking_date: Optional[str]
    total_amount: Optional[float]
    number_of_nights: Optional[int]
    source_file: Optional[str] = None  # New field for hyperlink


class CabData(BaseModel):
    passenger_name: Optional[str]
    operator_name: Optional[str]
    pickup_location: Optional[str]
    drop_location: Optional[str]
    ride_date: Optional[str]
    booking_date: Optional[str]
    total_amount: Optional[float]
    source_file: Optional[str] = None


class AttachmentExtraction(BaseModel):
    travels: List[TravelData]
    hotels: List[HotelData]
    cabs: List[CabData]


# ==========================
# 3️⃣ Extract ONLY Attachment Text
# ==========================

def extract_attachment_text(file_path: str) -> str:
    with open(file_path, 'rb') as f:
        msg = BytesParser(policy=policy.default).parse(f)

    os.makedirs("attachments", exist_ok=True)

    attachment_text = ""

    for part in msg.walk():
        if part.get_content_disposition() == "attachment":
            filename = part.get_filename()

            if filename:
                print(f"📎 Found attachment: {filename}")
                # This extractor reads only PDF attachments.
                if not filename.lower().endswith(".pdf"):
                    continue

                safe_filename = _safe_attachment_filename(filename)
                filepath = os.path.join("attachments", safe_filename)
                payload = part.get_payload(decode=True)

                if not payload:
                    continue

                with open(filepath, "wb") as f:
                    f.write(payload)

                print("📄 Extracting text from PDF...")
                with pdfplumber.open(filepath) as pdf:
                    for page in pdf.pages:
                        attachment_text += page.extract_text() or ""
    return attachment_text.strip()

def _safe_attachment_filename(filename: str) -> str:
    name = os.path.basename(filename).replace("\\", "_").replace("/", "_")
    name = re.sub(r'[:*?"<>|]', "_", name).strip(" .")
    return name or "attachment.pdf"


# ==========================
# 4️⃣ LLM Setup
# ==========================

client = genai.Client(api_key=GOOGLE_API_KEY)

prompt_template = """
You are a professional travel, hotel, and cab booking extraction system.
Extract all travel, hotel, and cab booking information from the document below.

TRAVEL fields:
- passenger_name (string)
- airline (string)
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
- city (string)
- check_in (string)
- check_out (string)
- booking_date (string)
- total_amount (float)
- number_of_nights (integer)

CAB fields:
- passenger_name (string)
- operator_name (string)
- pickup_location (string)
- drop_location (string)
- ride_date (string)
- booking_date (string)
- total_amount (float)

Rules:
- Respond with a valid JSON object. It must have exactly three keys: "travels", "hotels", and "cabs", all containing lists of the respective objects.
- If no travel bookings exist, return: "travels": []
- If no hotel bookings exist, return: "hotels": []
- If no cab bookings exist, return: "cabs": []
- NEVER return null inside lists
- Do NOT guess
- Return valid JSON only

Document Content:
{attachment_text}
"""


# ==========================
# 5️⃣ Extraction Function
# ==========================

def extract_from_attachment(attachment_text: str, source_file: str) -> AttachmentExtraction:

    formatted_prompt = prompt_template.format(
        attachment_text=attachment_text
    )

    response = client.models.generate_content(
        model='gemini-flash-lite-latest',
        contents=formatted_prompt,
        config={
            'response_mime_type': 'application/json',
            'response_schema': AttachmentExtraction,
        }
    )

    raw_output = response.text.strip() if response.text else ""

    if not raw_output:
        raise ValueError("❌ LLM returned empty response")

    # Assuming the response matches the schema, we inject the source_file
    data = json.loads(raw_output)
    
    for t in data.get("travels", []):
        t["source_file"] = source_file
    for h in data.get("hotels", []):
        h["source_file"] = source_file
    for c in data.get("cabs", []):
        c["source_file"] = source_file

    return AttachmentExtraction.model_validate(data)

    # --- Remove Markdown Fences ---
    raw_output = re.sub(r"```json", "", raw_output)
    raw_output = re.sub(r"```", "", raw_output)

    # --- Extract JSON Block ---
    json_match = re.search(r"\{.*\}", raw_output, re.DOTALL)
    if not json_match:
        raise ValueError("❌ No valid JSON object found in LLM response")

    json_string = json_match.group()

    try:
        parsed_json = json.loads(json_string)
    except json.JSONDecodeError as e:
        print("⚠ Raw LLM Output:")
        print(raw_output)
        raise ValueError("❌ JSON parsing failed")

    # --- Sanitize ---
    if parsed_json.get("travels") in [None, [None]]:
        parsed_json["travels"] = []

    if parsed_json.get("hotels") in [None, [None]]:
        parsed_json["hotels"] = []

    if parsed_json.get("cabs") in [None, [None]]:
        parsed_json["cabs"] = []

    return AttachmentExtraction.model_validate(parsed_json)

# ==========================
# 6️⃣ Save to Excel
# ==========================

def save_to_excel(result: AttachmentExtraction, output_file="travel_output.xlsx"):

    # Convert extracted data to rows
    travel_rows = []
    for t in result.travels:
        row = {
            "Passenger Name": t.passenger_name,
            "Airline": t.airline,
            "PNR": t.pnr,
            "Flight Number": t.flight_number,
            "From Location": t.from_location,
            "To Location": t.to_location,
            "Travel Date": t.travel_date,
            "Booking Date": t.booking_date,
            "Expense": t.expense,
        }
        if t.source_file:
            # Create Excel Hyperlink formula
            # Use relative path for better compatibility with Excel on Mac
            rel_path = os.path.relpath(t.source_file, os.path.dirname(os.path.abspath(output_file)))
            row["Source Email"] = f'=HYPERLINK("{rel_path}", "{os.path.basename(t.source_file)}")'
        else:
            row["Source Email"] = ""
        travel_rows.append(row)

    hotel_rows = []
    for h in result.hotels:
        row = {
            "Guest Name": h.guest_name,
            "Hotel Name": h.hotel_name,
            "City": h.city,
            "Check In": h.check_in,
            "Check Out": h.check_out,
            "Booking Date": h.booking_date,
            "Total Amount": h.total_amount,
            "Number of Nights": h.number_of_nights,
        }
        if h.source_file:
            rel_path = os.path.relpath(h.source_file, os.path.dirname(os.path.abspath(output_file)))
            row["Source Email"] = f'=HYPERLINK("{rel_path}", "{os.path.basename(h.source_file)}")'
        else:
            row["Source Email"] = ""
        hotel_rows.append(row)

    cab_rows = []
    for c in result.cabs:
        row = {
            "Passenger Name": c.passenger_name,
            "Operator Name": c.operator_name,
            "Pickup Location": c.pickup_location,
            "Drop Location": c.drop_location,
            "Ride Date": c.ride_date,
            "Booking Date": c.booking_date,
            "Total Amount": c.total_amount,
        }
        if c.source_file:
            rel_path = os.path.relpath(c.source_file, os.path.dirname(os.path.abspath(output_file)))
            row["Source Email"] = f'=HYPERLINK("{rel_path}", "{os.path.basename(c.source_file)}")'
        else:
            row["Source Email"] = ""
        cab_rows.append(row)

    new_travel_df = pd.DataFrame(travel_rows)
    new_hotel_df = pd.DataFrame(hotel_rows)
    new_cab_df = pd.DataFrame(cab_rows)

    # If file exists → append
    if os.path.exists(output_file):
        xls = pd.ExcelFile(output_file)
        existing_travel = pd.read_excel(output_file, sheet_name="Travel") if "Travel" in xls.sheet_names else pd.DataFrame()
        existing_hotel = pd.read_excel(output_file, sheet_name="Hotel") if "Hotel" in xls.sheet_names else pd.DataFrame()
        existing_cab = pd.read_excel(output_file, sheet_name="Cab") if "Cab" in xls.sheet_names else pd.DataFrame()

        combined_travel = pd.concat([existing_travel, new_travel_df], ignore_index=True)
        combined_hotel = pd.concat([existing_hotel, new_hotel_df], ignore_index=True)
        combined_cab = pd.concat([existing_cab, new_cab_df], ignore_index=True)
    else:
        combined_travel = new_travel_df
        combined_hotel = new_hotel_df
        combined_cab = new_cab_df

    # Save back
    with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
        combined_travel.to_excel(writer, sheet_name="Travel", index=False)
        combined_hotel.to_excel(writer, sheet_name="Hotel", index=False)
        combined_cab.to_excel(writer, sheet_name="Cab", index=False)

    print(f"\n✅ Data appended to {output_file}")


# ==========================
# 7️⃣ Main Runner
# ==========================

def main():

    folder_path = "Filtered_1"

    all_results = []

    print("📂 Scanning folder for .eml files...\n")

    for filename in os.listdir(folder_path):
        if filename.endswith(".eml"):

            file_path = os.path.join(folder_path, filename)
            print(f"\n📧 Processing: {filename}")

            attachment_text = extract_attachment_text(file_path)

            if not attachment_text:
                print("⚠ No readable PDF attachment found. Skipping.")
                continue

            try:
                result = extract_from_attachment(attachment_text, file_path)
                all_results.append(result)
                print("✅ Extraction successful.")

            except Exception as e:
                print(f"❌ Failed to extract from {filename}: {e}")
                continue

    if not all_results:
        print("\n⚠ No valid travel/hotel data found in any files.")
        return

    # Merge all results into one
    combined_travels = []
    combined_hotels = []
    combined_cabs = []

    for result in all_results:
        combined_travels.extend(result.travels)
        combined_hotels.extend(result.hotels)
        combined_cabs.extend(result.cabs)

    final_result = AttachmentExtraction(
        travels=combined_travels,
        hotels=combined_hotels,
        cabs=combined_cabs
    )

    save_to_excel(final_result)

    print("\n🎉 All files processed successfully!")

if __name__ == "__main__":
    main()

