import os
from email import policy
from email.parser import BytesParser
from dotenv import load_dotenv
from typing import Optional, List
import pdfplumber
import pandas as pd
import json
import re

# LangChain Imports
from langchain_groq import ChatGroq
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import PydanticOutputParser
from pydantic import BaseModel


# ==========================
# 1️⃣ Load Environment Variables
# ==========================
load_dotenv()

GROQ_API_KEY = os.getenv("GROQ_API_KEY")
if not GROQ_API_KEY:
    raise ValueError("GROQ_API_KEY not found in .env file")


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
    expense: Optional[float]


class HotelData(BaseModel):
    guest_name: Optional[str]
    hotel_name: Optional[str]
    city: Optional[str]
    check_in: Optional[str]
    check_out: Optional[str]
    total_amount: Optional[float]
    number_of_nights: Optional[int]


class AttachmentExtraction(BaseModel):
    travels: List[TravelData]
    hotels: List[HotelData]


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

                filepath = os.path.join("attachments", filename)
                payload = part.get_payload(decode=True)

                with open(filepath, "wb") as f:
                    f.write(payload)

                if filename.lower().endswith(".pdf"):
                    print("📄 Extracting text from PDF...")
                    with pdfplumber.open(filepath) as pdf:
                        for page in pdf.pages:
                            attachment_text += page.extract_text() or ""

    return attachment_text.strip()


# ==========================
# 4️⃣ LLM Setup
# ==========================

llm = ChatGroq(
    model="llama-3.3-70b-versatile",
    temperature=0
)

parser = PydanticOutputParser(pydantic_object=AttachmentExtraction)

prompt = ChatPromptTemplate.from_messages([
    ("system", "You are a professional travel and hotel booking extraction system."),
    ("user", """
Extract all travel and hotel booking information from the document below.

TRAVEL fields:
- passenger_name
- airline
- pnr
- flight_number
- from_location
- to_location
- travel_date
- expense

HOTEL fields:
- guest_name
- hotel_name
- city
- check_in
- check_out
- total_amount
- number_of_nights

Rules:
- If no travel bookings exist, return: "travels": []
- If no hotel bookings exist, return: "hotels": []
- NEVER return null inside lists
- Do NOT guess
- Return valid JSON only

{format_instructions}

Document Content:
{attachment_text}
""")
])


# ==========================
# 5️⃣ Extraction Function
# ==========================

def extract_from_attachment(attachment_text: str) -> AttachmentExtraction:

    formatted_prompt = prompt.format(
        attachment_text=attachment_text,
        format_instructions=parser.get_format_instructions()
    )

    response = llm.invoke(formatted_prompt)

    raw_output = response.content.strip()

    if not raw_output:
        raise ValueError("❌ LLM returned empty response")

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

    return AttachmentExtraction.model_validate(parsed_json)

# ==========================
# 6️⃣ Save to Excel
# ==========================

def save_to_excel(result: AttachmentExtraction, output_file="travel_output.xlsx"):

    # Convert extracted data to rows
    travel_rows = [{
        "Passenger Name": t.passenger_name,
        "Airline": t.airline,
        "PNR": t.pnr,
        "Flight Number": t.flight_number,
        "From Location": t.from_location,
        "To Location": t.to_location,
        "Travel Date": t.travel_date,
        "Expense": t.expense
    } for t in result.travels]

    hotel_rows = [{
        "Guest Name": h.guest_name,
        "Hotel Name": h.hotel_name,
        "City": h.city,
        "Check In": h.check_in,
        "Check Out": h.check_out,
        "Total Amount": h.total_amount,
        "Number of Nights": h.number_of_nights
    } for h in result.hotels]

    new_travel_df = pd.DataFrame(travel_rows)
    new_hotel_df = pd.DataFrame(hotel_rows)

    # If file exists → append
    if os.path.exists(output_file):
        existing_travel = pd.read_excel(output_file, sheet_name="Travel")
        existing_hotel = pd.read_excel(output_file, sheet_name="Hotel")

        combined_travel = pd.concat([existing_travel, new_travel_df], ignore_index=True)
        combined_hotel = pd.concat([existing_hotel, new_hotel_df], ignore_index=True)
    else:
        combined_travel = new_travel_df
        combined_hotel = new_hotel_df

    # Save back
    with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
        combined_travel.to_excel(writer, sheet_name="Travel", index=False)
        combined_hotel.to_excel(writer, sheet_name="Hotel", index=False)

    print(f"\n✅ Data appended to {output_file}")


# ==========================
# 7️⃣ Main Runner
# ==========================

def main():

    folder_path = "/Users/aryankharate/Travel_Detail_Extractor/emails"

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
                result = extract_from_attachment(attachment_text)
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

    for result in all_results:
        combined_travels.extend(result.travels)
        combined_hotels.extend(result.hotels)

    final_result = AttachmentExtraction(
        travels=combined_travels,
        hotels=combined_hotels
    )

    save_to_excel(final_result)

    print("\n🎉 All files processed successfully!")

if __name__ == "__main__":
    main()