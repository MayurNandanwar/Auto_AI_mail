import os
import uuid
from imapclient import IMAPClient
import pyzmail
from io import BytesIO
from datetime import datetime
import psycopg2
import time
import csv
import json
import time
from dateutil import parser
from pdf2image import convert_from_path
from datetime import datetime
from PIL import Image
import base64
from langchain_core.prompts import ChatPromptTemplate
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.output_parsers import JsonOutputParser
from dotenv import load_dotenv
import shutil
import pandas as pd
import uuid
import smtplib
import base64
import uuid
import smtplib
from email.message import EmailMessage
from datetime import datetime, timedelta
from langchain_google_genai import ChatGoogleGenerativeAI
from flask import Flask
from apscheduler.schedulers.background import BackgroundScheduler
import logging
import re
import requests
from requests.auth import HTTPBasicAuth
from itertools import cycle

load_dotenv()

GOOGLE_API_KEYS = os.getenv("GOOGLE_API_KEYS", "").split(",")

if not GOOGLE_API_KEYS or GOOGLE_API_KEYS == [""]:
    raise ValueError("No Google API keys found in env")

google_key_cycle = cycle(GOOGLE_API_KEYS)


def get_gemini_model(api_key):

    return ChatGoogleGenerativeAI(
        model="gemini-2.5-flash-lite",
        temperature=0,
        max_tokens=None,
        timeout=None,
        max_retries=0,
        google_api_key=api_key
    )


LOG_FILE =f"{datetime.now().strftime('%d_%m_%Y')}.log"
logs_path = os.path.join(os.getcwd(),"logs")
os.makedirs(logs_path,exist_ok=True)

LOG_FILE_PATH = os.path.join(logs_path,LOG_FILE)

yesterday = (datetime.now() - timedelta(days=1)).strftime("%d_%m_%Y") + ".log"
yesterday_log_path = os.path.join(logs_path, yesterday)

if os.path.isfile(yesterday_log_path):
    os.remove(yesterday_log_path)


logging.basicConfig(
    filename=LOG_FILE_PATH,
    format= "[ %(asctime)s ] %(lineno)d %(name)s - %(levelname)s - %(message)s",
    filemode= 'a',
    level = logging.INFO
)


EMAIL = os.getenv("EMAIL")
SENDER_MAIL = os.getenv("SENDER_MAIL")
PASSWORD = os.getenv("PASSWORD")
IMAP_SERVER = os.getenv("IMAP_SERVER")
DOWNLOAD_FOLDER = "attachments"

# PostgreSQL config
DB_CONFIG =  {
    'host': os.getenv("DB_HOST"),
    'port':  int(os.getenv("DB_PORT")),
    'database':  os.getenv("DATABASE"),
    'user':  os.getenv("DB_USER"),
    'password':  os.getenv("DB_PASSWORD")
    }

app = Flask(__name__)
google_api = os.getenv('GOOGLE_API_KEY')
sap_username =  os.getenv('SAP_USERNAME')
sap_password =  os.getenv('SAP_PASSWORD')
scheduler = BackgroundScheduler()


prompt_template = """You are a deterministic information extraction engine.

  Your task is to extract structured data ONLY from the provided document.
  You MUST follow all rules strictly and in the given order.
  Do not provide hallucinated or wrong answers. Extract text exactly as it appears.

  ====================
  HANDLING INSTRUCTIONS FOR UNCLEAR IMAGES
  ====================
  - If the image is tilted or skewed, mentally correct the orientation and read accordingly.
  - If any text is blurry or partially visible, use context clues from surrounding text
    to make your best inference.
  - If a field is completely unreadable, mark it as [UNREADABLE].
  - If a field is not present in the invoice, mark it as [NOT FOUND].
  - Do NOT skip rows from the material table — extract every line item visible.
  - If numbers are ambiguous, prefer the interpretation that makes mathematical sense.
  - Verify your extracted line item totals match the invoice subtotal/total where possible.

  ====================
  DOCUMENT TYPE RULE
  ====================
  If the document text contains any of the following (case-insensitive):
  "TAX INVOICE", "Tax Invoice", "TEX INVOICE"
  Then include: "Tax Invoice": Yes, Otherwise: "Tax Invoice": No

  ====================
  PAN AND GSTIN — CORE UNDERSTANDING (READ THIS FIRST BEFORE ANY EXTRACTION)
  ====================

  PAN:
  - Always exactly 10 characters
  - Format: 5 LETTERS + 4 DIGITS + 1 LETTER
  - Example: AABFG8026H
  - Labels: "PAN", "PAN No", "PAN Number", "Company's PAN", "PAN/IT No"

  GSTIN:
  - Always exactly 15 characters
  - Format: 2 digit state code + 10 char PAN + 1 digit + 1 alphanumeric + 1 check char
  - Example: 24AABFG8026H1ZN
  - Labels: "GSTIN", "GSTIN/UIN", "GST No", "GSTIN No", "GST Registration No"

  RELATIONSHIP BETWEEN PAN AND GSTIN:
  - GSTIN characters at position [2:12] will always equal the PAN of that same entity
  - Example: GSTIN = 24AABFG8026H1ZN → PAN = AABFG8026H
  - Use this ONLY as a validation cross-check, not as a replacement for reading

  DERIVING PAN FROM GSTIN (when PAN not explicitly printed):
  - If PAN label is not found anywhere in the document for vendor or buyer →
    derive PAN by extracting characters at position [2:12] from their GSTIN
  - Example: vendor GSTIN = 24AABFG7831N1Z7 → vendors pan no = AABFG7831N

  STRICT SEPARATION RULE:
  - NEVER assign a 15-character GSTIN value into a PAN field
  - NEVER assign a 10-character PAN value into a GSTIN field
  - If PAN field accidentally contains 15 characters → extract only chars [2:12] as PAN

  GSTIN CAREFUL READING RULE:
  - Read each GSTIN character by character from its exact location in the document
  - Do not rely on memory or assumption — go back to source text each time
  - After extracting any GSTIN re-read it one more time independently before finalizing
  - Pay extra attention to the last 4 characters as they are most prone to misread
    due to image skew, blur, or stamp overlap

  CROSS-VALIDATION RULE (use as correction trigger, not rejection):
  - After extracting both GSTIN and PAN for vendor → check if GSTIN[2:12] == PAN
  - After extracting both GSTIN and PAN for buyer → check if GSTIN[2:12] == PAN
  - If they do NOT match → do NOT reject or guess
    Go back and re-read GSTIN carefully from its source location
    Also re-read PAN carefully from all pages (PAN may be on a different page)
    Accept the reading that makes both values consistent

  ====================
  GSTIN CLASSIFICATION — VENDOR vs BUYER (MOST CRITICAL STEP)
  ====================

  Follow this exact 3-step process before assigning any GSTIN:

  STEP 1 — COLLECT ALL GSTINs FROM ENTIRE DOCUMENT:
  Scan the complete document and list every 15-character alphanumeric string
  that starts with 2 digits. These are all GSTINs present in the document.

  STEP 2 — IDENTIFY BUYER GSTIN FIRST (Buyer details are always in a fixed area):
  Buyer information is always printed in a dedicated fixed block on the invoice.
  A GSTIN belongs to BUYER if it appears inside this buyer block near ANY of:
    "Bill To", "Buyer", "M/s" (in buyer address section),
    "Place of Supply", "Consignee", "Ship To",
    "GSTIN No." label appearing beside buyer name or address
  → Assign this as "buyers gstin no"
  → This is the ONLY location to look for buyer GSTIN
  → Do NOT pick buyer GSTIN from any other location

  STEP 3 — IDENTIFY VENDOR GSTIN (Vendor GSTIN location varies across invoices):
  Any GSTIN that was NOT assigned to buyer in Step 2 belongs to the VENDOR.
  Vendor GSTIN can appear in ANY of these locations — scan ALL of them:

    LOCATION 1 — Header / Top of Invoice:
      Near vendor company name, letterhead, "Bill From", "Supplier", "M/s"
      This is the most common location.

    LOCATION 2 — Below Material Table:
      After the last item row, near bank details, subtotal, or grand total.
      A 15-character alphanumeric string starting with 2 digits in this zone
      is the vendor GSTIN.
      Example: "GSTIN No.: 24AABFG7831N1Z7" near bank details = vendor GSTIN

    LOCATION 3 — Inside Material Table:
      Sometimes printed as a standalone row or cell within the table itself.
      If a cell contains a 15-character alphanumeric string starting with
      2 digits → it is a GSTIN not a product row.
      Extract and classify as vendor GSTIN.

    LOCATION 4 — Footer:
      At the very bottom of the page in smaller or lighter font.

  SCAN PRIORITY: Header → Below Table → Inside Table → Footer

  CRITICAL RULES:
  - NEVER assign a GSTIN from below the table or footer to buyer
  - NEVER assign the buyer block GSTIN to vendor
  - If only ONE GSTIN found in entire document → it belongs to vendor
    Return NA for buyers gstin no
  - If TWO GSTINs found → classify using Step 2 and Step 3 above
  - NEVER assign same GSTIN to both vendor and buyer
  - NEVER leave vendor gstin no as NA if any unassigned 15-char GSTIN
    exists anywhere in the document

  ====================
  HEADER FIELDS
  ====================

  In output use EXACT key names below:

  - "buyer" (this field is must)
    → Company name from the fixed buyer block
    → Labels: "Bill To", "Buyer", "M/s" in buyer section
    → If label missing, use clearly identified buyer company name from buyer area

  - "buyers pan no" (this field is must)
    → Extract ONLY the 10-character PAN value
    → Labels: "PAN", "PAN No", "PAN Number", "PAN/IT No", "Buyer PAN"
    → Must be exactly 10 characters: 5 letters + 4 digits + 1 letter
    → Look inside the buyer block first
    → PAN may also appear on a different page — scan ALL pages
    → If PAN label is not found anywhere → derive from buyers gstin no[2:12]
    → If buyers gstin no is also NA → return NA

  - "buyers gstin no" (this field is must)
    → Extract ONLY from the fixed buyer block area (see GSTIN CLASSIFICATION Step 2)
    → Must be exactly 15 characters
    → Re-read last 4 characters one more time before finalizing
    → Validate: GSTIN[2:12] should match buyers pan no
      If mismatch → re-read from buyer block carefully, also re-check PAN from all pages

  - "buyers order number" (this field is must)
    → May appear anywhere in the document
    → Labels (case-insensitive):
      Buyers Order No, Buyer Order Number, Customer Order No,
      Customer Order Number, Order No, Order Number, Order No.,
      Purchase Order, Purchase Order No, PO, P.O., PO No,
      P.Order, Account PO, Customer Reference No

  - "vendor" (this field is must)
    → Company name of the seller/supplier
    → Look in: document header, letterhead, "Bill From", "Supplier",
      "For [Company Name]" signature block at bottom of invoice
    → If "Bill From" label is missing, use detected seller company name

  - "vendors pan no" (this field is must)
    → Extract ONLY the 10-character PAN value
    → Labels: "PAN", "PAN No", "PAN Number", "Company's PAN"
    → Must be exactly 10 characters: 5 letters + 4 digits + 1 letter
    → Vendor PAN location also varies — scan ALL locations and ALL pages
    → Common locations: header, footer, last page, near bank details
    → If PAN label is not found anywhere → derive from vendor gstin no[2:12]

  - "vendor gstin no" (this field is must)
    → Follow GSTIN CLASSIFICATION Step 3 above
    → Scan ALL locations: header, below table, inside table, footer
    → Must be exactly 15 characters
    → Re-read last 4 characters one more time before finalizing
    → NEVER leave as NA if any unassigned 15-char GSTIN exists in document
    → Validate: GSTIN[2:12] must match vendors pan no
      If mismatch → re-read both from source carefully

  - "invoice number" (this field is must)
    → Labels (case-insensitive):
      Invoice Number, Invoice No, Invoice No., Inv. No

  - "invoice date" (this field is must)
    → Labels (case-insensitive):
      Invoice Date, Inv. Date, Dated, Date

  ====================
  MULTI-PAGE INVOICE RULE (STRICT)
  ====================

  Some invoices span multiple pages. Each page repeats the header
  (vendor, buyer, GSTIN, invoice number) but the material table
  continues from where the previous page ended.

  RULES:
  1. Treat the ENTIRE invoice as ONE single document, not separate pages.
  2. Extract material rows ONLY ONCE — do not repeat rows already extracted
     from a previous page.
  3. If a later page header repeats the same sr no 1, 2, 3... already seen
     on page 1 → these are repeated header rows, SKIP them entirely.
  4. A row is DUPLICATE if it has same sr no + hsn + qty + amount
     as a previously extracted row → discard it, keep only first occurrence.
  5. Rows on later pages with sr no CONTINUING from previous page
     (e.g., page 1 ends at sr 5, page 2 starts at sr 6) →
     these are NEW rows, include them.
  6. Special adjustment rows like "R. OFF", "Round Off", "Less: R. OFF"
     with no sr no → extract ONCE only, do not repeat across pages.

  DEDUPLICATION STEP (mandatory before returning output):
    After collecting all rows across all pages:
    - Remove any row where (sr no + hsn + unit price) combination
      already exists in the list — keep only first occurrence.
    - For adjustment/round-off rows → keep only one instance total.

  ====================
  MATERIAL TABLE RULE
  ====================

  Use ONLY ONE key for materials:
  "material": [ list of objects ]

  Each material item may contain ONLY the following keys
  (use EXACT names, no variations):

  - "sr no" (this field is must)
    Labels: Sr No, SrNo, SI No., Item, Sr, S No.

  - "hsn" (this field is must)
    Labels: HSN code, HSN, HSN/SAC, SAC Code

  - "qty" (this field is must)
    Labels: Qty, Quantity
    Extract ONLY the numeric value — do NOT include unit text.
    Example: "2 NOS." → qty = "2"
    Example: "5 KGS" → qty = "5"
    Example: "10" → qty = "10"
    Note: if unit text appears inside qty cell → extract unit text
          separately into the "unit" field, not here.

  - "unit" (this field is must)
    Labels: Unit, per, UoM
    Note: if column not available → look for unit text inside qty cell
    If still not found → set as None

  - "unit price" (this field is must)
    Labels: Rate, Unit Price, Price

  - "discount" (this field is must)
    Labels: Discount, Dis, Disc, Disc.%
    Scan the row AND summary section. Return numeric value only.
    If value is 0.00 → return "0.00", do NOT return NA
    If column not present and no discount found anywhere → return NA

  - "taxable amt" (this field is must)
    Labels: Taxable Amt, Taxable Amount, Taxable Value, Taxable Value Currency INR
    Note: if no taxable amt column exists in the row →
    derive it as: qty × unit price − discount amount

  - "cgst %" (this field is must)
    Labels: CGST, CGST rate, CGST %, OUTPUT CGST @, Central Tax,
            GST % (when split equally between CGST and SGST)
    Extract numeric value only — do NOT include "%" symbol

  - "cgst amount" (this field is must)

  - "sgst %" (this field is must)
    Labels: SGST, SGST rate, SGST %, OUTPUT SGST @, State/UT Tax,
            GST % (when split equally between CGST and SGST)
    Extract numeric value only — do NOT include "%" symbol

  - "sgst amount" (this field is must)
    Labels: SGST Amount, SGST, State/UT Tax amount

  - "amount" (this field is must)
    Labels: Amount, Line Total, Net Amount

  ====================
  CGST / SGST EXTRACTION RULE (STRICT)
  ====================

  CGST and SGST values appear in TWO possible places — check BOTH:

  CASE A — Inline in Material Table Row:
  → Each row has its own CGST %, CGST Amount, SGST %, SGST Amount columns
  → Extract directly from each row

  CASE B — Summary Box at Bottom of Invoice:
  → A single combined CGST and SGST amount shown for all items together
  → Labels: "OUTPUT CGST", "OUTPUT SGST", "Central Tax", "State/UT Tax"
  → PROPORTIONAL DISTRIBUTION (mandatory — do NOT split equally):
      For each item row:
      item_cgst_amount = ROUND((item_taxable_amt / total_taxable_amt) × total_cgst_amount, 2)
      item_sgst_amount = ROUND((item_taxable_amt / total_taxable_amt) × total_sgst_amount, 2)
  → For CGST % and SGST % → extract percentage from summary label
      e.g. "Central Tax 9.00%" → cgst % = "9.00"
      e.g. "OUTPUT CGST @ 18%" → cgst % = "18"

  SINGLE GST % COLUMN CASE:
  → Some invoices have ONE "GST %" column instead of separate CGST and SGST
  → cgst % = sgst % = GST% value / 2
  → Example: GST % = 18 → cgst % = "9", sgst % = "9"
  → Calculate cgst amount and sgst amount proportionally per item

  COMBINED CASE (both inline and summary exist):
  → Prefer inline row values if clearly readable
  → Use summary box values only as fallback or validation

  NEVER return "NA" for cgst amount / sgst amount if ANY tax value
  exists anywhere in the document — even in footer or summary box.

  VALIDATION:
  cgst amount ≈ (taxable amt × cgst %) / 100
  sgst amount ≈ (taxable amt × sgst %) / 100

  Note: Sometimes CGST % + amount and SGST % + amount appear in same cell.
  Split them correctly and map to respective keys.

  ====================
  ROUND OFF / ADJUSTMENT ROW RULE
  ====================

  Rows labeled "R. OFF", "Round Off", "Less: R. OFF", "Less:", "Adjustment"
  that have no sr no and only an amount value:
  - Extract as ONE row only with all other fields as 'NA'
  - Do NOT repeat this row across multiple pages
  - Extract amount value as-is including negative sign
    Example: "(-)0.39" → amount = "-0.39"
    Example: "-0.40" → amount = "-0.40"

  ====================
  MATERIAL TABLE RULES (KEY POINTS — MUST FOLLOW)
  ====================
  - SR NO MUST BE A NUMBER:
    Sr no is always a small sequential integer (1, 2, 3...).
    If the value in the sr no column is a word or label such as:
    "Less:", "Less", "R. OFF", "Round Off", "Total", "Sub Total",
    "Note", "Adjustment" or any non-numeric text →
    that row is NOT a valid material row — SKIP it entirely.
    Do NOT extract it even if it has an amount value.

  - Do NOT extract item descriptions.
  - If "unit" is missing:
      - First try extracting unit from qty text.
      - If still unavailable → set "unit" to None
  - Sometimes all data is scattered — extract from any location accordingly.
  - If a key-value is not found in expected section →
    scan ENTIRE document including footer, margin, and summary boxes.
    Do NOT return "NA" if information exists anywhere on the page.
  - Treat all text inside () brackets as non-existent.
    Extract ONLY from text outside brackets.
  - Do NOT extract "%" symbol from any numeric value.
  - All extracted values must be of string data type.
  - If value for a key is not available → keep as 'NA'
  - If value is unreadable due to stamp or obstruction →
    keep as 'NA' and continue extracting other columns.
    Same rule applies for Discount column.
  -  A line without a sr no that appears directly below an item row
    is a continuation of that item's description — SKIP it entirely

  ====================
  HEADER-LESS TABLE HANDLING
  ====================

  If the material table has NO headers:

  - If vendor == "Mogli Labs (India) Pvt Ltd":
    Assume column order:
    sr no, hsn, qty, unit, unit price, taxable amt,
    cgst %, cgst amount, sgst %, sgst amount, amount

  - If vendor == "GHCL LTD":
    Assume column order:
    sr no, hsn, qty, unit, unit price, discount, taxable amt,
    cgst %, cgst amount, sgst %, sgst amount, amount

  ====================
  FINAL VALIDATION CHECKLIST (Run before returning output — MANDATORY)
  ====================

  1. PAN VALIDATION:
     - Is vendors pan no exactly 10 characters? Format: 5 letters + 4 digits + 1 letter?
     - Is buyers pan no exactly 10 characters? Format: 5 letters + 4 digits + 1 letter?
     - If either PAN field contains 15 characters → wrong, re-extract chars [2:12] only
     - If PAN not found in document → derive from respective GSTIN[2:12]
     - PAN may be on a different page — confirm you scanned all pages

  2. GSTIN VALIDATION:
     - Did you collect ALL GSTINs from entire document first (Step 1)?
     - Did you identify buyer GSTIN ONLY from the fixed buyer block (Step 2)?
     - Did you assign remaining unassigned GSTIN to vendor (Step 3)?
     - Is vendor gstin no exactly 15 characters?
     - Is buyers gstin no exactly 15 characters?
     - Does vendor gstin no[2:12] == vendors pan no?
       If not → re-read both from source carefully
     - Does buyers gstin no[2:12] == buyers pan no?
       If not → re-read both from source carefully
     - Re-read last 4 characters of each GSTIN one more time independently

  3. GSTIN CONFLICT CHECK:
     - Is vendor gstin no ≠ buyers gstin no?
     - If same value assigned to both → re-scan and correct immediately

  4. UNASSIGNED GSTIN CHECK:
     - Is there any 15-character alphanumeric string in the document
       NOT yet assigned to vendor or buyer?
     - If yes → determine ownership by location and assign accordingly

  5. CGST / SGST CHECK:
     - Did you check both inline rows AND summary box for each item?
     - For summary box → did you distribute PROPORTIONALLY per item
       based on each item's taxable amount — NOT equally split?
     - Is any cgst amount or sgst amount still "NA"?
       If yes → re-scan summary box and distribute proportionally to rows
     - Validate: cgst amount ≈ (taxable amt × cgst %) / 100

  6. DUPLICATE ROW CHECK:
     - Are there any duplicate material rows (same sr no + hsn + unit price)?
     - If yes → remove duplicates, keep only first occurrence
     - Is the adjustment/round-off row appearing more than once?
       If yes → keep only one instance

  7. DISCOUNT CHECK:
     - If invoice has a Disc.% or Discount column → ensure values are extracted
     - A value of 0.00 is valid — do not replace with NA

Final Result:
  
  Return result in the following exact JSON structure. 
  in result output: not need to extract given keys:"total taxable amount","total cgst amount","total sgst amount","total_igst_amount"

  RULES:
  - Every material row must contain ALL keys listed above.
  - Header fields (TaxInvoice, Buyer, BuyersPanNo, BuyersGstinNo,
    BuyersOrderNumber, Vendor, VendorPanNo, VendorGstinNo,
    InvoiceNo, InvoiceDate) must be repeated in every row.
  - Do NOT add any extra keys beyond what is listed above.
  - Do NOT rename any key.
  - Return ONLY the JSON only JSON Not list — no extra text, explanation, or markdown.
"""



def image_to_text(image_path, prompt_template, output_dir=None):
    try:
        logging.info('Enter into image_to_text function.')
        # Apply output_dir FIRST
        if output_dir is not None:
            image_path = f'{output_dir}/{image_path}'

        # Extract extension AFTER final path
        ext = image_path.split('.')[-1].lower()
        logging.info(f'ext: {ext}')

        with open(image_path, "rb") as img_file:
            img_bytes = img_file.read()

        # Convert PNG → JPEG in memory
        if ext == "png":
            logging.info('converting png to jpeg')
            img = Image.open(BytesIO(img_bytes)).convert("RGB")

            buffer = BytesIO()
            img.save(buffer, format="JPEG", quality=95)
            buffer.seek(0)

            img_bytes = buffer.getvalue()
            ext = "jpeg"   # IMPORTANT: update ext

        # Base64 encode
        b64_string = base64.b64encode(img_bytes).decode('utf-8')

        messages = [
            (
                "user",
                [
                    {"type": "text", "text": prompt_template},
                    {
                        "type": "image_url",
                        "image_url": {
                            "url": f"data:image/{ext};base64,{b64_string}"
                        },
                    },
                ],
            )
        ]

        last_error = None
        for _ in range(len(GOOGLE_API_KEYS)):
            logging.info(f'in loop:{_}')
            api_key = next(google_key_cycle)
            gemini_model = get_gemini_model(api_key)

            try:
                prompt = ChatPromptTemplate.from_messages(messages)
                chain = prompt | gemini_model | JsonOutputParser()
                result = chain.invoke({"b64_img": b64_string, "ext": ext})
                time.sleep(10)
                #logging.info(f'res:{result}')
                return result
            except Exception as e:
                error_msg = str(e).lower()
                last_error = e

                # only switch key if quota/rate-limit
                if "quota" in error_msg or "429" in error_msg or "rate limit" in error_msg:
                    logging.error(f"API key exhausted → switching key")
                    continue

                else:
                    raise e

        raise RuntimeError("All Google API keys exhausted") from last_error

    except (RuntimeError,Exception) as e:
        logging.error(str(e))
        return str(e)


def extract_page_no(filename):
    match = re.search(r'page_(\d+)_', filename, re.IGNORECASE)
    return int(match.group(1)) if match else 0

def normalize_date(date_str):
    try:
        if re.match(r"^\d{4}-\d{2}-\d{2}$", date_str):
            return date_str
        date_obj = parser.parse(date_str, dayfirst=True)
        return date_obj.strftime("%Y-%m-%d")
    except Exception:
        return None



def post_pdf_data_to_sap(username,password,json_data):
    json_data = json.dumps(json_data,indent=4,ensure_ascii=False)


    header= {
        "Content-Type": "application/json",
        "X-Requested-With":'X',
        "Accept": "application/json"
        }

    req_response = requests.post(url="https://S4HANASADEV.ghclindia.net:44300/sap/opu/odata/SAP/YOCR_IMG_SRV/YINVMEDSet",
                            headers=header,
                            data=json_data,
                            auth=HTTPBasicAuth(username, password))

    if req_response.status_code == 201:
        return req_response.status_code,req_response.text
    else:
        return req_response.status_code,req_response.text

def post_data_to_sap(username,password,json_data):
    json_data = json.dumps(json_data,indent=4,ensure_ascii=False)

    header= {
        "Content-Type": "application/json",
        "X-Requested-With":'X',
        "Accept": "application/json"
        }

    req_response = requests.post(url="https://S4HANASADEV.ghclindia.net:44300/sap/opu/odata/sap/YOCR_INV_SRV/OCRHEADSet",
                            headers=header,
                            data=json_data,
                            auth=HTTPBasicAuth(username, password))

    if req_response.status_code == 201:
        return req_response.status_code,req_response.text
    else:
        return req_response.status_code,req_response.text


def pdf_to_text(pdf_path):

    logging.info("Enter in pdf to text function")
    valid_file_folder = '/home/mayur/chatbot/Auto_Invo_Extractor/valid_invoice_folder/'
    invalid_file_folder = '/home/mayur/chatbot/Auto_Invo_Extractor/Invalid_invoice_folder/'
    invalid_file_pdf_folder = '/home/mayur/chatbot/Auto_Invo_Extractor/Invalid_invoice_pdf_folder/'
    # valid_file_folder = 'C:/Users/G01889/OneDrive/Documents/invoice_extractor_auto/InvoExtract/valid_invoice_folder/'
    # invalid_file_folder = 'C:/Users/G01889/OneDrive/Documents/invoice_extractor_auto/InvoExtract/Invalid_invoice_folder/'
    # invalid_file_pdf_folder = 'C:/Users/G01889/OneDrive/Documents/invoice_extractor_auto/InvoExtract/Invalid_invoice_pdf_folder/'
    if pdf_path.endswith('pdf'):
        logging.info('File Format is PDF.')

        # Output directory for images
        output_dir = os.path.basename(pdf_path).split('.')[0]

        os.makedirs(output_dir, exist_ok=True)

        # Convert PDF to list of images (one per page)
        
        images = convert_from_path(pdf_path, dpi=300,poppler_path='/usr/bin')
        # images = convert_from_path(pdf_path, dpi=300,poppler_path=r'C:\Program Files\poppler-24.08.0\Library\bin')
        # Save each page as a JPEG file
        for i, image in enumerate(images):
            image_path = os.path.join(output_dir, f"page_{i}_{uuid.uuid4().hex}.jpeg")
            #image_path = os.path.join(output_dir, f"{output_dir}_{i + 1}.jpeg")
            image.save(image_path, 'JPEG')

        img_file_lst = sorted(os.listdir(output_dir), key=extract_page_no)
        if len(img_file_lst)!=0:
            page_wise_res_final = []
            for image_file in img_file_lst:
                logging.info(f'in image:{image_file}')
                result = image_to_text(image_file,prompt_template,output_dir)
                if isinstance(result,str):
                    shutil.rmtree(output_dir)
                    return 0

                page_wise_res_final.append(result)

            logging.info(f'Combine Image result in List:{page_wise_res_final}')
            final_dict = page_wise_res_final[0]
            for i in range(1,len(page_wise_res_final)):
                if "material" in page_wise_res_final[i]:
                    final_dict['material'].extend(page_wise_res_final[i]['material'])

            logging.info(f"Final List of result: {final_dict}")

            if not isinstance(final_dict,str):
                if final_dict['Tax Invoice'] == 'No':
                    # destination_file = os.path.basename(pdf_path)
                    shutil.copy(pdf_path,invalid_file_pdf_folder+output_dir)
                    shutil.rmtree(output_dir)
                    return "TAX INVOICE Not Found."

                elif final_dict["buyers gstin no"] != "24AAACG5609C1Z5":
                    # destination_file = os.path.basename(pdf_path)
                    shutil.copy(pdf_path,invalid_file_pdf_folder+output_dir)
                    shutil.rmtree(output_dir)
                    return "Buyers GST number is not 24AAACG5609C1Z5"

                elif final_dict["invoice date"]:
                    date_obj = normalize_date(final_dict["invoice date"])

                    date = pd.to_datetime(date_obj).date()

                    final_dict["invoice date"] = str(date)

                    # date = pd.to_datetime(final_dict["invoice date"]).date()
                    if date > datetime.now().date():
                        shutil.copy(pdf_path,invalid_file_pdf_folder+output_dir)
                        shutil.rmtree(output_dir)
                        return "Invoice date should not be in future."
                    else:
                        if not final_dict['buyers order number'].startswith('45'):
                            final_dict['Status'] = 'HE'
                            final_dict['ErrorMessage'] = "Po number not readable"
                        else:
                            if  len(final_dict['buyers order number']) != 10:
                                final_dict['Status'] = 'HE'
                                final_dict['ErrorMessage'] = "Po number not readable"
                            else:
                                final_dict['Status'] = 'HS'
                                final_dict['ErrorMessage'] = "Invoice validation Successful."

                        for dct in final_dict['material']:
                            for key, val in dct.items():
                                if isinstance(val, str):
                                    dct[key] = val.replace(' ', '')

                        mat_df = pd.DataFrame(final_dict['material'])

                        if 'sr no' in final_dict:
                            mat_df = mat_df[~mat_df['sr no'].isna()]
                            mat_df = mat_df[~mat_df.duplicated(subset=['sr no'])]

                        mat_df = mat_df.fillna("NA")
                        mat_df.replace('nan','Na',inplace=True)
                        mat_df.drop_duplicates(inplace=True)

                        invoice_status = final_dict['Status']

                        final_dict['material'] = mat_df.to_dict(orient='records')

                        final_dict['buyer'] = final_dict['buyer'][:30]
                        final_dict['vendor'] = final_dict['vendor'][:30]

                        final_dict,invoice_no = json_format_conversion(final_dict)

                        final_df_1 = pd.DataFrame(final_dict['Material'])

                        if invoice_status == 'HE':
                            final_df_1['Status'] = 'IE'
                            final_df_1['ErrorMessage'] = "Po number not readable"
                        else:
                            final_df_1['Status'] = 'IS'
                            final_df_1['ErrorMessage'] = "Invoice validation Successful."

                        final_df_1['DataInputDate'] = str(datetime.now().date())
                        final_df_1['DataInputTime'] = str(datetime.now().time())
                        
                        final_df_1 = final_df_1[final_df_1['Srno']!='NA']

                        final_res_df = final_df_1.to_dict(orient='records')

                        final_res_dict = {}
                        final_res_dict['Material'] = final_res_df

                        shutil.copy(pdf_path,valid_file_folder+output_dir)
                        shutil.rmtree(output_dir)
                        return final_res_dict,invoice_no
                else:
                    final_dict = 'Not able to capture data from image'
                    shutil.rmtree(output_dir)
                    return final_dict
            else:
                final_dict = 'data not found in pdf'
                shutil.rmtree(output_dir)
                return final_dict


    elif pdf_path.lower().endswith(('jpeg','png','jpg')):
        output_dir = os.path.basename(pdf_path)
        logging.info(f'Image formate so enter in elif: {pdf_path}')
        result = image_to_text(pdf_path,prompt_template)
    
        if isinstance(result,str):
            return str(result)

        if not isinstance(result,str):
            if result['Tax Invoice'] == 'No':
                output_dir = os.path.basename(pdf_path).split('.')[0]
                destination_file = os.path.basename(pdf_path)
                shutil.copy(pdf_path,invalid_file_pdf_folder+destination_file)
                return "Tax Invoice Not Found In Image."

            elif result["buyers gstin no"] != "24AAACG5609C1Z5":
                output_dir = os.path.basename(pdf_path).split('.')[0]
                # os.remove(upload_path)
                # shutil.rmtree(output_dir)
                destination_file = os.path.basename(pdf_path)
                shutil.copy(pdf_path,invalid_file_pdf_folder+destination_file)
                shutil.rmtree(destination_file)
                return "Buyers GST number is not 24AAACG5609C1Z5"
    

            elif result["invoice date"]:
                output_dir = os.path.basename(pdf_path).split('.')[0]

                date_obj = normalize_date(result["invoice date"])

                date = pd.to_datetime(date_obj).date()

                result["invoice date"] = str(date)

                # date = pd.to_datetime(result["invoice date"]).date()
                if date > datetime.now().date():
                    destination_file = os.path.basename(pdf_path)
                    shutil.copy(pdf_path,invalid_file_pdf_folder+destination_file)
                    shutil.rmtree(destination_file)
                    return "Invoice date should not be in future."
                    # result["Invoice date"] = "Invoice date should not be in future."
                else:
                    if not result['buyers order number'].startswith('45'):
                            result['Status'] = 'HE'
                            result['ErrorMessage'] = "Po number not readable"
                    else:
                        if  len(result['buyers order number']) != 10:
                            result['Status'] = 'HE'
                            result['ErrorMessage'] = "Po number not readable"
                        else:
                            result['Status'] = 'HS'
                            result['ErrorMessage'] = "Invoice validation Successful."
                    
                    result['buyer'] = result['buyer'][:30]
                    result['vendor'] = result['vendor'][:30]

                    output_dir = os.path.basename(pdf_path).split('.')[0]

                    invoice_status = result['Status']
                    result, invoice_no = json_format_conversion(result)
                    final_df = pd.DataFrame(result['Material'])

                    if invoice_status == 'HE':
                        final_df['Status'] = 'IE'
                        final_df['ErrorMessage'] = "Po number not readable"

                    elif invoice_status == 'HS':
                        final_df['Status'] = 'IS'
                        final_df['ErrorMessage'] = "Invoice validation Successful."

                    final_df['DataInputDate'] = str(datetime.now().date())
                    final_df['DataInputTime'] = str(datetime.now().time())

                    final_df = final_df[final_df['Srno']!='NA']

                    final_result = final_df.to_dict(orient='records')

                    final_res_dict = {}
                    final_res_dict['Material'] = final_result

                    destination_file = os.path.basename(pdf_path)
                    shutil.copy(pdf_path,valid_file_folder+destination_file)
                    return final_res_dict,invoice_no
            else:
                return 0
        else:
            return 0

def rename_keys(data, mapping):
    return {mapping.get(k, k): v for k, v in data.items()}


def json_format_conversion(json_data):
    InvoiceNo = json_data['invoice number']
    
    invo_dict = {}

    key_need_to_replace = {"invoice number":"InvoiceNo",
                        "Tax Invoice": "TaxInvoice",
                        "buyer": "Buyer",
                        "buyers pan no":"BuyersPanNo",
                        "buyers gstin no":"BuyersGstinNo",
                        "buyers order number":"BuyersOrderNumber",
                        "vendor": "Vendor",
                        "vendors pan no": "VendorPanNo",
                        "vendor gstin no": "VendorGstinNo",
                        "invoice date":"InvoiceDate",
                        "material":"Material"
                        }

    material_key_to_replace = {"sr no":"Srno","hsn":"Hsn","qty":"Qty","unit":"Unit",
                           "unit price":"UnitPrice","discount":"Discount",
                           "taxable amt":"TaxableAmt","cgst %":"Cgst",
                           "cgst amount":"CgstAmount","sgst %":"Sgst",
                           "sgst amount":"SgstAmount","amount":"Amount"}
    
    #['InvoiceNo', 'Hsn', 'Qty', 'Unit', 'UnitPrice', 'Discount', 'TaxableAmt', 'Cgst', 'CgstAmount', 'Sgst', 'SgstAmount', 'Amount', 'TaxInvoice', 'Buyer', 'BuyersPanNo', 'BuyersGstinNo', 'BuyersOrderNumber', 'Vendor', 'VendorPanNo', 'VendorGstinNo', 'InvoiceDate']

    new_json = rename_keys(json_data, key_need_to_replace)
    material = []
    if 'Material' in new_json:
        for i,mat_json in enumerate(new_json['Material']):
            mat_j = rename_keys(mat_json, material_key_to_replace)
            material.append(mat_j)


    new_json['Material'] = material

    data = json.dumps(new_json,indent=4)

    data  = json.loads(data)

    mat_dict = data["Material"]

    del data['Material']

    material_lst = []
    for dict in mat_dict:
        final_dict = dict | data
        material_lst.append(final_dict)


    invo_dict['Material'] = material_lst
    final_json_data = json.dumps(invo_dict,indent=4)
    final_json_data = json.loads(final_json_data)
    logging.info(f'final_json_data:{final_json_data}')
    return final_json_data, InvoiceNo



def download_unread_attachments_now(EMAIL, PASSWORD, IMAP_SERVER, DOWNLOAD_FOLDER):
    logging.info("Downloading the pdf or images from Email")
    os.makedirs(DOWNLOAD_FOLDER, exist_ok=True)
    downloaded_files = []

    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    with IMAPClient(IMAP_SERVER) as client:
        client.login(EMAIL, PASSWORD)
        client.select_folder("INBOX")

        messages = client.search(['UNSEEN'])
        logging.info(f"Unread emails found: {len(messages)}")

        for msgid, data in client.fetch(messages, ['RFC822']).items():
            mail = pyzmail.PyzMessage.factory(data[b'RFC822'])
            sender_email = mail.get_addresses('from')[0][1]

            for part in mail.mailparts:
                filename = part.filename
                if not filename:
                    continue

                lowercase = filename.lower()
                allowed_extensions = [".pdf", ".jpg", ".jpeg", ".png"]
                if not any(lowercase.endswith(ext) for ext in allowed_extensions):
                    continue

                now_dt = datetime.now()
                now_str = now_dt.strftime("%d-%m-%Y_%H-%M-%S")

                # ✅ SAFE & CORRECT: UUID for original attachment (NO page_no here)
                name, ext = os.path.splitext(filename)
                base_filename = f"{name.replace(' ','_')}_{uuid.uuid4().hex}{ext}"
                #base_filename = f"{uuid.uuid4().hex}{ext}"

                save_path = os.path.abspath(
                    os.path.join(DOWNLOAD_FOLDER, base_filename)
                ).replace('\\', '/')

                counter = 1
                while os.path.exists(save_path):
                    save_path = os.path.abspath(
                        os.path.join(DOWNLOAD_FOLDER, f"{uuid.uuid4().hex}_{counter}{ext}")
                    )
                    counter += 1

                payload = part.get_payload()
                if not payload:
                    logging.info(f"Empty attachment skipped: {filename}")
                    continue

                with open(save_path, "wb") as f:
                    f.write(payload)

                final_filename = os.path.basename(save_path)
                logging.info(f"Downloaded: {final_filename}")

                cursor.execute("""
                    INSERT INTO mail_attachments
                    (sender_email, original_filename, saved_filename, file_path, status)
                    VALUES (%s, %s, %s, %s, %s)
                """, (sender_email, filename, final_filename, save_path, 'PENDING'))
                conn.commit()

                downloaded_files.append({
                    "sender": sender_email,
                    "filename": final_filename,
                    "filepath": save_path,
                    "original_filename": filename,
                    "download_datetime": now_str
                })

            client.add_flags(msgid, [b'\\Seen'])

    cursor.close()
    conn.close()
    logging.info("Done processing all unread emails and storing in DB.")
    return downloaded_files



create_script = """
CREATE TABLE mail_attachments (
    id SERIAL PRIMARY KEY,
    sender_email VARCHAR(255) NOT NULL,
    original_filename VARCHAR(255),
    saved_filename VARCHAR(255) NOT NULL,
    file_path TEXT NOT NULL,
    status VARCHAR(50) DEFAULT 'PENDING',  -- PENDING, SUCCESS, FAILED(RESENT Mail)
    created_at TIMESTAMP DEFAULT NOW(),
    status_update_date TIMESTAMP DEFAULT NOW()
); """

def insert_files_to_db(downloaded_files):
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    for file in downloaded_files:
        cursor.execute("""
            INSERT INTO mail_attachments
            (sender_email, original_filename, saved_filename, file_path, status)
            VALUES (%s, %s, %s, %s, %s)
        """, (
            file['sender'],
            file['original_filename'],
            file['filename'],
            file['filepath'],
            'PENDING'
        ))

    conn.commit()
    cursor.close()
    conn.close()
    logging.info("All downloaded files inserted into DB.")


def send_failed_file(subject,invoice_status,sender_email, file_path,file_name):
    logging.info('Enter in send failed_file function.')
    msg = EmailMessage()
    msg['Subject'] = subject #'Your file could not be processed'
    msg['From'] = EMAIL
    msg['To'] = sender_email
    msg.set_content(f"Hello,\n\n{invoice_status}\n\nThanks.")

    # Attach the failed file
    with open(file_path, 'rb') as f:
        file_data = f.read()

    msg.add_attachment(file_data, maintype='application', subtype='octet-stream', filename=file_name)

    # Send email via SMTP
    with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
        smtp.login(EMAIL, PASSWORD)  # Use app password if 2FA
        smtp.send_message(msg)

    # print(f"Failed file sent back to {sender_email}")


def sap_failure(subject,sender_email,file_path,file_name):
    logging.info('Enter in send failed_file function.')
    msg = EmailMessage()
    msg['Subject'] = "Failed to send data in SAP" #'Your file could not be processed'
    msg['From'] = EMAIL
    msg['To'] = sender_email
    msg.set_content(f"Hello,\n\n Failed to send {file_name} file data in SAP\n\nThanks.")

    # Attach the failed file
    with open(file_path, 'rb') as f:
        file_data = f.read()

    msg.add_attachment(file_data, maintype='application', subtype='octet-stream', filename=file_name)

    # Send email via SMTP
    with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
        smtp.login(EMAIL, PASSWORD)  # Use app password if 2FA
        smtp.send_message(msg)

    # print(f"Failed file sent back to {sender_email}")


def process_files():
    try:
        logging.info("Enter into Process File function.")

        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        sql_query  = """SELECT id, sender_email, original_filename, file_path
                        FROM mail_attachments
                        WHERE status = 'PENDING'
                        ORDER BY created_at ASC;
                        """
        cursor.execute(sql_query)
        mail_path_tuple_lst= cursor.fetchall()

        logging.info(f'List if File where status is Pending: {mail_path_tuple_lst}')

        for id_mail_path in mail_path_tuple_lst:
            id = id_mail_path[0]
            sender_mail = id_mail_path[1]
            original_file_name = id_mail_path[2]
            pdf_file_path = id_mail_path[3]

            result,invoice_no = pdf_to_text(pdf_file_path)

            # logging.info(f'json_format:{final_dict}')
            api_response_code,api_response_text = post_data_to_sap(sap_username,sap_password,result)
            if api_response_code == 201:
                logging.info("Successfully send the data to SAP.")
            else:
                subject = "Failed to send data in SAP"
                sap_mail = 'pradhyumansinh@ghcl.co.in'
                sap_failure(subject,sap_mail,pdf_file_path,original_file_name)
                logging.error(f"Status Code: {api_response_code} and error is : {api_response_text}")

            # send pdf binary format to api
            pdf_base64_json = {}

            file_name = os.path.split(pdf_file_path)[1]
            pdf_base64_json['Filename'] = original_file_name
            pdf_base64_json["Mimetype"] = "PDF"
            pdf_base64_json['InvoiceNo'] = invoice_no
            # Encode binary to Base64 string
            # attach file with json
            with open(pdf_file_path, "rb") as f:
                pdf_binary = f.read()

            pdf_base64_decode = base64.b64encode(pdf_binary).decode("utf-8")
            pdf_base64_json['Content'] = pdf_base64_decode

            response_code,response_text = post_pdf_data_to_sap(sap_username,sap_password,pdf_base64_json)
            if response_code == 201:
                logging.info("Successfully send the data to SAP.")
            else:
                subject = "Failed to send Pdf file to SAP"
                sap_mail = 'pradhyumansinh@ghcl.co.in'
                sap_failure(subject,sap_mail,pdf_file_path,original_file_name)
                logging.error(f"Status Code: {response_code} and error is : {response_text}")

            logging.info(f'pdf_to_text function result: {result}')

            if isinstance(result,dict):
                sql_query = """
                    UPDATE mail_attachments
                    SET status = 'SUCCESS', status_update_date = NOW()
                    WHERE id = %s AND sender_email = %s
                """
                cursor.execute(sql_query, (id, sender_mail))
    
                subject  = "Invoice Received Successfully"
                invoice_status = "We acknowledge the invoice. The payment will be released as per the agreed payment terms."
                send_failed_file(subject,invoice_status,sender_mail, pdf_file_path,original_file_name)
                conn.commit()
            elif isinstance(result,str):
                sql_query = """UPDATE mail_attachments
                    SET status = 'FAILED', status_update_date = NOW()
                    WHERE id = %s and sender_email = %s"""

                # status_time = datetime.now()
                cursor.execute(sql_query,(id, sender_mail))
               
                subject = "Reject Invoice"
                invoice_status = "Invoice submitted by you is not submitted. please connect with account department."
                send_failed_file(subject, invoice_status, sender_mail, pdf_file_path,original_file_name)
                conn.commit()
            
            else:
                continue
        cursor.close()
        conn.close()
        return 'ok'

    except Exception as exe:
        logging.error(str(exe))
        return str(exe)

def schedular_function():
    download_files = download_unread_attachments_now(EMAIL, PASSWORD, IMAP_SERVER, DOWNLOAD_FOLDER)
    logging.info('File downloaded.')
    res = process_files()
    logging.info("Process Completed.")
    return res

if __name__ == '__main__':
    schedular_function()
    # while True:
    #     logging.info('Scheduler Start.')
    #     schedular_function()
    #     logging.info('Schedular End.')
    #     time.sleep(2 * 60)