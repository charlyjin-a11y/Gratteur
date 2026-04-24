"""
GRATTEUR - Détecteur de fraude promo -50% Jilypet
Version webhook : Shopify envoie les commandes directement ici.
"""

import os
import json
import hmac
import hashlib
import base64
import requests as http_requests
from datetime import datetime

from flask import Flask, request, jsonify
import gspread
from google.oauth2.service_account import Credentials

app = Flask(__name__)

# Configuration via variables d'environnement
SHOPIFY_WEBHOOK_SECRET = os.environ.get("SHOPIFY_WEBHOOK_SECRET", "")
SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID", "11VW1DxD315CMWMbmqIiLi_TODxJ1Q1BtPAnaeJLxEzU")
WORKSHEET_NAME = os.environ.get("WORKSHEET_NAME", "Feuille 1")
EMAIL_TO = os.environ.get("EMAIL_TO", "hello@jilypet.com")
RESEND_API_KEY = os.environ.get("RESEND_API_KEY", "")
SKU_ABO1 = "LIVRAISONOFFERTE"

# Google Sheets connection
def get_worksheet():
    creds_json = os.environ.get("GOOGLE_CREDENTIALS", "{}")
    creds_dict = json.loads(creds_json)
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    gc = gspread.authorize(creds)
    spreadsheet = gc.open_by_key(SPREADSHEET_ID)
    return spreadsheet.worksheet(WORKSHEET_NAME)


def verify_webhook(data, hmac_header):
    """Verify Shopify webhook signature"""
    if not SHOPIFY_WEBHOOK_SECRET:
        return True
    digest = hmac.new(
        SHOPIFY_WEBHOOK_SECRET.encode("utf-8"),
        data,
        hashlib.sha256
    ).digest()
    computed = base64.b64encode(digest).decode()
    return hmac.compare_digest(computed, hmac_header)


def is_abo1(order):
    """Check if order contains LIVRAISONOFFERTE SKU"""
    for item in order.get("line_items", []):
        if item.get("sku") == SKU_ABO1:
            return True
    return False


def extract_info(order):
    """Extract client info from order"""
    shipping = order.get("shipping_address", {}) or {}
    # order_name: try "name" first, then build from "order_number"
    order_name = order.get("name", "")
    if not order_name:
        order_number = order.get("order_number", "")
        if order_number:
            order_name = f"#{order_number}"
    return {
        "date": order.get("created_at", ""),
        "order_name": order_name,
        "email": order.get("email", "") or "",
        "tel": (shipping.get("phone", "") or "").replace(" ", ""),
        "nom": (shipping.get("last_name", "") or "").upper(),
        "prenom": shipping.get("first_name", "") or "",
        "adresse": (shipping.get("address1", "") or "").upper(),
        "code_postal": shipping.get("zip", "") or "",
    }


def lookup_client(worksheet, info):
    """
    Check if client exists in sheet.
    Returns (is_doublon, matched_fields)
    Requires 2+ field matches for DOUBLON.
    """
    all_values = worksheet.get_all_values()
    if len(all_values) <= 1:
        return False, []

    best_score = 0
    matched_fields = []

    for row in all_values[1:]:
        if len(row) < 8:
            continue

        score = 0
        fields = []
        row_email = str(row[2]).strip().lower()
        row_tel = str(row[3]).strip().replace("+33", "0").replace(" ", "")
        row_nom = str(row[4]).strip().upper()
        row_prenom = str(row[5]).strip().upper()
        row_adresse = str(row[6]).strip().upper()
        row_zip = str(row[7]).strip()

        client_tel = info["tel"].replace("+33", "0").replace(" ", "")
        client_email = info["email"].strip().lower()
        client_prenom = info["prenom"].strip().upper()

        # 1. Email
        if client_email and row_email and client_email == row_email:
            score += 1
            fields.append(f"Email: {client_email}")

        # 2. Code postal
        if info["code_postal"] and row_zip and info["code_postal"] == row_zip:
            score += 1
            fields.append(f"ZIP: {info['code_postal']}")

        # 3. Adresse
        if info["adresse"] and row_adresse and info["adresse"] == row_adresse:
            score += 1
            fields.append(f"Adresse: {info['adresse']}")

        # 4. Nom
        if info["nom"] and row_nom and info["nom"] == row_nom:
            score += 1
            fields.append(f"Nom: {info['nom']}")

        # 5. Prenom
        if client_prenom and row_prenom and client_prenom == row_prenom:
            score += 1
            fields.append(f"Prenom: {client_prenom}")

        # 6. Telephone
        if client_tel and row_tel and client_tel == row_tel:
            score += 1
            fields.append(f"Tel: {client_tel}")

        if score > best_score:
            best_score = score
            matched_fields = fields

    # DOUBLON si 2 champs ou plus matchent
    return best_score >= 2, matched_fields


def add_to_sheet(worksheet, info):
    """Add new client to Google Sheet"""
    try:
        dt = datetime.fromisoformat(info["date"].replace("Z", "+00:00"))
        date_str = dt.strftime("%d/%m/%Y %H:%M")
    except Exception:
        date_str = info["date"]

    row = [
        date_str,
        info["order_name"],
        info["email"],
        info["tel"],
        info["nom"],
        info["prenom"],
        info["adresse"],
        info["code_postal"],
        "NOUVEAU",
    ]
    worksheet.append_row(row, value_input_option="USER_ENTERED")


def send_alert(info, matched_fields):
    """Send email alert for doublon via Resend API"""
    if not RESEND_API_KEY:
        print(f"[ALERTE] DOUBLON detecte mais email non configure: {info['order_name']}")
        return

    subject = "GRATTEUR PROMO DETECTE"
    body = f"""DOUBLON DETECTE

Commande : {info['order_name']}
Client : {info['prenom']} {info['nom']}
Email : {info['email']}
Telephone : {info['tel']}
Adresse : {info['adresse']}
Code postal : {info['code_postal']}

Champs qui ont matche :
{chr(10).join('- ' + f for f in matched_fields)}

Ce client a deja utilise la promo -50%.
Verifiez la commande dans Shopify.
"""

    try:
        response = http_requests.post(
            "https://api.resend.com/emails",
            headers={
                "Authorization": f"Bearer {RESEND_API_KEY}",
                "Content-Type": "application/json",
            },
            json={
                "from": "Gratteur <onboarding@resend.dev>",
                "to": [EMAIL_TO],
                "subject": subject,
                "text": body,
            },
            timeout=10,
        )
        if response.status_code == 200:
            print(f"  Email alerte envoye pour {info['order_name']}")
        else:
            print(f"  [ERREUR] Email Resend: {response.status_code} - {response.text}")
    except Exception as e:
        print(f"  [ERREUR] Email: {e}")


@app.route("/webhook/order-created", methods=["POST"])
def handle_order():
    """Handle Shopify order/create webhook"""
    # Verify webhook
    hmac_header = request.headers.get("X-Shopify-Hmac-Sha256", "")
    if not verify_webhook(request.data, hmac_header):
        return jsonify({"error": "Invalid signature"}), 401

    order = request.json
    order_name = order.get("name", "") or f"#{order.get('order_number', '?')}"
    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Commande recue: {order_name}")
    print(f"  Debug - name: {order.get('name')}, order_number: {order.get('order_number')}")

    # Check if Abo-1
    if not is_abo1(order):
        print(f"  Commande recurrente, ignoree")
        return jsonify({"status": "skipped", "reason": "not abo1"}), 200

    print(f"  Abo-1 detectee!")
    info = extract_info(order)

    # Check for doublon
    try:
        worksheet = get_worksheet()
        is_doublon, matched_fields = lookup_client(worksheet, info)

        if is_doublon:
            print(f"  GRATTEUR! Champs: {matched_fields}")
            send_alert(info, matched_fields)
            return jsonify({"status": "doublon", "matched": matched_fields}), 200
        else:
            print(f"  Nouveau client, ajout au sheet")
            add_to_sheet(worksheet, info)
            return jsonify({"status": "nouveau"}), 200
    except Exception as e:
        print(f"  [ERREUR] {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/health", methods=["GET"])
def health():
    """Health check endpoint"""
    return jsonify({"status": "ok", "service": "gratteur"}), 200


@app.route("/", methods=["GET"])
def home():
    return jsonify({
        "service": "Gratteur Jilypet",
        "status": "running",
        "version": "2.0"
    }), 200


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    print(f"Gratteur demarre sur le port {port}")
    app.run(host="0.0.0.0", port=port)
