"""
GRATTEUR - Détecteur de fraude promo -50% Jilypet
Version webhook : Shopify envoie les commandes directement ici.
"""

import os
import json
import hmac
import hashlib
import base64
import unicodedata
import requests as http_requests
from datetime import datetime

from flask import Flask, request, jsonify
import gspread
from google.oauth2.service_account import Credentials

app = Flask(__name__)


def normalize(text):
    """Normalise un texte pour comparaison : majuscules, sans accents, sans espaces multiples."""
    if not text:
        return ""
    text = str(text).strip().upper()
    # Retirer les accents (é -> E, ô -> O, etc.)
    text = unicodedata.normalize("NFD", text)
    text = "".join(c for c in text if unicodedata.category(c) != "Mn")
    # Reduire les espaces multiples en un seul
    text = " ".join(text.split())
    return text

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
    billing = order.get("billing_address", {}) or {}
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
        "nom": (shipping.get("last_name", "") or "").strip(),
        "prenom": (shipping.get("first_name", "") or "").strip(),
        "adresse": (shipping.get("address1", "") or "").strip(),
        "adresse2": (shipping.get("address2", "") or "").strip(),
        "code_postal": (shipping.get("zip", "") or "").strip(),
        "ip": order.get("browser_ip", "") or "",
        "tel_fact": (billing.get("phone", "") or "").replace(" ", ""),
        "nom_fact": (billing.get("last_name", "") or "").strip(),
        "prenom_fact": (billing.get("first_name", "") or "").strip(),
        "adresse_fact": (billing.get("address1", "") or "").strip(),
        "adresse2_fact": (billing.get("address2", "") or "").strip(),
        "code_postal_fact": (billing.get("zip", "") or "").strip(),
    }


def lookup_client(worksheet, info):
    """
    Check if client exists in sheet.
    Returns (is_doublon, matched_fields)
    Champs FORTS (1 match = DOUBLON) : Email, Tel, IP
    Champs FAIBLES (2 matchs = DOUBLON) : Nom, Prenom, Adresse, Code postal
    L'adresse de facturation est aussi comparee.
    """
    all_values = worksheet.get_all_values()
    if len(all_values) <= 1:
        return False, []

    def norm_tel(t):
        return str(t).strip().replace("+33", "0").replace(" ", "")

    # Valeurs client (livraison + facturation)
    client_email = info["email"].strip().lower()
    client_ip = info.get("ip", "").strip()
    client_tels = {t for t in [norm_tel(info["tel"]), norm_tel(info.get("tel_fact", ""))] if t}
    client_noms = {n for n in [normalize(info["nom"]), normalize(info.get("nom_fact", ""))] if n}
    client_prenoms = {p for p in [normalize(info["prenom"]), normalize(info.get("prenom_fact", ""))] if p}
    client_adresses = {a for a in [
        normalize(info["adresse"]),
        normalize(info.get("adresse2", "")),
        normalize(info.get("adresse_fact", "")),
        normalize(info.get("adresse2_fact", "")),
    ] if a}
    client_zips = {z for z in [normalize(info["code_postal"]), normalize(info.get("code_postal_fact", ""))] if z}

    for row in all_values[1:]:
        if len(row) < 9:
            continue

        row_email = str(row[2]).strip().lower()
        row_tel = norm_tel(row[3])
        row_nom = normalize(row[4])
        row_prenom = normalize(row[5])
        row_adresse = normalize(row[6])
        row_adresse2 = normalize(row[7])
        row_zip = normalize(row[8])
        row_ip = str(row[10]).strip() if len(row) > 10 else ""
        row_adresse_fact = normalize(row[11]) if len(row) > 11 else ""
        row_zip_fact = normalize(row[12]) if len(row) > 12 else ""
        row_nom_fact = normalize(row[13]) if len(row) > 13 else ""
        row_prenom_fact = normalize(row[14]) if len(row) > 14 else ""

        row_adresses = {a for a in [row_adresse, row_adresse2, row_adresse_fact] if a}
        row_zips = {z for z in [row_zip, row_zip_fact] if z}
        row_noms = {n for n in [row_nom, row_nom_fact] if n}
        row_prenoms = {p for p in [row_prenom, row_prenom_fact] if p}

        # === CHAMPS FORTS : 1 match = DOUBLON ===
        if client_email and row_email and client_email == row_email:
            return True, [f"Email: {client_email}"]

        if client_tels and row_tel and row_tel in client_tels:
            return True, [f"Tel: {row_tel}"]

        if client_ip and row_ip and client_ip == row_ip:
            return True, [f"IP: {client_ip}"]

        # === CHAMPS FAIBLES : 2 matchs = DOUBLON ===
        weak_score = 0
        weak_fields = []

        if client_noms and row_noms and client_noms & row_noms:
            matched_nom = (client_noms & row_noms).pop()
            weak_score += 1
            weak_fields.append(f"Nom: {matched_nom}")

        if client_prenoms and row_prenoms and client_prenoms & row_prenoms:
            matched_prenom = (client_prenoms & row_prenoms).pop()
            weak_score += 1
            weak_fields.append(f"Prenom: {matched_prenom}")

        if client_adresses and row_adresses and client_adresses & row_adresses:
            matched_addr = (client_adresses & row_adresses).pop()
            weak_score += 1
            weak_fields.append(f"Adresse: {matched_addr}")

        if client_zips and row_zips and client_zips & row_zips:
            matched_zip = (client_zips & row_zips).pop()
            weak_score += 1
            weak_fields.append(f"ZIP: {matched_zip}")

        if weak_score >= 2:
            return True, weak_fields

    return False, []


def add_to_sheet(worksheet, info, statut="NOUVEAU"):
    """Add client to Google Sheet"""
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
        info.get("adresse2", ""),
        info["code_postal"],
        statut,
        info.get("ip", ""),
        info.get("adresse_fact", ""),
        info.get("code_postal_fact", ""),
        info.get("nom_fact", ""),
        info.get("prenom_fact", ""),
    ]
    worksheet.append_row(row, value_input_option="USER_ENTERED")
    print(f"  -> Ajout au sheet: {info['prenom']} {info['nom']} ({statut})")


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
Adresse livraison : {info['adresse']}
Adresse complementaire : {info.get('adresse2', '')}
Code postal : {info['code_postal']}
Adresse facturation : {info.get('adresse_fact', 'N/A')}
Code postal fact. : {info.get('code_postal_fact', 'N/A')}
Nom facturation : {info.get('prenom_fact', '')} {info.get('nom_fact', '')}
IP : {info.get('ip', 'N/A')}

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
                "from": "Gratteur <gratteur@jilypet.com>",
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
    except Exception as e:
        print(f"  [ERREUR] Lookup: {e}")
        return jsonify({"error": str(e)}), 500

    # TOUJOURS ajouter au sheet (nouveau ou gratteur)
    statut = "DOUBLON" if is_doublon else "NOUVEAU"
    try:
        add_to_sheet(worksheet, info, statut=statut)
    except Exception as e:
        print(f"  [ERREUR] Ajout sheet: {e}")

    # Si gratteur, envoyer l'alerte email
    if is_doublon:
        print(f"  GRATTEUR! Champs: {matched_fields}")
        try:
            send_alert(info, matched_fields)
        except Exception as e:
            print(f"  [ERREUR] Alerte: {e}")
        return jsonify({"status": "doublon", "matched": matched_fields}), 200
    else:
        return jsonify({"status": "nouveau"}), 200


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
