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


def normalize_email(email):
    """Normalise un email. Gere l'astuce Gmail : points et +alias ignores.
    jean.dupont+promo@gmail.com -> jeandupont@gmail.com"""
    if not email:
        return ""
    email = str(email).strip().lower()
    if "@" not in email:
        return email
    local, domain = email.rsplit("@", 1)
    # Retirer tout ce qui suit un +
    if "+" in local:
        local = local.split("+", 1)[0]
    # Pour Gmail/Googlemail : les points sont ignores
    if domain in ("gmail.com", "googlemail.com"):
        local = local.replace(".", "")
        domain = "gmail.com"
    return f"{local}@{domain}"


# Domaines d'emails jetables/temporaires (les plus courants)
DISPOSABLE_DOMAINS = {
    "yopmail.com", "yopmail.fr", "yopmail.net", "temp-mail.org", "tempmail.com",
    "guerrillamail.com", "guerrillamail.info", "guerrillamail.net", "sharklasers.com",
    "10minutemail.com", "10minutemail.net", "mailinator.com", "mailinator.net",
    "throwawaymail.com", "trashmail.com", "trashmail.fr", "getnada.com",
    "maildrop.cc", "mintemail.com", "mohmal.com", "fakeinbox.com",
    "tempmailo.com", "temp-mail.io", "tempmail.plus", "mailnesia.com",
    "dispostable.com", "spamgourmet.com", "jetable.org", "jetable.fr",
    "emailondeck.com", "mytemp.email", "tempr.email", "burnermail.io",
    "moakt.com", "luxusmail.org", "email-fake.com", "fakemail.net",
    "tmpmail.org", "tmpmail.net", "mail-temp.com", "0clock.net",
    "spam4.me", "grr.la", "guerrillamailblock.com", "pokemail.net",
    "tempinbox.com", "tempmailaddress.com", "wegwerfemail.de", "mail.tm",
}


def is_disposable_email(email):
    """Detecte si l'email utilise un domaine jetable/temporaire."""
    if not email or "@" not in email:
        return False
    domain = str(email).strip().lower().rsplit("@", 1)[1]
    return domain in DISPOSABLE_DOMAINS


# Abreviations courantes dans les adresses pour normalisation
ADDR_ABBREV = {
    "AVENUE": "AV", "AV.": "AV", "AVE": "AV",
    "BOULEVARD": "BD", "BLVD": "BD", "BVD": "BD", "BLD": "BD",
    "RUE": "R", "R.": "R",
    "PLACE": "PL", "IMPASSE": "IMP", "ALLEE": "ALL",
    "CHEMIN": "CH", "ROUTE": "RTE", "RESIDENCE": "RES",
    "BATIMENT": "BAT", "BAT.": "BAT", "APPARTEMENT": "APT", "APP": "APT", "APP.": "APT",
    "SAINT": "ST", "SAINTE": "STE",
}


def normalize_adresse(adresse):
    """Normalise une adresse : majuscules, sans accents, abreviations unifiees, sans ponctuation."""
    if not adresse:
        return ""
    text = normalize(adresse)
    # Retirer la ponctuation courante
    for char in [",", ".", "-", "'"]:
        text = text.replace(char, " ")
    text = " ".join(text.split())
    # Unifier les abreviations mot par mot
    words = []
    for word in text.split():
        words.append(ADDR_ABBREV.get(word, word))
    return " ".join(words)

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
    customer = order.get("customer", {}) or {}
    client_details = order.get("client_details", {}) or {}
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
        "ville": (shipping.get("city", "") or "").strip(),
        "ip": order.get("browser_ip", "") or "",
        "customer_id": str(customer.get("id", "") or ""),
        "user_agent": (client_details.get("user_agent", "") or "").strip(),
        "tel_fact": (billing.get("phone", "") or "").replace(" ", ""),
        "nom_fact": (billing.get("last_name", "") or "").strip(),
        "prenom_fact": (billing.get("first_name", "") or "").strip(),
        "adresse_fact": (billing.get("address1", "") or "").strip(),
        "adresse2_fact": (billing.get("address2", "") or "").strip(),
        "code_postal_fact": (billing.get("zip", "") or "").strip(),
        "ville_fact": (billing.get("city", "") or "").strip(),
    }


def lookup_client(worksheet, info):
    """
    Check if client exists in sheet.
    Returns (is_doublon, matched_fields)

    CHAMPS FORTS (1 seul match = DOUBLON) : Email, Tel, IP, Adresse
    COMBINAISONS :
      - Nom ET Prenom ensemble = DOUBLON
      - Code postal + (Nom OU Prenom) = DOUBLON
    L'adresse de facturation est aussi comparee.
    """
    all_values = worksheet.get_all_values()
    if len(all_values) <= 1:
        return False, []

    def norm_tel(t):
        return str(t).strip().replace("+33", "0").replace(" ", "")

    # Valeurs client (livraison + facturation)
    client_email = normalize_email(info["email"])
    client_ip = info.get("ip", "").strip()
    client_cid = info.get("customer_id", "").strip()
    client_tels = {t for t in [norm_tel(info["tel"]), norm_tel(info.get("tel_fact", ""))] if t}
    client_noms = {n for n in [normalize(info["nom"]), normalize(info.get("nom_fact", ""))] if n}
    client_prenoms = {p for p in [normalize(info["prenom"]), normalize(info.get("prenom_fact", ""))] if p}
    client_adresses = {a for a in [
        normalize_adresse(info["adresse"]),
        normalize_adresse(info.get("adresse2", "")),
        normalize_adresse(info.get("adresse_fact", "")),
        normalize_adresse(info.get("adresse2_fact", "")),
    ] if a}
    client_zips = {z for z in [normalize(info["code_postal"]), normalize(info.get("code_postal_fact", ""))] if z}
    client_villes = {v for v in [normalize(info.get("ville", "")), normalize(info.get("ville_fact", ""))] if v}
    client_ua = (info.get("user_agent", "") or "").strip()

    for row in all_values[1:]:
        if len(row) < 9:
            continue

        row_email = normalize_email(row[2])
        row_tel = norm_tel(row[3])
        row_nom = normalize(row[4])
        row_prenom = normalize(row[5])
        row_adresse = normalize_adresse(row[6])
        row_adresse2 = normalize_adresse(row[7])
        row_zip = normalize(row[8])
        row_ip = str(row[10]).strip() if len(row) > 10 else ""
        row_adresse_fact = normalize_adresse(row[11]) if len(row) > 11 else ""
        row_zip_fact = normalize(row[12]) if len(row) > 12 else ""
        row_nom_fact = normalize(row[13]) if len(row) > 13 else ""
        row_prenom_fact = normalize(row[14]) if len(row) > 14 else ""
        row_ville = normalize(row[15]) if len(row) > 15 else ""
        row_ville_fact = normalize(row[16]) if len(row) > 16 else ""
        row_cid = str(row[17]).strip() if len(row) > 17 else ""
        row_ua = str(row[18]).strip() if len(row) > 18 else ""

        row_adresses = {a for a in [row_adresse, row_adresse2, row_adresse_fact] if a}
        row_zips = {z for z in [row_zip, row_zip_fact] if z}
        row_noms = {n for n in [row_nom, row_nom_fact] if n}
        row_prenoms = {p for p in [row_prenom, row_prenom_fact] if p}
        row_villes = {v for v in [row_ville, row_ville_fact] if v}

        # === CHAMPS FORTS : 1 seul match = DOUBLON ===
        if client_email and row_email and client_email == row_email:
            return True, [f"Email: {client_email}"]

        if client_tels and row_tel and row_tel in client_tels:
            return True, [f"Tel: {row_tel}"]

        if client_ip and row_ip and client_ip == row_ip:
            return True, [f"IP: {client_ip}"]

        if client_cid and row_cid and client_cid == row_cid:
            return True, [f"Client ID Shopify: {client_cid}"]

        if client_adresses and row_adresses and client_adresses & row_adresses:
            matched_addr = (client_adresses & row_adresses).pop()
            return True, [f"Adresse: {matched_addr}"]

        # === COMBINAISONS : plusieurs champs ensemble ===
        match_nom = bool(client_noms and row_noms and client_noms & row_noms)
        match_prenom = bool(client_prenoms and row_prenoms and client_prenoms & row_prenoms)
        match_zip = bool(client_zips and row_zips and client_zips & row_zips)
        match_ville = bool(client_villes and row_villes and client_villes & row_villes)

        # Regle 1 : Nom ET Prenom ensemble = DOUBLON
        if match_nom and match_prenom:
            matched_nom = (client_noms & row_noms).pop()
            matched_prenom = (client_prenoms & row_prenoms).pop()
            return True, [f"Nom: {matched_nom}", f"Prenom: {matched_prenom}"]

        # Regle 2 : Code postal + un autre champ (nom OU prenom) = DOUBLON
        if match_zip and (match_nom or match_prenom):
            matched_zip = (client_zips & row_zips).pop()
            fields = [f"ZIP: {matched_zip}"]
            if match_nom:
                fields.append(f"Nom: {(client_noms & row_noms).pop()}")
            if match_prenom:
                fields.append(f"Prenom: {(client_prenoms & row_prenoms).pop()}")
            return True, fields

        # Regle 3 : Meme navigateur (user-agent) + meme ville = suspect
        # (user-agent seul trop commun, mais combine avec la ville c'est un signal)
        match_ua = bool(client_ua and row_ua and client_ua == row_ua)
        match_ville = bool(client_villes and row_villes and client_villes & row_villes)
        if match_ua and match_ville:
            matched_ville = (client_villes & row_villes).pop()
            return True, [f"Navigateur identique", f"Ville: {matched_ville}"]

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
        info.get("ville", ""),
        info.get("ville_fact", ""),
        info.get("customer_id", ""),
        info.get("user_agent", ""),
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
Ville : {info.get('ville', 'N/A')}
IP : {info.get('ip', 'N/A')}
Client ID Shopify : {info.get('customer_id', 'N/A')}

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

    # Email jetable = signal fort (meme si pas de doublon dans le sheet)
    if is_disposable_email(info["email"]):
        print(f"  EMAIL JETABLE detecte: {info['email']}")
        if not is_doublon:
            is_doublon = True
            matched_fields = []
        matched_fields = list(matched_fields) + [f"Email JETABLE: {info['email']}"]

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
