import os
import json
import uuid
import datetime
import requests
from flask import Flask, request, jsonify
import mysql.connector
from mysql.connector import Error

# --- Flask Application Initialization ---
app = Flask(__name__)

# --- Configuration (Set via .env) ---
# Your Aiven MySQL configuration
DB_USER = os.getenv('MYSQL_USER', 'avnadmin') 
DB_PASS = os.getenv('MYSQL_PASSWORD')
DB_HOST = os.getenv('MYSQL_HOST', 'mysql-88b5bc0-santhoshkumarbsk1998-43a0.g.aivencloud.com') 
DB_PORT = os.getenv('MYSQL_PORT', '10438') 
DB_NAME = os.getenv('MYSQL_DATABASE', 'mastercard_db')

# Aiven SSL Certificate Authority file path (REQUIRED FOR CONNECTION)
# You MUST download this from your Aiven Console and ensure the path is correct.
SSL_CA_CERT_PATH = os.getenv('SSL_CA_CERT_PATH', 'ca.pem') 

# SAP CPI Target URL (Endpoint for the MC_PAY_INIT iFlow - TSD Step 3)
CPI_INITIATE_URL = os.getenv('CPI_INITIATE_URL', 'https://mock-cpi-url.com/cpi/mastercard/initiate_payment')


# --- Database Connection Utilities ---

def get_db_connection():
    """Establishes and returns a MySQL database connection with SSL configured."""
    try:
        conn = mysql.connector.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS,
            port=DB_PORT,
            # --- CONNECTION FIX: Aiven requires SSL CA file ---
            ssl_ca=SSL_CA_CERT_PATH, 
            # Temporary workaround for the authentication plugin issue if SSL alone fails:
            # auth_plugin='mysql_native_password' 
        )
        return conn
    except Error as e:
        print(f"MySQL Database connection failed: {e}")
        # Note: If this still fails, double-check your CA file path and firewall rules.
        return None

def init_db():
    """Simple check to ensure database connection is working."""
    conn = get_db_connection()
    if conn: 
        print("Database connection successfully established and checked.")
        conn.close()
    else:
        print("Initial database connection failed. Check SSL config and credentials.")

init_db()

# --- CORE ENDPOINT: Payment Submission and CPI Trigger (Outbound Flow) ---

@app.post("/mastercard/submit_payment")
def submit_payment_request():
    """
    1. Receives payment request from the client/SAP.
    2. Validates essential data (TSD Step 2).
    3. Triggers the SAP CPI iFlow (TSD Step 3).
    """
    data = request.json
    
    # Define required fields based on the payment file payload from SAP F110
    required_fields = ["vendorld", "invoice", "amount", "currency", "companyCode", "reference"] 
    
    if not all(field in data for field in required_fields):
        return jsonify({
            "status": "ERROR", 
            "message": "Missing one or more required payment fields."
        }), 400

    # --- Validation Logic (TSD Step 2) ---
    try:
        amount = float(data["amount"])
        if amount <= 0:
            return jsonify({"status": "FAILED", "message": "Validation Error: Amount must be positive."}), 400
        # Add more complex validation here (e.g., format checks)
    except (TypeError, ValueError):
        return jsonify({"status": "FAILED", "message": "Validation Error: Amount field is invalid or missing."}), 400


    # --- TSD Step 3: Trigger CPI iFlow (HTTP Call) ---
    print(f"Validation successful for ref: {data.get('reference')}. Forwarding to CPI.")
    
    cpi_headers = {
        'Content-Type': 'application/json',
        # ADD SECURITY HEADERS HERE (e.g., Basic Auth or Bearer Token for CPI)
    }
    
    try:
        cpi_response = requests.post(
            CPI_INITIATE_URL, 
            json=data, 
            headers=cpi_headers, 
            verify=False # Set to True in production
        )
        cpi_response.raise_for_status() # Raise exception for 4xx or 5xx status codes

        # TSD Step 6: Return Response
        return jsonify({
            "status": "ACCEPTED",
            "reference": data["reference"],
            "message": "Payment request validated and successfully forwarded to SAP CPI.",
            "cpi_status_code": cpi_response.status_code,
            "cpi_response": cpi_response.json() if cpi_response.content else {"note": "No JSON body returned from CPI"}
        }), 202 # Accepted status

    except requests.exceptions.RequestException as e:
        print(f"CPI Trigger FAILED for ref: {data.get('reference')}. Error: {e}")
        http_code = getattr(e.response, 'status_code', 500) if e.response else 500
        # TSD Step 6: Return Error
        return jsonify({
            "status": "FAILED", 
            "reference": data["reference"],
            "message": "Failed to trigger SAP CPI iFlow. Check CPI configuration.",
            "error_detail": str(e),
            "cpi_http_code": http_code
        }), 502 # Bad Gateway


# --- INBOUND ENDPOINT: Settlement Confirmation (Reconciliation Support) ---

@app.post("/mastercard/receive_settlement_confirmation")
def receive_settlement_confirmation():
    """
    Placeholder for the Inbound flow (Automated Reconciliation / EBRS).
    Receives settlement status/UTR confirmation from Mastercard (via CPI).
    """
    data = request.json
    
    # In a real scenario, you would log this to a custom table
    print(f"Received settlement confirmation for Ref: {data.get('reference')}. Status: {data.get('status')}")
    
    # After logging, this data would typically be pushed to the SAP OData 
    # service dedicated to bank statement processing for EBRS.
    
    return jsonify({
        "status": "ACKNOWLEDGED", 
        "message": "Settlement confirmation received and successfully queued for SAP reconciliation."
    }), 200


# --- MANAGEMENT/CRUD ENDPOINTS (For Testing/Monitoring) ---

@app.get("/health")
def health_check():
    """Simple health check endpoint."""
    conn = get_db_connection()
    if conn:
        conn.close()
        db_status = "Online"
        db_code = 0
    else:
        db_status = "Offline"
        db_code = 1

    return jsonify({
        "status": "OK",
        "service": "Mastercard API Wrapper (Britannia)",
        "db_status": db_status,
        "db_code": db_code
    }), 200

@app.get("/accounts/<string:acc_type>/<string:acc_no>")
def get_account_details(acc_type, acc_no):
    """Retrieves account details for CUSTOMER or VENDOR accounts (MOCK)."""
    if acc_type not in ['customer', 'vendor']:
        return jsonify({"message": "Invalid account type"}), 400
    
    table_name = "CUSTOMER_ACCOUNT" if acc_type == 'customer' else "VENDOR_ACCOUNT"
    conn = get_db_connection()
    if not conn: return jsonify({"message": "DB connection failed"}), 503
    
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute(f"SELECT * FROM {table_name} WHERE ACC_NO = %s", (acc_no,))
        account = cur.fetchone()
        
        if account:
            return jsonify(account), 200
        else:
            return jsonify({"message": f"{acc_type.capitalize()} account not found"}), 404
    except Error as e:
        return jsonify({"message": str(e)}), 500
    finally:
        if cur: cur.close()
        if conn: conn.close()

@app.get("/transactions/<string:request_id>")
def get_transaction_details(request_id):
    """
    Retrieves a single transaction log from PAYMENT_REQUEST (MOCK log table).
    NOTE: Using new fields REFERENCE and VENDOR_ID.
    """
    conn = get_db_connection()
    if not conn: return jsonify({"message": "DB connection failed"}), 503
    
    try:
        cur = conn.cursor(dictionary=True)
        # Select fields that align with the new schema logic
        cur.execute("SELECT REQUEST_ID, REFERENCE, VENDOR_ID, AMOUNT, CURRENCY, STATUS, RECEIVED_AT, LAST_UPDATED FROM PAYMENT_REQUEST WHERE REQUEST_ID = %s", (request_id,))
        transaction = cur.fetchone()
        
        if transaction:
            return jsonify(transaction), 200
        else:
            return jsonify({"message": "Transaction request not found"}), 404
    except Error as e:
        return jsonify({"message": str(e)}), 500
    finally:
        if cur: cur.close()
        if conn: conn.close()


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=os.environ.get('PORT', 5000), debug=True)