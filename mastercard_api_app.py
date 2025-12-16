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
DB_PASS = os.getenv('MYSQL_PASSWORD', 'AVNS_NXWZq4aSI2jA1w3aEBT')
DB_HOST = os.getenv('MYSQL_HOST', 'mysql-88b5bc0-santhoshkumarbsk1998-43a0.g.aivencloud.com') 
DB_PORT = os.getenv('MYSQL_PORT', '10438') 
DB_NAME = os.getenv('MYSQL_DATABASE', 'mastercard_db')

# Aiven SSL Certificate Authority file path (REQUIRED FOR CONNECTION)
SSL_CA_CERT_PATH = os.getenv('SSL_CA_CERT_PATH', 'ca.pem') 

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
        )
        return conn
    except Error as e:
        print(f"MySQL Database connection failed: {e}")
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


# --- DATABASE LOGGING FUNCTION ---

def log_payment_request(data):
    """Logs the incoming payment request data to the PAYMENT_REQUEST table."""
    request_uuid = str(uuid.uuid4())
    
    conn = get_db_connection()
    if not conn:
        print("CRITICAL: Failed to get DB connection for logging.")
        return False, request_uuid 
        
    try:
        cursor = conn.cursor()
        
        # SQL matches your Schema
        sql = """
        INSERT INTO PAYMENT_REQUEST 
        (REQUEST_ID, REFERENCE, VENDOR_ID, AMOUNT, CURRENCY, STATUS, RECEIVED_AT, LAST_UPDATED, CPI_RESPONSE) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        current_time = datetime.datetime.utcnow()
        
        # Store the entire received payload in CPI_RESPONSE column for trace
        full_payload_json = json.dumps(data) 
        
        values = (
            request_uuid,              # 1. REQUEST_ID
            data["reference"],         # 2. REFERENCE
            data["vendorld"],          # 3. VENDOR_ID
            str(data["amount"]),       # 4. AMOUNT
            data["currency"],          # 5. CURRENCY
            "INITIATED",               # 6. STATUS
            current_time,              # 7. RECEIVED_AT
            current_time,              # 8. LAST_UPDATED 
            full_payload_json          # 9. CPI_RESPONSE (Storing payload here)
        )
        
        cursor.execute(sql, values)
        conn.commit()
        print(f"Logged payment request {request_uuid} to database.")
        return True, request_uuid
        
    except Error as e:
        print(f"Database insertion failed: {e}")
        conn.rollback()
        return False, request_uuid
        
    finally:
        if cursor: cursor.close()
        if conn: conn.close()
# ----------------------------------------


# --- CORE ENDPOINT: Payment Submission (FIXED) ---

@app.post("/mastercard/submit_payment")
def submit_payment_request():
    """
    1. Receives payment request from CPI.
    2. Validates essential data.
    3. Logs request to Aiven DB.
    4. Returns Success to CPI (Ends the process).
    """
    data = request.json
    
    # Define required fields based on the payment file payload from SAP F110
    required_fields = ["vendorld", "invoice", "amount", "currency", "companyCode", "reference"] 
    
    # 1. Check Required Fields
    if not all(field in data for field in required_fields):
        return jsonify({
            "status": "ERROR", 
            "message": "Missing one or more required payment fields."
        }), 400

    # 2. Validation Logic
    try:
        amount = float(data["amount"])
        if amount <= 0:
            return jsonify({"status": "FAILED", "message": "Validation Error: Amount must be positive."}), 400
    except (TypeError, ValueError):
        return jsonify({"status": "FAILED", "message": "Validation Error: Amount field is invalid or missing."}), 400

    # 3. Log the request to Aiven MySQL
    log_success, request_uuid = log_payment_request(data)

    # 4. Return Success Response to CPI
    # --- CRITICAL FIX: Loop Removed. We do NOT call CPI back here. ---
    
    if log_success:
        print(f"Validation successful for ref: {data.get('reference')}. Payment Stored.")
        
        # Simulate a Mastercard VCC Number generation
        mock_vcc_number = "5500" + str(uuid.uuid4().int)[:12] 

        return jsonify({
            "status": "ACCEPTED",
            "reference": data["reference"],
            "request_id": request_uuid,
            "message": "Payment request received and stored successfully.",
            "mastercard_vcc_number": mock_vcc_number,
            "mastercard_status": "PROCESSING"
        }), 202 # Accepted status

    else:
        return jsonify({
            "status": "FAILED",
            "message": "Database Error: Could not store payment request."
        }), 500


# --- INBOUND ENDPOINT: Settlement Confirmation ---

@app.post("/mastercard/receive_settlement_confirmation")
def receive_settlement_confirmation():
    """
    Placeholder for the Inbound flow (Automated Reconciliation).
    """
    data = request.json
    print(f"Received settlement confirmation for Ref: {data.get('reference')}. Status: {data.get('status')}")
    return jsonify({
        "status": "ACKNOWLEDGED", 
        "message": "Settlement confirmation received and successfully queued."
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

@app.get("/transactions/<string:request_id>")
def get_transaction_details(request_id):
    """Retrieves a single transaction log from PAYMENT_REQUEST."""
    conn = get_db_connection()
    if not conn: return jsonify({"message": "DB connection failed"}), 503
    
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute("SELECT REQUEST_ID, REFERENCE, VENDOR_ID, AMOUNT, CURRENCY, STATUS, RECEIVED_AT, LAST_UPDATED, CPI_RESPONSE FROM PAYMENT_REQUEST WHERE REQUEST_ID = %s", (request_id,))
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
