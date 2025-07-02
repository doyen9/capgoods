import tkinter as tk
from tkinter import ttk, messagebox, filedialog
import sqlite3
from datetime import datetime, timedelta
import os
from PIL import Image, ImageTk
import pandas as pd
from reportlab.lib.pagesizes import letter, A4
from reportlab.pdfgen import canvas as pdf_canvas
from reportlab.lib import colors
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Spacer, Paragraph
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
import shutil
import json
import re
import sys

def resource_path(relative_path):
    try:
        base_path = sys._MEIPASS
    except Exception:
        base_path = os.path.abspath(".")
    return os.path.join(base_path, relative_path)

import customtkinter as ctk

COLORS = {
    "primary_blue": "#2A4D69",
    "secondary_blue": "#3E7CB1",
    "tertiary_blue": "#6B8EAD",
    
    "background_light": "#E9F1F8",
    "card_background": "#FFFFFF",
    "text_dark": "#263238",
    "text_light": "#78909C",
    "accent_success": "#66BB6A",
    "accent_warning": "#FFA726",
    "accent_error": "#C62828",
    "border_subtle": "#DCE2E9",
    "hover_light": "#F0F5F9",
    "zebra_stripe": "#F9FBFD",
}

FONTS = {
    "title": ("Roboto", 36, "bold"),
    "heading_main": ("Roboto", 26, "bold"),
    "heading_card": ("Roboto", 18, "bold"),
    "subheading": ("Roboto", 14, "bold"),
    "body": ("Roboto", 13),
    "small_body": ("Roboto", 10),
}

CORNER_RADIUS = 10 

ctk.set_appearance_mode("light")
ctk.set_default_color_theme("blue")

DB_NAME = 'cg_management.db' 
BACKUP_DIR = 'cg_backups' 
BACKUP_META_FILE = os.path.join(BACKUP_DIR, 'cg_backup_meta.json')
RETENTION_DAYS = 30

DELETE_ALL_PASSWORD = "Infinity@1254"

def get_db_connection():
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    return conn

def create_new_tables():
    conn = None
    try:
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            password TEXT NOT NULL,
            role TEXT NOT NULL DEFAULT 'user',
            created_at TEXT NOT NULL
        )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS Categories (
                category_id INTEGER PRIMARY KEY AUTOINCREMENT,
                category_name TEXT NOT NULL UNIQUE
            );
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS Employees (
                employee_id INTEGER PRIMARY KEY AUTOINCREMENT,
                employee_name TEXT NOT NULL UNIQUE
            );
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS CapitalGoods (
                cg_id INTEGER PRIMARY KEY AUTOINCREMENT,
                cg_code TEXT UNIQUE, 
                cg_name TEXT NOT NULL,
                description TEXT,
                current_status TEXT NOT NULL DEFAULT 'Available',
                acquisition_date DATETIME DEFAULT CURRENT_TIMESTAMP,
                category_id INTEGER,
                FOREIGN KEY (category_id) REFERENCES Categories(category_id)
            );
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS CGTransactions (
                transaction_id INTEGER PRIMARY KEY AUTOINCREMENT,
                cg_id INTEGER NOT NULL,
                employee_id INTEGER, 
                transaction_type TEXT NOT NULL,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                condition_notes TEXT,
                logged_by_user_id INTEGER,
                FOREIGN KEY (cg_id) REFERENCES CapitalGoods(cg_id),
                FOREIGN KEY (employee_id) REFERENCES Employees(employee_id),
                FOREIGN KEY (logged_by_user_id) REFERENCES users(id)
            );
        ''')

        cursor.execute('''
        CREATE TABLE IF NOT EXISTS activity_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            action TEXT NOT NULL,
            details TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            FOREIGN KEY (user_id) REFERENCES users (id)
        )
        ''')
        
        cursor.execute("SELECT COUNT(*) FROM users WHERE username='admin'")
        if cursor.fetchone()[0] == 0:
            cursor.execute(
                "INSERT INTO users (username, password, role, created_at) VALUES (?, ?, ?, ?)",
                ('admin', 'admin123', 'admin', datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            )
        
        conn.commit()
        print("Database tables created/checked successfully.")
    except Exception as e:
        messagebox.showerror("Database Error", f"Failed to create database tables: {e}")
        exit()
    finally:
        if conn:
            conn.close()

class DatabaseManager:
    def __init__(self):
        self.db_name = DB_NAME
        self.current_user = None
        self._setup_backup_directory()
        self._cleanup_old_backups()

    def log_activity(self, action, details):
        if not self.current_user or not self.current_user.get("id"):
            return
        
        user_id = self.current_user["id"]
        try:
            conn = sqlite3.connect(self.db_name)
            cursor = conn.cursor()
            
            cursor.execute(
                """
                INSERT INTO activity_log (
                    user_id, action, details, timestamp
                ) VALUES (?, ?, ?, ?)
                """,
                (user_id, action, details, datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            )
            conn.commit()
        except Exception as e:
            print(f"Error logging activity: {e}")
        finally:
            if 'conn' in locals():
                conn.close()

    def _setup_backup_directory(self):
        try:
            if not os.path.exists(BACKUP_DIR):
                os.makedirs(BACKUP_DIR)
        except Exception as e:
            messagebox.showerror("Backup Error", f"Failed to create backup directory: {e}")

    def _load_backup_meta(self):
        meta = {"last_morning_backup": None, "last_afternoon_backup": None}
        try:
            if os.path.exists(BACKUP_META_FILE):
                with open(BACKUP_META_FILE, 'r') as f:
                    loaded_meta = json.load(f)
                    if loaded_meta.get("last_morning_backup"):
                        meta["last_morning_backup"] = datetime.strptime(loaded_meta["last_morning_backup"], "%Y-%m-%d")
                    if loaded_meta.get("last_afternoon_backup"):
                        meta["last_afternoon_backup"] = datetime.strptime(loaded_meta["last_afternoon_backup"], "%Y-%m-%d %H:%M:%S")
        except Exception as e:
            print(f"Warning: Could not load backup metadata, resetting. Error: {e}")
            meta = {"last_morning_backup": None, "last_afternoon_backup": None}
        return meta

    def _save_backup_meta(self, meta):
        try:
            saveable_meta = {
                "last_morning_backup": meta["last_morning_backup"].strftime("%Y-%m-%d") if meta["last_morning_backup"] else None,
                "last_afternoon_backup": meta["last_afternoon_backup"].strftime("%Y-%m-%d %H:%M:%S") if meta["last_afternoon_backup"] else None
            }
            with open(BACKUP_META_FILE, 'w') as f:
                json.dump(saveable_meta, f, indent=4)
        except Exception as e:
            messagebox.showerror("Backup Error", f"Failed to save backup metadata: {e}")

    def _perform_backup(self, backup_type="manual"):
        current_time = datetime.now()
        timestamp_str = current_time.strftime("%Y%m%d_%H%M%S")
        backup_filename = f"cg_management_{timestamp_str}.db"
        backup_path = os.path.join(BACKUP_DIR, backup_filename)

        try:
            shutil.copy2(self.db_name, backup_path)
            messagebox.showinfo("Backup Success", f"Database backup created successfully: {backup_filename}", icon="info")
            self.log_activity(
                "backup_created",
                f"Automated '{backup_type}' backup created: {backup_filename}"
            )
            return True
        except Exception as e:
            messagebox.showerror("Backup Error", f"Failed to create database backup: {e}", icon="error")
            return False

    def _cleanup_old_backups(self):
        cutoff_date = datetime.now() - timedelta(days=RETENTION_DAYS)
        deleted_count = 0
        try:
            if not os.path.exists(BACKUP_DIR):
                os.makedirs(BACKUP_DIR)
                return 

            for filename in os.listdir(BACKUP_DIR):
                if filename.startswith("cg_management_") and filename.endswith(".db"):
                    try:
                        date_part = filename.split('_')[1] 
                        file_date = datetime.strptime(date_part, "%Y%m%d")
                        if file_date < cutoff_date:
                            os.remove(os.path.join(BACKUP_DIR, filename))
                            deleted_count += 1
                    except ValueError:
                        pass
            if deleted_count > 0:
                print(f"Cleaned up {deleted_count} old backup files.")
        except Exception as e:
            print(f"Backup Cleanup Error: Failed to clean up old backups: {e}")
            messagebox.showerror("Backup Cleanup Error", f"Failed to clean up old backups: {e}", icon="error")

    def check_and_perform_daily_backups(self):
        self._setup_backup_directory() 
        meta = self._load_backup_meta()
        current_datetime = datetime.now()
        current_date = current_datetime.date() 
        
        if not meta["last_morning_backup"] or meta["last_morning_backup"].date() < current_date:
            print("Performing morning backup...")
            if self._perform_backup(backup_type="morning"):
                meta["last_morning_backup"] = current_datetime
                self._save_backup_meta(meta)

        if (current_datetime.hour >= 15 and current_datetime.hour < 16) and \
           (not meta["last_afternoon_backup"] or meta["last_afternoon_backup"].date() < current_date):
            print("Performing afternoon backup...")
            if self._perform_backup(backup_type="afternoon"):
                meta["last_afternoon_backup"] = current_datetime
                self._save_backup_meta(meta)

    def export_full_database_to_excel(self):
        file_path = filedialog.asksaveasfilename(
            defaultextension=".xlsx",
            filetypes=[("Excel files", "*.xlsx")]
        )
        if not file_path:
            return

        conn = None
        try:
            conn = sqlite3.connect(self.db_name)
            
            with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
                df_cgs = pd.read_sql_query("SELECT * FROM CapitalGoods", conn)
                df_cgs.to_excel(writer, sheet_name='CapitalGoods', index=False)

                df_categories = pd.read_sql_query("SELECT * FROM Categories", conn)
                df_categories.to_excel(writer, sheet_name='Categories', index=False)

                df_employees = pd.read_sql_query("SELECT * FROM Employees", conn)
                df_employees.to_excel(writer, sheet_name='Employees', index=False)

                df_transactions = pd.read_sql_query("SELECT * FROM CGTransactions", conn)
                df_transactions.to_excel(writer, sheet_name='CGTransactions', index=False)

                df_users = pd.read_sql_query("SELECT id, username, role, created_at FROM users", conn)
                df_users.to_excel(writer, sheet_name='Users', index=False)

                df_activity_log = pd.read_sql_query("SELECT * FROM activity_log", conn)
                df_activity_log.to_excel(writer, sheet_name='ActivityLog', index=False)

            messagebox.showinfo("Export Success", f"All database data exported to Excel:\n{file_path}", icon="info")
            self.log_activity("database_exported", f"Full database exported to Excel: {file_path}")

        except Exception as e:
            messagebox.showerror("Export Error", f"Failed to export full database: {e}", icon="error")
        finally:
            if conn:
                conn.close()        

def add_cg_db(cg_code, cg_name, description="", category_id=None, user_id=None):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cg_code_to_store = cg_code if cg_code else None 

        cursor.execute("INSERT INTO CapitalGoods (cg_code, cg_name, description, current_status, category_id) VALUES (?, ?, ?, ?, ?)",
                       (cg_code_to_store, cg_name, description, 'Available', category_id))
        cg_id = cursor.lastrowid
        cursor.execute("INSERT INTO CGTransactions (cg_id, transaction_type, condition_notes, logged_by_user_id) VALUES (?, ?, ?, ?)",
                       (cg_id, 'Acquisition', 'New C.G. acquired', user_id))
        conn.commit()
        return True, f"C.G. '{cg_code_to_store if cg_code_to_store else 'N/A'}' ({cg_name}) registered successfully."
    except sqlite3.IntegrityError:
        return False, f"Error: C.G. code '{cg_code}' already exists. Please use a unique code or leave it blank."
    except Exception as e:
        return False, f"Error registering C.G.: {e}"
    finally:
        conn.close()

def update_cg_db(cg_id, cg_code, cg_name, description, category_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cg_code_to_store = cg_code if cg_code else None 

        if cg_code_to_store:
            cursor.execute("SELECT cg_id FROM CapitalGoods WHERE cg_code = ? AND cg_id != ?", (cg_code_to_store, cg_id))
            if cursor.fetchone():
                return False, f"Error: C.G. code '{cg_code}' already exists for another item. Please use a unique code."

        cursor.execute(
            "UPDATE CapitalGoods SET cg_code = ?, cg_name = ?, description = ?, category_id = ? WHERE cg_id = ?",
            (cg_code_to_store, cg_name, description, category_id, cg_id)
        )
        conn.commit()
        return True, f"C.G. '{cg_code_to_store if cg_code_to_store else 'N/A'}' ({cg_name}) updated successfully."
    except Exception as e:
        conn.rollback()
        return False, f"Error updating C.G.: {e}"
    finally:
        conn.close()

def issue_cg_db(cg_id, employee_id, user_id=None):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT current_status, cg_code FROM CapitalGoods WHERE cg_id = ?", (cg_id,))
        cg_info = cursor.fetchone()
        if not cg_info:
            return False, "Error: C.G. not found."

        if cg_info['current_status'] != 'Available':
            return False, f"Error: C.G. '{cg_info['cg_code'] if cg_info['cg_code'] else 'N/A'}' is not available for issue. Current status: {cg_info['current_status']}."

        cursor.execute("UPDATE CapitalGoods SET current_status = ? WHERE cg_id = ?", ('Issued', cg_id))
        cursor.execute("INSERT INTO CGTransactions (cg_id, employee_id, transaction_type, condition_notes, logged_by_user_id) VALUES (?, ?, ?, ?, ?)",
                       (cg_id, employee_id, 'Issue', 'Issued to employee', user_id))
        conn.commit()
        
        employee_name = get_employee_by_id_db(employee_id)['employee_name']
        return True, f"C.G. '{cg_info['cg_code'] if cg_info['cg_code'] else 'N/A'}' issued successfully to '{employee_name}'."
    except Exception as e:
        conn.rollback()
        return False, f"Error issuing C.G.: {e}"
    finally:
        conn.close()

def return_cg_db(cg_id, employee_id, condition_notes, user_id=None):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT current_status, cg_code FROM CapitalGoods WHERE cg_id = ?", (cg_id,))
        cg_info = cursor.fetchone()
        if not cg_info:
            return False, "Error: C.G. not found."

        if cg_info['current_status'] != 'Issued':
            return False, f"Error: C.G. '{cg_info['cg_code'] if cg_info['cg_code'] else 'N/A'}' is not currently issued. Current status: {cg_info['current_status']}."

        cursor.execute("UPDATE CapitalGoods SET current_status = ? WHERE cg_id = ?", ('Available', cg_id))
        cursor.execute("INSERT INTO CGTransactions (cg_id, employee_id, transaction_type, condition_notes, logged_by_user_id) VALUES (?, ?, ?, ?, ?)",
                       (cg_id, employee_id, 'Return', condition_notes, user_id))
        conn.commit()
        
        employee_name = get_employee_by_id_db(employee_id)['employee_name']
        return True, f"C.G. '{cg_info['cg_code'] if cg_info['cg_code'] else 'N/A'}' returned successfully by '{employee_name}'."
    except Exception as e:
        conn.rollback()
        return False, f"Error returning C.G.: {e}"
    finally:
        conn.close()

def delete_cg_db(cg_id, cg_code):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cg_status = cursor.execute("SELECT current_status FROM CapitalGoods WHERE cg_id = ?", (cg_id,)).fetchone()
        
        if not cg_status:
            return False, "Error: Capital Good not found."
        
        if cg_status['current_status'] == 'Issued':
            return False, f"Error: C.G. '{cg_code if cg_code else 'N/A'}' is currently 'Issued' and cannot be deleted."

        cursor.execute("DELETE FROM CGTransactions WHERE cg_id = ?", (cg_id,))
        
        cursor.execute("DELETE FROM CapitalGoods WHERE cg_id = ?", (cg_id,))
        
        conn.commit()
        return True, f"C.G. '{cg_code if cg_code else 'N/A'}' deleted successfully along with its transactions."
    except Exception as e:
        conn.rollback()
        return False, f"Error deleting C.G.: {e}"
    finally:
        conn.close()

def get_all_cgs_db(search_term="", category_id=None, status=None): 
    conn = get_db_connection()
    cursor = conn.cursor()
    query = '''
        SELECT 
            CG.*, 
            C.category_name 
        FROM CapitalGoods AS CG
        LEFT JOIN Categories AS C ON CG.category_id = C.category_id
        WHERE 1=1 
    '''
    params = []

    if search_term:
        query += " AND (CG.cg_code LIKE ? OR CG.cg_name LIKE ?)"
        params.append(f"%{search_term}%")
        params.append(f"%{search_term}%")
    
    if category_id is not None:
        query += " AND CG.category_id = ?"
        params.append(category_id)

    if status: 
        query += " AND CG.current_status = ?"
        params.append(status)
    
    query += " ORDER BY CG.cg_name, CG.cg_code"
    
    cgs = cursor.execute(query, params).fetchall()
    conn.close()
    return [dict(cg) for cg in cgs]

def get_cg_by_id_db(cg_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    query = '''
        SELECT 
            CG.*, 
            C.category_name 
        FROM CapitalGoods AS CG
        LEFT JOIN Categories AS C ON CG.category_id = C.category_id
        WHERE CG.cg_id = ?
    '''
    cg = cursor.execute(query, (cg_id,)).fetchone()
    conn.close()
    return dict(cg) if cg else None

def get_all_categories_db():
    conn = get_db_connection()
    categories = conn.execute('SELECT category_id, category_name FROM Categories ORDER BY category_name').fetchall()
    conn.close()
    return [dict(category) for category in categories]

def add_category_db(category_name):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("INSERT INTO Categories (category_name) VALUES (?)", (category_name,))
        conn.commit()
        return True, f"Category '{category_name}' added successfully."
    except sqlite3.IntegrityError:
        return False, f"Error: Category '{category_name}' already exists."
    except Exception as e:
        return False, f"Error adding category: {e}"
    finally:
        conn.close()

def delete_category_db(category_id, category_name):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT category_id FROM Categories WHERE category_name = 'Unassigned Category'")
        unassigned_category = cursor.fetchone()
        if unassigned_category:
            unassigned_category_id = unassigned_category['category_id']
        else:
            cursor.execute("INSERT INTO Categories (category_name) VALUES ('Unassigned Category')")
            unassigned_category_id = cursor.lastrowid
            conn.commit()

        cursor.execute("UPDATE CapitalGoods SET category_id = ? WHERE category_id = ?",
                       (unassigned_category_id, category_id))
        
        cursor.execute("DELETE FROM Categories WHERE category_id = ?", (category_id,))
        
        conn.commit()
        return True, f"Category '{category_name}' deleted successfully. Associated C.G.s moved to 'Unassigned Category'."
    except Exception as e:
        conn.rollback()
        return False, f"Error deleting category: {e}"
    finally:
        conn.close()

def add_employee_db(employee_name):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("INSERT INTO Employees (employee_name) VALUES (?)", (employee_name,))
        conn.commit()
        return True, f"Employee '{employee_name}' added successfully."
    except sqlite3.IntegrityError:
        return False, f"Error: Employee '{employee_name}' already exists."
    except Exception as e:
        return False, f"Error adding employee: {e}"
    finally:
        conn.close()

def get_all_employees_db(search_term=""):
    conn = get_db_connection()
    cursor = conn.cursor()
    query = 'SELECT employee_id, employee_name FROM Employees WHERE 1=1 '
    params = []
    if search_term:
        query += " AND employee_name LIKE ?"
        params.append(f"%{search_term}%")
    query += ' ORDER BY employee_name'
    employees = cursor.execute(query, params).fetchall()
    conn.close()
    return [dict(emp) for emp in employees]

def get_employee_by_id_db(employee_id):
    conn = get_db_connection()
    employee = conn.execute('SELECT employee_id, employee_name FROM Employees WHERE employee_id = ?', (employee_id,)).fetchone()
    conn.close()
    return dict(employee) if employee else None

def delete_employee_db(employee_id, employee_name):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        issued_cgs = get_cgs_issued_to_employee_db(employee_id)
        
        for cg in issued_cgs:
            cg_id = cg['cg_id']
            cursor.execute("UPDATE CapitalGoods SET current_status = 'Available' WHERE cg_id = ?", (cg_id,))
            
            logged_by_user_id = 1
            cursor.execute("INSERT INTO CGTransactions (cg_id, employee_id, transaction_type, condition_notes, logged_by_user_id) VALUES (?, ?, ?, ?, ?)",
                           (cg_id, employee_id, 'Return', f'Auto-returned due to employee deletion: {employee_name}', logged_by_user_id))
        
        cursor.execute("UPDATE CGTransactions SET employee_id = NULL WHERE employee_id = ?", (employee_id,))
        
        cursor.execute("DELETE FROM Employees WHERE employee_id = ?", (employee_id,))
        
        conn.commit()
        return True, f"Employee '{employee_name}' deleted successfully. Any issued C.G.s have been returned."
    except Exception as e:
        conn.rollback()
        return False, f"Error deleting employee: {e}"
    finally:
        conn.close()

def get_current_cg_allocations_db(employee_id_filter=None, category_id_filter=None):
    conn = get_db_connection()
    query = '''
        SELECT
            CG.cg_code,
            CG.cg_name,
            C.category_name,
            E.employee_name,
            CGT.timestamp AS issue_timestamp
        FROM
            CapitalGoods AS CG
        JOIN
            CGTransactions AS CGT ON CG.cg_id = CGT.cg_id
        LEFT JOIN
            Categories AS C ON CG.category_id = C.category_id
        LEFT JOIN
            Employees AS E ON CGT.employee_id = E.employee_id
        WHERE
            CG.current_status = 'Issued' AND CGT.transaction_type = 'Issue'
            AND CGT.transaction_id = (
                SELECT MAX(transaction_id)
                FROM CGTransactions
                WHERE cg_id = CG.cg_id AND transaction_type = 'Issue'
            )
    '''
    params = []

    if employee_id_filter is not None:
        query += " AND CGT.employee_id = ?"
        params.append(employee_id_filter)
    
    if category_id_filter is not None:
        query += " AND CG.category_id = ?"
        params.append(category_id_filter)

    query += " ORDER BY E.employee_name, CG.cg_name;"
    
    allocations = conn.execute(query, params).fetchall()
    conn.close()
    return [dict(row) for row in allocations]


def get_cg_transaction_log_db(start_date=None, end_date=None):
    conn = get_db_connection()
    query = '''
        SELECT
            CGT.timestamp,
            CG.cg_code,
            CG.cg_name,
            E.employee_name,
            CGT.transaction_type,
            CGT.condition_notes,
            U.username AS logged_by_username
        FROM
            CGTransactions AS CGT
        JOIN
            CapitalGoods AS CG ON CGT.cg_id = CG.cg_id
        LEFT JOIN
            users AS U ON CGT.logged_by_user_id = U.id
        LEFT JOIN
            Employees AS E ON CGT.employee_id = E.employee_id
        WHERE 1=1
    '''
    params = []

    if start_date:
        query += " AND CGT.timestamp >= ?"
        params.append(start_date.strftime("%Y-%m-%d %H:%M:%S"))
    if end_date:
        query += " AND CGT.timestamp <= ?"
        params.append(end_date.strftime("%Y-%m-%d %H:%M:%S"))

    query += " ORDER BY CGT.timestamp DESC;"
    
    log_entries = conn.execute(query, params).fetchall()
    conn.close()
    return [dict(row) for row in log_entries]

def get_all_issued_employee_ids_and_names_db():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT employee_id, employee_name FROM Employees ORDER BY employee_name;")
    employees = cursor.fetchall()
    conn.close()
    return [(row['employee_id'], row['employee_name']) for row in employees]

def get_cgs_issued_to_employee_db(employee_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    query = '''
        SELECT
            CG.cg_id,
            CG.cg_code,
            CG.cg_name,
            C.category_name,
            CG.description
        FROM
            CapitalGoods AS CG
        LEFT JOIN
            Categories AS C ON CG.category_id = C.category_id
        WHERE
            CG.current_status = 'Issued'
            AND CG.cg_id IN (
                SELECT cg_id FROM CGTransactions
                WHERE transaction_type = 'Issue' AND employee_id = ?
                AND transaction_id = (
                    SELECT MAX(transaction_id)
                    FROM CGTransactions
                    WHERE cg_id = CG.cg_id AND transaction_type = 'Issue'
                )
            )
        ORDER BY CG.cg_name, CG.cg_code;
    '''
    cgs = cursor.execute(query, (employee_id,)).fetchall()
    conn.close()
    return [dict(cg) for cg in cgs]

def get_last_issued_employee_id_for_cg(cg_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT employee_id FROM CGTransactions WHERE cg_id = ? AND transaction_type = 'Issue' ORDER BY timestamp DESC LIMIT 1;",
        (cg_id,)
    )
    result = cursor.fetchone()
    conn.close()
    return result['employee_id'] if result else None

def update_user_credentials_db(user_id, new_username, new_password):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT id FROM users WHERE username = ? AND id != ?", (new_username, user_id))
        if cursor.fetchone():
            return False, f"Error: Username '{new_username}' already exists. Please choose a different username."

        cursor.execute(
            "UPDATE users SET username = ?, password = ? WHERE id = ?",
            (new_username, new_password, user_id)
        )
        conn.commit()
        return True, f"User '{new_username}' updated successfully."
    except Exception as e:
        conn.rollback()
        return False, f"Error updating user: {e}"
    finally:
        conn.close()

class CustomDialog(ctk.CTkToplevel):
    def __init__(self, parent, title, message, dialog_type="info", on_yes=None):
        super().__init__(parent)
        self.title(title)
        self.transient(parent)  
        self.grab_set()  
        self.resizable(False, False)

        parent_x = parent.winfo_x()
        parent_y = parent.winfo_y()
        parent_width = parent.winfo_width()
        parent_height = parent.winfo_height()

        self.update_idletasks() 
        dialog_width = 380
        dialog_height = 180
        self.geometry(f"{dialog_width}x{dialog_height}+{parent_x + (parent_width - dialog_width) // 2}+{parent_y + (parent_height - dialog_height) // 2}")

        self.configure(fg_color=COLORS["card_background"], corner_radius=CORNER_RADIUS)

        frame = ctk.CTkFrame(self, fg_color="transparent")
        frame.pack(expand=True, fill="both", padx=20, pady=20)

        message_label = ctk.CTkLabel(
            frame,
            text=message,
            font=FONTS["subheading"],
            text_color=COLORS["text_dark"],
            wraplength=dialog_width - 40,
            justify="center"
        )
        message_label.pack(pady=(10, 20))

        button_frame = ctk.CTkFrame(frame, fg_color="transparent")
        button_frame.pack(pady=(0, 10))

        if dialog_type == "confirm":
            yes_btn = ctk.CTkButton(
                button_frame,
                text="Yes, Delete",
                command=lambda: [self.destroy(), on_yes()],
                width=120,
                height=40,
                font=FONTS["subheading"],
                text_color=COLORS["card_background"],
                corner_radius=CORNER_RADIUS,
                fg_color=COLORS["accent_error"],
                hover_color=COLORS["text_dark"] 
            )
            yes_btn.pack(side="left", padx=(0, 15))

            no_btn = ctk.CTkButton(
                button_frame,
                text="Cancel",
                command=self.destroy,
                width=120,
                height=40,
                font=FONTS["subheading"],
                text_color=COLORS["card_background"],
                corner_radius=CORNER_RADIUS,
                fg_color=COLORS["primary_blue"],
                hover_color=COLORS["secondary_blue"]
            )
            no_btn.pack(side="left") 

        elif dialog_type == "info":
            ok_btn = ctk.CTkButton(
                button_frame,
                text="OK",
                command=self.destroy,
                width=120,
                height=40,
                font=FONTS["subheading"],
                text_color=COLORS["card_background"],
                corner_radius=CORNER_RADIUS,
                fg_color=COLORS["primary_blue"],
                hover_color=COLORS["secondary_blue"]
            )
            ok_btn.pack(pady=10) 

        self.wait_window(self) 

class IssueReturnCGDialog(ctk.CTkToplevel):
    def __init__(self, parent, cg_id, cg_code, cg_name, current_status, db_manager, is_issue_action=True, initial_employee_id=None):
        super().__init__(parent)
        self.parent = parent
        self.cg_id = cg_id
        self.cg_code = cg_code
        self.cg_name = cg_name
        self.current_status = current_status
        self.db_manager = db_manager
        self.is_issue_action = is_issue_action
        self.initial_employee_id = initial_employee_id

        self.employee_data = {}
        self.filtered_employee_names = []

        action_type = "Issue" if is_issue_action else "Return"
        self.title(f"{action_type} C.G.: {self.cg_code if self.cg_code else 'N/A'} - {self.cg_name}")
        self.transient(parent)
        self.grab_set()
        self.resizable(False, False)

        parent_x = parent.winfo_x()
        parent_y = parent.winfo_y()
        parent_width = parent.winfo_width()
        parent_height = parent.winfo_height()

        self.update_idletasks()
        dialog_width = 450
        dialog_height = 300 if is_issue_action else 380
        self.geometry(f"{dialog_width}x{dialog_height}+{parent_x + (parent_width - dialog_width) // 2}+{parent_y + (parent_height - dialog_height) // 2}")

        self.configure(fg_color=COLORS["card_background"], corner_radius=CORNER_RADIUS)

        main_frame = ctk.CTkFrame(self, fg_color="transparent")
        main_frame.pack(expand=True, fill="both", padx=20, pady=20)
        main_frame.grid_columnconfigure(1, weight=1)

        ctk.CTkLabel(main_frame, text=f"C.G. Code:", font=FONTS["body"], text_color=COLORS["text_dark"]).grid(row=0, column=0, padx=10, pady=5, sticky="w")
        ctk.CTkLabel(main_frame, text=self.cg_code if self.cg_code else "N/A", font=FONTS["body"], text_color=COLORS["text_dark"]).grid(row=0, column=1, padx=10, pady=5, sticky="w")

        ctk.CTkLabel(main_frame, text=f"C.G. Name:", font=FONTS["body"], text_color=COLORS["text_dark"]).grid(row=1, column=0, padx=10, pady=5, sticky="w")
        ctk.CTkLabel(main_frame, text=self.cg_name, font=FONTS["body"], text_color=COLORS["text_dark"]).grid(row=1, column=1, padx=10, pady=5, sticky="w")
        
        ctk.CTkLabel(main_frame, text=f"{'Issue To' if is_issue_action else 'Returned By'} Employee:", font=FONTS["body"], text_color=COLORS["text_dark"]).grid(row=2, column=0, padx=10, pady=5, sticky="w")
        
        self.employee_combobox = ctk.CTkComboBox(
            main_frame,
            state="readonly",
            width=250,
            height=40,
            font=FONTS["body"],
            fg_color=COLORS["background_light"],
            text_color=COLORS["text_dark"],
            dropdown_fg_color=COLORS["card_background"],
            dropdown_text_color=COLORS["text_dark"],
            dropdown_hover_color=COLORS["hover_light"],
            button_color=COLORS["primary_blue"],
            button_hover_color=COLORS["secondary_blue"],
            border_color=COLORS["border_subtle"],
            border_width=1,
            corner_radius=CORNER_RADIUS,
            command=self._on_employee_combobox_select
        )
        self.employee_combobox.grid(row=2, column=1, padx=10, pady=5, sticky="ew")
        
        self.populate_employee_combobox()

        if not is_issue_action:
            ctk.CTkLabel(main_frame, text="Condition Notes:", font=FONTS["body"], text_color=COLORS["text_dark"]).grid(row=3, column=0, padx=10, pady=5, sticky="nw")
            self.condition_notes_text = ctk.CTkTextbox(
                main_frame,
                height=80,
                width=250,
                font=FONTS["body"],
                fg_color=COLORS["background_light"],
                text_color=COLORS["text_dark"],
                border_color=COLORS["border_subtle"],
                border_width=1,
                corner_radius=CORNER_RADIUS
            )
            self.condition_notes_text.grid(row=3, column=1, padx=10, pady=5, sticky="ew")

        action_btn = ctk.CTkButton(
            main_frame,
            text=f"{action_type} C.G.",
            command=self._perform_action,
            height=45,
            font=FONTS["subheading"],
            text_color=COLORS["card_background"],
            corner_radius=CORNER_RADIUS,
            fg_color=COLORS["primary_blue"] if is_issue_action else COLORS["accent_success"],
            hover_color=COLORS["secondary_blue"] if is_issue_action else COLORS["secondary_blue"]
        )
        action_btn.grid(row=4 if is_issue_action else 5, column=0, columnspan=2, pady=20)

        self.wait_window(self)

    def populate_employee_combobox(self):
        employees = get_all_employees_db()
        self.employee_data = {"-- Select Employee --": None}
        for emp in employees:
            self.employee_data[emp['employee_name']] = emp['employee_id']
        
        self.filtered_employee_names = list(self.employee_data.keys())
        self.employee_combobox.configure(values=self.filtered_employee_names)
        
        if self.is_issue_action:
            self.employee_combobox.set("-- Select Employee --")
        else:
            if self.initial_employee_id:
                initial_employee_name = get_employee_by_id_db(self.initial_employee_id)['employee_name']
                if initial_employee_name in self.employee_data:
                    self.employee_combobox.set(initial_employee_name)
                else:
                    self.employee_combobox.set("Employee Not Found (ID: {})".format(self.initial_employee_id))
                    self.employee_combobox.configure(state="disabled")
            else:
                self.employee_combobox.set("-- Select Employee --")

    def _on_employee_combobox_select(self, selected_name):
        pass

    def _perform_action(self):
        selected_employee_name = self.employee_combobox.get()
        employee_id = self.employee_data.get(selected_employee_name)

        if employee_id is None:
            messagebox.showerror("Input Error", "Please select a valid employee.", icon="warning")
            return

        user_id = self.db_manager.current_user['id'] if self.db_manager.current_user else None
        
        if self.is_issue_action:
            success, message = issue_cg_db(self.cg_id, employee_id, user_id)
            action_log = "C.G. issued"
        else:
            condition_notes = self.condition_notes_text.get("1.0", tk.END).strip()
            if not condition_notes:
                condition_notes = "Good condition"
            success, message = return_cg_db(self.cg_id, employee_id, condition_notes, user_id)
            action_log = "C.G. returned"

        messagebox.showinfo("Success" if success else "Error", message, icon="info" if success else "error")
        if success:
            self.db_manager.log_activity(action_log, f"C.G. '{self.cg_code if self.cg_code else 'N/A'}' ({self.cg_name}) {action_log} to/by '{selected_employee_name}'") 
            self.parent.refresh_all_data() 
            self.destroy()

class RegisterCGDialog(ctk.CTkToplevel):
    def __init__(self, parent, db_manager):
        super().__init__(parent)
        self.parent = parent
        self.db_manager = db_manager

        self.title("Register New C.G.")
        self.transient(parent)
        self.grab_set()
        self.resizable(False, False)

        parent_x = parent.winfo_x()
        parent_y = parent.winfo_y()
        parent_width = parent.winfo_width()
        parent_height = parent.winfo_height()

        self.update_idletasks() 
        dialog_width = 450
        dialog_height = 450 
        self.geometry(f"{dialog_width}x{dialog_height}+{parent_x + (parent_width - dialog_width) // 2}+{parent_y + (parent_height - dialog_height) // 2}")

        self.configure(fg_color=COLORS["card_background"], corner_radius=CORNER_RADIUS)

        main_frame = ctk.CTkFrame(self, fg_color="transparent")
        main_frame.pack(expand=True, fill="both", padx=20, pady=20)
        main_frame.grid_columnconfigure(1, weight=1)

        ctk.CTkLabel(main_frame, text="C.G. Code (Optional):", font=FONTS["body"], text_color=COLORS["text_dark"]).grid(row=0, column=0, padx=10, pady=5, sticky="w")
        self.cg_code_entry = ctk.CTkEntry(main_frame, width=250, font=FONTS["body"],
                                             fg_color=COLORS["background_light"], text_color=COLORS["text_dark"],
                                             border_color=COLORS["border_subtle"], border_width=1, corner_radius=CORNER_RADIUS,
                                             placeholder_text="Leave blank for no code") 
        self.cg_code_entry.grid(row=0, column=1, padx=10, pady=5, sticky="ew")

        ctk.CTkLabel(main_frame, text="C.G. Name:", font=FONTS["body"], text_color=COLORS["text_dark"]).grid(row=1, column=0, padx=10, pady=5, sticky="w")
        self.cg_name_entry = ctk.CTkEntry(main_frame, width=250, font=FONTS["body"],
                                             fg_color=COLORS["background_light"], text_color=COLORS["text_dark"],
                                             border_color=COLORS["border_subtle"], border_width=1, corner_radius=CORNER_RADIUS)
        self.cg_name_entry.grid(row=1, column=1, padx=10, pady=5, sticky="ew")

        ctk.CTkLabel(main_frame, text="Description (Optional):", font=FONTS["body"], text_color=COLORS["text_dark"]).grid(row=2, column=0, padx=10, pady=5, sticky="nw")
        self.cg_description_text = ctk.CTkTextbox(main_frame, height=80, width=250, 
                                                     font=FONTS["body"], fg_color=COLORS["background_light"],
                                                     text_color=COLORS["text_dark"],
                                                     border_color=COLORS["border_subtle"], border_width=1, corner_radius=CORNER_RADIUS)
        self.cg_description_text.grid(row=2, column=1, padx=10, pady=5, sticky="ew")

        ctk.CTkLabel(main_frame, text="Category:", font=FONTS["body"], text_color=COLORS["text_dark"]).grid(row=3, column=0, padx=10, pady=5, sticky="w")
        self.category_combobox = ctk.CTkComboBox(main_frame, state="readonly", width=250, font=FONTS["body"],
                                                     fg_color=COLORS["background_light"], text_color=COLORS["text_dark"],
                                                     dropdown_fg_color=COLORS["card_background"], dropdown_text_color=COLORS["text_dark"],
                                                     dropdown_hover_color=COLORS["hover_light"], button_color=COLORS["primary_blue"],
                                                     button_hover_color=COLORS["secondary_blue"], border_color=COLORS["border_subtle"],
                                                     border_width=1, corner_radius=CORNER_RADIUS)
        self.populate_categories()
        self.category_combobox.grid(row=3, column=1, padx=10, pady=5, sticky="ew")

        register_btn = ctk.CTkButton(main_frame, text="Register C.G.", command=self._register_cg,
                                     height=45, font=FONTS["subheading"], text_color=COLORS["card_background"],
                                     corner_radius=CORNER_RADIUS, fg_color=COLORS["primary_blue"],
                                     hover_color=COLORS["secondary_blue"])
        register_btn.grid(row=4, column=0, columnspan=2, pady=20)

        self.wait_window(self)

    def populate_categories(self):
        categories = get_all_categories_db()
        self.category_data = {"-- Select Category --": None}
        for cat in categories:
            self.category_data[cat['category_name']] = cat['category_id']
        
        self.category_combobox.configure(values=list(self.category_data.keys()))
        self.category_combobox.set("-- Select Category --")

    def _register_cg(self):
        cg_code = self.cg_code_entry.get().strip()
        cg_name = self.cg_name_entry.get().strip()
        description = self.cg_description_text.get("1.0", tk.END).strip()
        selected_category_name = self.category_combobox.get()
        category_id = self.category_data.get(selected_category_name)

        if not cg_name: 
            messagebox.showerror("Input Error", "C.G. Name is required.", icon="warning")
            return
        if selected_category_name == "-- Select Category --":
            messagebox.showerror("Input Error", "Please select a category.", icon="warning")
            return
        
        user_id = self.db_manager.current_user['id'] if self.db_manager.current_user else None
        success, message = add_cg_db(cg_code, cg_name, description, category_id, user_id)
        messagebox.showinfo("Success" if success else "Error", message, icon="info" if success else "error")
        if success:
            self.db_manager.log_activity("cg_registered", f"Registered new C.G.: {cg_name} (Code: {cg_code if cg_code else 'N/A'})")
            self.parent.refresh_all_data()
            self.destroy()

class EditCGDialog(ctk.CTkToplevel):
    def __init__(self, parent, db_manager, cg_id):
        super().__init__(parent)
        self.parent = parent
        self.db_manager = db_manager
        self.cg_id = cg_id
        self.original_cg_code = None

        self.title("Edit C.G. Details")
        self.transient(parent)
        self.grab_set()
        self.resizable(False, False)

        parent_x = parent.winfo_x()
        parent_y = parent.winfo_y()
        parent_width = parent.winfo_width()
        parent_height = parent.winfo_height()

        self.update_idletasks() 
        dialog_width = 450
        dialog_height = 450 
        self.geometry(f"{dialog_width}x{dialog_height}+{parent_x + (parent_width - dialog_width) // 2}+{parent_y + (parent_height - dialog_height) // 2}")

        self.configure(fg_color=COLORS["card_background"], corner_radius=CORNER_RADIUS)

        main_frame = ctk.CTkFrame(self, fg_color="transparent")
        main_frame.pack(expand=True, fill="both", padx=20, pady=20)
        main_frame.grid_columnconfigure(1, weight=1)

        ctk.CTkLabel(main_frame, text="C.G. Code (Optional):", font=FONTS["body"], text_color=COLORS["text_dark"]).grid(row=0, column=0, padx=10, pady=5, sticky="w")
        self.cg_code_entry = ctk.CTkEntry(main_frame, width=250, font=FONTS["body"],
                                             fg_color=COLORS["background_light"], text_color=COLORS["text_dark"],
                                             border_color=COLORS["border_subtle"], border_width=1, corner_radius=CORNER_RADIUS,
                                             placeholder_text="Leave blank for no code") 
        self.cg_code_entry.grid(row=0, column=1, padx=10, pady=5, sticky="ew")

        ctk.CTkLabel(main_frame, text="C.G. Name:", font=FONTS["body"], text_color=COLORS["text_dark"]).grid(row=1, column=0, padx=10, pady=5, sticky="w")
        self.cg_name_entry = ctk.CTkEntry(main_frame, width=250, font=FONTS["body"],
                                             fg_color=COLORS["background_light"], text_color=COLORS["text_dark"],
                                             border_color=COLORS["border_subtle"], border_width=1, corner_radius=CORNER_RADIUS)
        self.cg_name_entry.grid(row=1, column=1, padx=10, pady=5, sticky="ew")

        ctk.CTkLabel(main_frame, text="Description (Optional):", font=FONTS["body"], text_color=COLORS["text_dark"]).grid(row=2, column=0, padx=10, pady=5, sticky="nw")
        self.cg_description_text = ctk.CTkTextbox(main_frame, height=80, width=250, 
                                                     font=FONTS["body"], fg_color=COLORS["background_light"],
                                                     text_color=COLORS["text_dark"],
                                                     border_color=COLORS["border_subtle"], border_width=1, corner_radius=CORNER_RADIUS)
        self.cg_description_text.grid(row=2, column=1, padx=10, pady=5, sticky="ew")

        ctk.CTkLabel(main_frame, text="Category:", font=FONTS["body"], text_color=COLORS["text_dark"]).grid(row=3, column=0, padx=10, pady=5, sticky="w")
        self.category_combobox = ctk.CTkComboBox(main_frame, state="readonly", width=250, font=FONTS["body"],
                                                     fg_color=COLORS["background_light"], text_color=COLORS["text_dark"],
                                                     dropdown_fg_color=COLORS["card_background"], dropdown_text_color=COLORS["text_dark"],
                                                     dropdown_hover_color=COLORS["hover_light"], button_color=COLORS["primary_blue"],
                                                     button_hover_color=COLORS["secondary_blue"], border_color=COLORS["border_subtle"],
                                                     border_width=1, corner_radius=CORNER_RADIUS)
        self.populate_categories()
        self.category_combobox.grid(row=3, column=1, padx=10, pady=5, sticky="ew")

        save_btn = ctk.CTkButton(main_frame, text="Save Changes", command=self._save_changes,
                                     height=45, font=FONTS["subheading"], text_color=COLORS["card_background"],
                                     corner_radius=CORNER_RADIUS, fg_color=COLORS["primary_blue"],
                                     hover_color=COLORS["secondary_blue"])
        save_btn.grid(row=4, column=0, columnspan=2, pady=20)
        
        self._load_cg_data()
        self.wait_window(self)

    def populate_categories(self):
        categories = get_all_categories_db()
        self.category_data = {"-- Select Category --": None}
        for cat in categories:
            self.category_data[cat['category_name']] = cat['category_id']
        
        self.category_combobox.configure(values=list(self.category_data.keys()))
        self.category_combobox.set("-- Select Category --")

    def _load_cg_data(self):
        cg_data = get_cg_by_id_db(self.cg_id)
        if cg_data:
            self.cg_code_entry.insert(0, cg_data['cg_code'] if cg_data['cg_code'] else "")
            self.original_cg_code = cg_data['cg_code']
            self.cg_name_entry.insert(0, cg_data['cg_name'])
            self.cg_description_text.insert("1.0", cg_data['description'] if cg_data['description'] else "")
            
            if cg_data['category_name'] in self.category_data:
                self.category_combobox.set(cg_data['category_name'])
            else:
                self.category_combobox.set("-- Select Category --")

    def _save_changes(self):
        cg_code = self.cg_code_entry.get().strip()
        cg_name = self.cg_name_entry.get().strip()
        description = self.cg_description_text.get("1.0", tk.END).strip()
        selected_category_name = self.category_combobox.get()
        category_id = self.category_data.get(selected_category_name)

        if not cg_name: 
            messagebox.showerror("Input Error", "C.G. Name is required.", icon="warning")
            return
        if selected_category_name == "-- Select Category --":
            messagebox.showerror("Input Error", "Please select a category.", icon="warning")
            return
        
        success, message = update_cg_db(self.cg_id, cg_code, cg_name, description, category_id)
        self.show_message(message, is_error=not success)
        if success:
            self.db_manager.log_activity("cg_updated", f"Updated C.G.: {cg_name} (Code: {cg_code if cg_code else 'N/A'})")
            self.parent.refresh_all_data()
            self.destroy()

class AddDeleteCategoryDialog(ctk.CTkToplevel):
    def __init__(self, parent, db_manager):
        super().__init__(parent)
        self.parent = parent
        self.db_manager = db_manager

        self.title("Add / Delete Category")
        self.transient(parent)
        self.grab_set()
        self.resizable(False, False)

        parent_x = parent.winfo_x()
        parent_y = parent.winfo_y()
        parent_width = parent.winfo_width()
        parent_height = parent.winfo_height()

        self.update_idletasks() 
        dialog_width = 550
        dialog_height = 500 
        self.geometry(f"{dialog_width}x{dialog_height}+{parent_x + (parent_width - dialog_width) // 2}+{parent_y + (parent_height - dialog_height) // 2}")

        self.configure(fg_color=COLORS["card_background"], corner_radius=CORNER_RADIUS)

        main_frame = ctk.CTkFrame(self, fg_color="transparent")
        main_frame.pack(expand=True, fill="both", padx=20, pady=20)
        main_frame.grid_columnconfigure(0, weight=1)
        main_frame.grid_rowconfigure(1, weight=1)

        add_delete_frame = ctk.CTkFrame(main_frame, fg_color="transparent")
        add_delete_frame.grid(row=0, column=0, sticky="ew", pady=(0, 20))
        add_delete_frame.grid_columnconfigure(1, weight=1)
        add_delete_frame.grid_columnconfigure(2, weight=0)
        add_delete_frame.grid_columnconfigure(3, weight=0)

        ctk.CTkLabel(add_delete_frame, text="Category Name:", font=FONTS["body"], text_color=COLORS["text_dark"]).grid(row=0, column=0, padx=10, pady=5, sticky="w")
        self.new_category_entry = ctk.CTkEntry(add_delete_frame, width=250, font=FONTS["body"],
                                                   fg_color=COLORS["background_light"], text_color=COLORS["text_dark"],
                                                   border_color=COLORS["border_subtle"], border_width=1, corner_radius=CORNER_RADIUS,
                                                   placeholder_text="Enter new category name") 
        self.new_category_entry.grid(row=0, column=1, padx=10, pady=5, sticky="ew")

        add_btn = ctk.CTkButton(add_delete_frame, text="Add", command=self._add_category,
                                       height=40, font=FONTS["subheading"], text_color=COLORS["card_background"],
                                       corner_radius=CORNER_RADIUS, fg_color=COLORS["primary_blue"],
                                       hover_color=COLORS["secondary_blue"])
        add_btn.grid(row=0, column=2, padx=(10, 5), pady=5)

        self.delete_category_btn = ctk.CTkButton(
            add_delete_frame,
            text="Delete",
            command=self._delete_category,
            height=40,
            font=FONTS["subheading"],
            text_color=COLORS["card_background"],
            corner_radius=CORNER_RADIUS,
            fg_color=COLORS["accent_error"],
            hover_color=COLORS["text_dark"],
            state="disabled"
        )
        self.delete_category_btn.grid(row=0, column=3, padx=(5, 10), pady=5)


        existing_categories_frame = ctk.CTkFrame(main_frame, fg_color="transparent")
        existing_categories_frame.grid(row=1, column=0, sticky="nsew", pady=(0, 10))
        existing_categories_frame.grid_columnconfigure(0, weight=1)
        existing_categories_frame.grid_rowconfigure(1, weight=1)

        ctk.CTkLabel(existing_categories_frame, text="Existing Categories:", font=FONTS["body"], text_color=COLORS["text_dark"]).grid(row=0, column=0, padx=10, pady=(10, 5), sticky="w")
        
        search_frame = ctk.CTkFrame(existing_categories_frame, fg_color="transparent")
        search_frame.grid(row=0, column=0, sticky="ew", padx=10, pady=(0, 5))
        search_frame.grid_columnconfigure(0, weight=1)

        self.category_search_entry = ctk.CTkEntry(
            search_frame,
            width=250,
            height=40,
            font=FONTS["body"],
            fg_color=COLORS["background_light"],
            text_color=COLORS["text_dark"],
            placeholder_text="Search categories",
            border_color=COLORS["border_subtle"],
            border_width=1,
            corner_radius=CORNER_RADIUS
        )
        self.category_search_entry.grid(row=0, column=0, sticky="ew")
        self.category_search_entry.bind("<KeyRelease>", self._filter_categories)

        table_frame = ctk.CTkFrame(
            existing_categories_frame,
            fg_color=COLORS["background_light"],
            corner_radius=CORNER_RADIUS,
            border_width=1,
            border_color=COLORS["border_subtle"]
        )
        table_frame.grid(row=1, column=0, sticky="nsew", padx=10, pady=(5, 10))
        table_frame.grid_columnconfigure(0, weight=1)
        table_frame.grid_rowconfigure(0, weight=1)

        self.categories_tree = ttk.Treeview(table_frame, columns=("ID", "Category Name"), show="headings", selectmode="browse")
        self.categories_tree.heading("ID", text="ID")
        self.categories_tree.heading("Category Name", text="Category Name")
        self.categories_tree.column("ID", width=0, stretch=tk.NO)
        self.categories_tree.column("Category Name", width=300, anchor=tk.CENTER)

        scrollbar = ttk.Scrollbar(table_frame, orient="vertical", command=self.categories_tree.yview, style="Vertical.TScrollbar")
        self.categories_tree.configure(yscrollcommand=scrollbar.set)
        self.categories_tree.grid(row=0, column=0, sticky="nsew", padx=(5,0), pady=5)
        scrollbar.grid(row=0, column=1, sticky="ns", padx=(0,5), pady=5)
        
        self.categories_tree.tag_configure("evenrow", background=COLORS["card_background"], foreground=COLORS["text_dark"])
        self.categories_tree.tag_configure("oddrow", background=COLORS["zebra_stripe"], foreground=COLORS["text_dark"])

        self.populate_categories_treeview()
        self.categories_tree.bind("<<TreeviewSelect>>", self._on_category_select)

        self.wait_window(self)

    def populate_categories_treeview(self, search_term=""):
        for i in self.categories_tree.get_children():
            self.categories_tree.delete(i)
        
        categories = get_all_categories_db()
        filtered_categories = [cat for cat in categories if search_term.lower() in cat['category_name'].lower()]

        for i, cat in enumerate(filtered_categories):
            tag = "evenrow" if i % 2 == 0 else "oddrow"
            self.categories_tree.insert("", "end", values=(cat['category_id'], cat['category_name']), tags=(tag,))
        
        self.delete_category_btn.configure(state="disabled")

    def _filter_categories(self, event=None):
        search_term = self.category_search_entry.get().strip()
        self.populate_categories_treeview(search_term)

    def _on_category_select(self, event):
        selected_item = self.categories_tree.focus()
        if selected_item:
            self.delete_category_btn.configure(state="normal")
        else:
            self.delete_category_btn.configure(state="disabled")

    def _add_category(self):
        category_name = self.new_category_entry.get().strip()
        if not category_name:
            messagebox.showerror("Input Error", "Category Name cannot be empty.", icon="warning")
            return

        success, message = add_category_db(category_name)
        messagebox.showinfo("Success" if success else "Error", message, icon="info" if success else "error")
        if success:
            self.db_manager.log_activity("category_added", f"Added new category: {category_name}")
            self.new_category_entry.delete(0, tk.END)
            self.populate_categories_treeview()
            self.parent.refresh_all_data()

    def _delete_category(self):
        selected_item = self.categories_tree.focus()
        if not selected_item:
            messagebox.showerror("Selection Error", "Please select a category to delete.", icon="warning")
            return
        
        item_data = self.categories_tree.item(selected_item)
        category_id = item_data['values'][0]
        category_name = item_data['values'][1]

        self.parent.show_custom_confirm_dialog(
            title="Confirm Category Deletion",
            message=f"Are you sure you want to delete category:\n'{category_name}'?\n\nAssociated C.G.s will be moved to 'Unassigned Category'.",
            on_yes=lambda: self._perform_delete_category(category_id, category_name)
        )

    def _perform_delete_category(self, category_id, category_name):
        success, message = delete_category_db(category_id, category_name)
        messagebox.showinfo("Success" if success else "Error", message, icon="info" if success else "error")
        if success:
            self.db_manager.log_activity("category_deleted", f"Deleted category: {category_name} (ID: {category_id})")
            self.populate_categories_treeview()
            self.parent.refresh_all_data()

class AddRemoveEmployeeDialog(ctk.CTkToplevel):
    def __init__(self, parent, db_manager):
        super().__init__(parent)
        self.parent = parent
        self.db_manager = db_manager

        self.title("Add / Remove Employee")
        self.transient(parent)
        self.grab_set()
        self.resizable(False, False)

        parent_x = parent.winfo_x()
        parent_y = parent.winfo_y()
        parent_width = parent.winfo_width()
        parent_height = parent.winfo_height()

        self.update_idletasks() 
        dialog_width = 550
        dialog_height = 500 
        self.geometry(f"{dialog_width}x{dialog_height}+{parent_x + (parent_width - dialog_width) // 2}+{parent_y + (parent_height - dialog_height) // 2}")

        self.configure(fg_color=COLORS["card_background"], corner_radius=CORNER_RADIUS)

        main_frame = ctk.CTkFrame(self, fg_color="transparent")
        main_frame.pack(expand=True, fill="both", padx=20, pady=20)
        main_frame.grid_columnconfigure(0, weight=1)
        main_frame.grid_rowconfigure(1, weight=1)

        add_remove_frame = ctk.CTkFrame(main_frame, fg_color="transparent")
        add_remove_frame.grid(row=0, column=0, sticky="ew", pady=(0, 20))
        add_remove_frame.grid_columnconfigure(1, weight=1)
        add_remove_frame.grid_columnconfigure(2, weight=0)
        add_remove_frame.grid_columnconfigure(3, weight=0)

        ctk.CTkLabel(add_remove_frame, text="Employee Name:", font=FONTS["body"], text_color=COLORS["text_dark"]).grid(row=0, column=0, padx=10, pady=5, sticky="w")
        self.new_employee_entry = ctk.CTkEntry(add_remove_frame, width=250, font=FONTS["body"],
                                                   fg_color=COLORS["background_light"], text_color=COLORS["text_dark"],
                                                   border_color=COLORS["border_subtle"], border_width=1, corner_radius=CORNER_RADIUS,
                                                   placeholder_text="Enter new employee name")
        self.new_employee_entry.grid(row=0, column=1, padx=10, pady=5, sticky="ew")

        add_btn = ctk.CTkButton(add_remove_frame, text="Add", command=self._add_employee,
                                       height=40, font=FONTS["subheading"], text_color=COLORS["card_background"],
                                       corner_radius=CORNER_RADIUS, fg_color=COLORS["primary_blue"],
                                       hover_color=COLORS["secondary_blue"])
        add_btn.grid(row=0, column=2, padx=(10, 5), pady=5)

        self.remove_employee_btn = ctk.CTkButton(
            add_remove_frame,
            text="Delete",
            command=self._remove_employee,
            height=40,
            font=FONTS["subheading"],
            text_color=COLORS["card_background"],
            corner_radius=CORNER_RADIUS,
            fg_color=COLORS["accent_error"],
            hover_color=COLORS["text_dark"],
            state="disabled"
        )
        self.remove_employee_btn.grid(row=0, column=3, padx=(5, 10), pady=5)

        existing_employees_frame = ctk.CTkFrame(main_frame, fg_color="transparent")
        existing_employees_frame.grid(row=1, column=0, sticky="nsew", pady=(0, 10))
        existing_employees_frame.grid_columnconfigure(0, weight=1)
        existing_employees_frame.grid_rowconfigure(1, weight=1)

        ctk.CTkLabel(existing_employees_frame, text="Existing Employees:", font=FONTS["body"], text_color=COLORS["text_dark"]).grid(row=0, column=0, padx=10, pady=(10, 5), sticky="w")
        
        search_frame = ctk.CTkFrame(existing_employees_frame, fg_color="transparent")
        search_frame.grid(row=0, column=0, sticky="ew", padx=10, pady=(0, 5))
        search_frame.grid_columnconfigure(0, weight=1)

        self.employee_search_entry = ctk.CTkEntry(
            search_frame,
            width=250,
            height=40,
            font=FONTS["body"],
            fg_color=COLORS["background_light"],
            text_color=COLORS["text_dark"],
            placeholder_text="Search employees",
            border_color=COLORS["border_subtle"],
            border_width=1,
            corner_radius=CORNER_RADIUS
        )
        self.employee_search_entry.grid(row=0, column=0, sticky="ew")
        self.employee_search_entry.bind("<KeyRelease>", self._filter_employees)

        table_frame = ctk.CTkFrame(
            existing_employees_frame,
            fg_color=COLORS["background_light"],
            corner_radius=CORNER_RADIUS,
            border_width=1,
            border_color=COLORS["border_subtle"]
        )
        table_frame.grid(row=1, column=0, sticky="nsew", padx=10, pady=(5, 10))
        table_frame.grid_columnconfigure(0, weight=1)
        table_frame.grid_rowconfigure(0, weight=1)

        self.employees_tree = ttk.Treeview(table_frame, columns=("ID", "Employee Name"), show="headings", selectmode="browse")
        self.employees_tree.heading("ID", text="ID")
        self.employees_tree.heading("Employee Name", text="Employee Name")
        self.employees_tree.column("ID", width=0, stretch=tk.NO)
        self.employees_tree.column("Employee Name", width=300, anchor=tk.CENTER)

        scrollbar = ttk.Scrollbar(table_frame, orient="vertical", command=self.employees_tree.yview, style="Vertical.TScrollbar")
        self.employees_tree.configure(yscrollcommand=scrollbar.set)
        self.employees_tree.grid(row=0, column=0, sticky="nsew", padx=(5,0), pady=5)
        scrollbar.grid(row=0, column=1, sticky="ns", padx=(0,5), pady=5)
        
        self.employees_tree.tag_configure("evenrow", background=COLORS["card_background"], foreground=COLORS["text_dark"])
        self.employees_tree.tag_configure("oddrow", background=COLORS["zebra_stripe"], foreground=COLORS["text_dark"])

        self.populate_employees_treeview()
        self.employees_tree.bind("<<TreeviewSelect>>", self._on_employee_select)

        self.wait_window(self)

    def populate_employees_treeview(self, search_term=""):
        for i in self.employees_tree.get_children():
            self.employees_tree.delete(i)
        
        employees = get_all_employees_db(search_term)
        
        for i, emp in enumerate(employees):
            tag = "evenrow" if i % 2 == 0 else "oddrow"
            self.employees_tree.insert("", "end", values=(emp['employee_id'], emp['employee_name']), tags=(tag,))
        
        self.remove_employee_btn.configure(state="disabled")

    def _filter_employees(self, event=None):
        search_term = self.employee_search_entry.get().strip()
        self.populate_employees_treeview(search_term)

    def _on_employee_select(self, event):
        selected_item = self.employees_tree.focus()
        if selected_item:
            self.remove_employee_btn.configure(state="normal")
        else:
            self.remove_employee_btn.configure(state="disabled")

    def _add_employee(self):
        employee_name = self.new_employee_entry.get().strip()
        if not employee_name:
            messagebox.showerror("Input Error", "Employee Name cannot be empty.", icon="warning")
            return

        success, message = add_employee_db(employee_name)
        messagebox.showinfo("Success" if success else "Error", message, icon="info" if success else "error")
        if success:
            self.db_manager.log_activity("employee_added", f"Added new employee: {employee_name}")
            self.new_employee_entry.delete(0, tk.END)
            self.populate_employees_treeview()
            self.parent.refresh_all_data()

    def _remove_employee(self):
        selected_item = self.employees_tree.focus()
        if not selected_item:
            messagebox.showerror("Selection Error", "Please select an employee to remove.", icon="warning")
            return
        
        item_data = self.employees_tree.item(selected_item)
        employee_id = item_data['values'][0]
        employee_name = item_data['values'][1]

        self.parent.show_custom_confirm_dialog(
            title="Confirm Employee Removal",
            message=f"Are you sure you want to remove employee:\n'{employee_name}'?\n\nAny C.G.s currently issued to this employee will be returned.",
            on_yes=lambda: self._perform_remove_employee(employee_id, employee_name)
        )

    def _perform_remove_employee(self, employee_id, employee_name):
        success, message = delete_employee_db(employee_id, employee_name)
        messagebox.showinfo("Success" if success else "Error", message, icon="info" if success else "error")
        if success:
            self.db_manager.log_activity("employee_removed", f"Removed employee: {employee_name} (ID: {employee_id})")
            self.populate_employees_treeview()
            self.parent.refresh_all_data()

class BulkIssueCGDialog(ctk.CTkToplevel):
    def __init__(self, parent, db_manager):
        super().__init__(parent)
        self.parent = parent
        self.db_manager = db_manager
        self.title("Issue Multiple C.G.s to Employee")
        self.transient(parent)
        self.grab_set()
        self.resizable(False, False)

        self.employee_data = {}
        self.filtered_employee_names = []

        parent_x = parent.winfo_x()
        parent_y = parent.winfo_y()
        parent_width = parent.winfo_width()
        parent_height = parent.winfo_height()

        self.update_idletasks()
        dialog_width = 800
        dialog_height = 600
        self.geometry(f"{dialog_width}x{dialog_height}+{parent_x + (parent_width - dialog_width) // 2}+{parent_y + (parent_height - dialog_height) // 2}")

        self.configure(fg_color=COLORS["card_background"], corner_radius=CORNER_RADIUS)

        main_frame = ctk.CTkFrame(self, fg_color="transparent")
        main_frame.pack(expand=True, fill="both", padx=20, pady=20)
        main_frame.grid_columnconfigure(0, weight=1)
        main_frame.grid_rowconfigure(2, weight=1)

        employee_frame = ctk.CTkFrame(main_frame, fg_color="transparent")
        employee_frame.grid(row=0, column=0, sticky="ew", pady=(0, 10))
        employee_frame.grid_columnconfigure(1, weight=1)

        ctk.CTkLabel(employee_frame, text="Employee Name:", font=FONTS["body"], text_color=COLORS["text_dark"]).grid(row=0, column=0, padx=10, pady=5, sticky="w")
        self.employee_combobox = ctk.CTkComboBox(
            employee_frame,
            state="readonly",
            width=300,
            height=40,
            font=FONTS["body"],
            fg_color=COLORS["background_light"],
            text_color=COLORS["text_dark"],
            dropdown_fg_color=COLORS["card_background"],
            dropdown_text_color=COLORS["text_dark"],
            dropdown_hover_color=COLORS["hover_light"],
            button_color=COLORS["primary_blue"],
            button_hover_color=COLORS["secondary_blue"],
            border_color=COLORS["border_subtle"],
            border_width=1,
            corner_radius=CORNER_RADIUS,
            command=self._on_employee_combobox_select
        )
        self.employee_combobox.grid(row=0, column=1, padx=10, pady=5, sticky="ew")
        self.populate_employee_combobox()


        search_frame = ctk.CTkFrame(main_frame, fg_color="transparent")
        search_frame.grid(row=1, column=0, sticky="ew", pady=(0, 10))
        search_frame.grid_columnconfigure(1, weight=1)

        ctk.CTkLabel(search_frame, text="Search Available C.G.s (Code/Name):", font=FONTS["body"], text_color=COLORS["text_dark"]).grid(row=0, column=0, padx=10, pady=5, sticky="w")
        self.cg_search_entry_bulk = ctk.CTkEntry(
            search_frame,
            width=300,
            height=40,
            font=FONTS["body"],
            fg_color=COLORS["background_light"],
            text_color=COLORS["text_dark"],
            placeholder_text="Search by code or name",
            border_color=COLORS["border_subtle"],
            border_width=1,
            corner_radius=CORNER_RADIUS
        )
        self.cg_search_entry_bulk.grid(row=0, column=1, padx=10, pady=5, sticky="ew")
        self.cg_search_entry_bulk.bind("<KeyRelease>", self._filter_available_cgs)


        table_frame = ctk.CTkFrame(
            main_frame,
            fg_color=COLORS["background_light"],
            corner_radius=CORNER_RADIUS,
            border_width=1,
            border_color=COLORS["border_subtle"]
        )
        table_frame.grid(row=2, column=0, sticky="nsew", padx=10, pady=(10, 20))
        table_frame.grid_columnconfigure(0, weight=1)
        table_frame.grid_rowconfigure(0, weight=1)

        self.available_cgs_tree_bulk = ttk.Treeview(table_frame, columns=(
            "ID", "C.G. Code", "C.G. Name", "Category", "Description"
        ), show="headings", selectmode="extended")

        self.available_cgs_tree_bulk.heading("ID", text="ID")
        self.available_cgs_tree_bulk.heading("C.G. Code", text="C.G. Code")
        self.available_cgs_tree_bulk.heading("C.G. Name", text="C.G. Name")
        self.available_cgs_tree_bulk.heading("Category", text="Category")
        self.available_cgs_tree_bulk.heading("Description", text="Description")

        self.available_cgs_tree_bulk.column("ID", width=0, stretch=tk.NO)
        self.available_cgs_tree_bulk.column("C.G. Code", width=100, anchor=tk.CENTER)
        self.available_cgs_tree_bulk.column("C.G. Name", width=150)
        self.available_cgs_tree_bulk.column("Category", width=100, anchor=tk.CENTER)
        self.available_cgs_tree_bulk.column("Description", width=250)

        scrollbar = ttk.Scrollbar(table_frame, orient="vertical", command=self.available_cgs_tree_bulk.yview, style="Vertical.TScrollbar")
        self.available_cgs_tree_bulk.configure(yscrollcommand=scrollbar.set)
        
        self.available_cgs_tree_bulk.grid(row=0, column=0, sticky="nsew", padx=(5,0), pady=5)
        scrollbar.grid(row=0, column=1, sticky="ns", padx=(0,5), pady=5)
        
        self.available_cgs_tree_bulk.tag_configure("evenrow", background=COLORS["card_background"], foreground=COLORS["text_dark"])
        self.available_cgs_tree_bulk.tag_configure("oddrow", background=COLORS["zebra_stripe"], foreground=COLORS["text_dark"])

        self._populate_available_cgs_tree()

        button_frame = ctk.CTkFrame(main_frame, fg_color="transparent")
        button_frame.grid(row=3, column=0, sticky="e", pady=(0, 10))

        issue_btn = ctk.CTkButton(
            button_frame,
            text="Issue Selected C.G.s",
            command=self._issue_selected_cgs,
            height=45,
            font=FONTS["subheading"],
            text_color=COLORS["card_background"],
            corner_radius=CORNER_RADIUS,
            fg_color=COLORS["primary_blue"],
            hover_color=COLORS["secondary_blue"]
        )
        issue_btn.pack(side="left", padx=(0, 10))

        cancel_btn = ctk.CTkButton(
            button_frame,
            text="Cancel",
            command=self.destroy,
            height=45,
            font=FONTS["subheading"],
            text_color=COLORS["card_background"],
            corner_radius=CORNER_RADIUS,
            fg_color=COLORS["text_light"],
            hover_color=COLORS["text_dark"]
        )
        cancel_btn.pack(side="left")
        
        self.wait_window(self)

    def populate_employee_combobox(self):
        employees = get_all_employees_db()
        self.employee_data = {"-- Select Employee --": None}
        for emp in employees:
            self.employee_data[emp['employee_name']] = emp['employee_id']
        
        self.filtered_employee_names = list(self.employee_data.keys())
        self.employee_combobox.configure(values=self.filtered_employee_names)
        self.employee_combobox.set("-- Select Employee --")

    def _on_employee_combobox_select(self, selected_name):
        pass

    def _populate_available_cgs_tree(self, search_term=""):
        for i in self.available_cgs_tree_bulk.get_children():
            self.available_cgs_tree_bulk.delete(i)
        
        available_cgs = get_all_cgs_db(search_term=search_term, status='Available')
        
        for i, cg in enumerate(available_cgs):
            code = cg['cg_code'] if cg['cg_code'] else "N/A"
            self.available_cgs_tree_bulk.insert("", "end", values=(
                cg['cg_id'], 
                code,
                cg['cg_name'],
                cg['category_name'] if cg['category_name'] else "Uncategorized",
                cg['description']
            ), tags=("evenrow" if i % 2 == 0 else "oddrow",))

    def _filter_available_cgs(self, event=None):
        search_term = self.cg_search_entry_bulk.get().strip()
        self._populate_available_cgs_tree(search_term)

    def _issue_selected_cgs(self):
        selected_employee_name = self.employee_combobox.get()
        employee_id = self.employee_data.get(selected_employee_name)

        if employee_id is None:
            messagebox.showerror("Input Error", "Please select a valid employee.", icon="warning")
            return

        selected_items = self.available_cgs_tree_bulk.selection()
        if not selected_items:
            messagebox.showerror("Selection Error", "Please select one or more C.G.s to issue.", icon="warning")
            return

        user_id = self.db_manager.current_user['id'] if self.db_manager.current_user else None
        
        issued_count = 0
        failed_issues = []

        for item in selected_items:
            cg_id = self.available_cgs_tree_bulk.item(item, 'values')[0]
            cg_code = self.available_cgs_tree_bulk.item(item, 'values')[1]
            cg_name = self.available_cgs_tree_bulk.item(item, 'values')[2]

            success, message = issue_cg_db(cg_id, employee_id, user_id)
            if success:
                issued_count += 1
                self.db_manager.log_activity("C.G. issued (bulk)", f"C.G. '{cg_code if cg_code else 'N/A'}' ({cg_name}) issued to '{selected_employee_name}' (bulk operation).")
            else:
                failed_issues.append(f"C.G. '{cg_code if cg_code else 'N/A'}' ({cg_name}): {message}")

        if issued_count > 0:
            success_message = f"Successfully issued {issued_count} C.G.s to '{selected_employee_name}'."
            if failed_issues:
                success_message += "\n\nHowever, some C.G.s could not be issued:\n" + "\n".join(failed_issues)
                messagebox.showwarning("Bulk Issue Partial Success", success_message, icon="warning")
            else:
                messagebox.showinfo("Bulk Issue Success", success_message, icon="info")
        else:
            messagebox.showerror("Bulk Issue Failed", "No C.G.s were successfully issued.\n\n" + "\n".join(failed_issues), icon="error")

        self.parent.refresh_all_data()
        self.destroy()

class BulkReturnCGDialog(ctk.CTkToplevel):
    def __init__(self, parent, db_manager):
        super().__init__(parent)
        self.parent = parent
        self.db_manager = db_manager
        self.title("Return Multiple C.G.s from Employee")
        self.transient(parent)
        self.grab_set()
        self.resizable(False, False)

        self.employee_data = {}

        parent_x = parent.winfo_x()
        parent_y = parent.winfo_y()
        parent_width = parent.winfo_width()
        parent_height = parent.winfo_height()

        self.update_idletasks()
        dialog_width = 800
        dialog_height = 650
        self.geometry(f"{dialog_width}x{dialog_height}+{parent_x + (parent_width - dialog_width) // 2}+{parent_y + (parent_height - dialog_height) // 2}")

        self.configure(fg_color=COLORS["card_background"], corner_radius=CORNER_RADIUS)

        main_frame = ctk.CTkFrame(self, fg_color="transparent")
        main_frame.pack(expand=True, fill="both", padx=20, pady=20)
        main_frame.grid_columnconfigure(0, weight=1)
        main_frame.grid_rowconfigure(2, weight=1)

        employee_frame = ctk.CTkFrame(main_frame, fg_color="transparent")
        employee_frame.grid(row=0, column=0, sticky="ew", pady=(0, 10))
        employee_frame.grid_columnconfigure(1, weight=1)

        ctk.CTkLabel(employee_frame, text="Select Employee:", font=FONTS["body"], text_color=COLORS["text_dark"]).grid(row=0, column=0, padx=10, pady=5, sticky="w")
        self.employee_combobox = ctk.CTkComboBox(
            employee_frame,
            state="readonly",
            width=300,
            height=40,
            font=FONTS["body"],
            fg_color=COLORS["background_light"],
            text_color=COLORS["text_dark"],
            dropdown_fg_color=COLORS["card_background"], dropdown_text_color=COLORS["text_dark"],
            dropdown_hover_color=COLORS["hover_light"], button_color=COLORS["primary_blue"],
            button_hover_color=COLORS["secondary_blue"], border_color=COLORS["border_subtle"],
            border_width=1,
            corner_radius=CORNER_RADIUS,
            command=self._on_employee_selected
        )
        self.employee_combobox.grid(row=0, column=1, padx=10, pady=5, sticky="ew")
        self.populate_employee_combobox()

        ctk.CTkLabel(main_frame, text="Condition Notes (for all returns):", font=FONTS["body"], text_color=COLORS["text_dark"]).grid(row=1, column=0, padx=10, pady=5, sticky="nw")
        self.condition_notes_text_bulk = ctk.CTkTextbox(
            main_frame,
            height=80,
            width=250,
            font=FONTS["body"],
            fg_color=COLORS["background_light"],
            text_color=COLORS["text_dark"],
            border_color=COLORS["border_subtle"],
            border_width=1,
            corner_radius=CORNER_RADIUS
        )
        self.condition_notes_text_bulk.grid(row=1, column=0, padx=10, pady=5, sticky="ew")
        
        table_frame = ctk.CTkFrame(
            main_frame,
            fg_color=COLORS["background_light"],
            corner_radius=CORNER_RADIUS,
            border_width=1,
            border_color=COLORS["border_subtle"]
        )
        table_frame.grid(row=2, column=0, sticky="nsew", padx=10, pady=(10, 20))
        table_frame.grid_columnconfigure(0, weight=1)
        table_frame.grid_rowconfigure(0, weight=1)

        self.issued_cgs_tree_bulk = ttk.Treeview(table_frame, columns=(
            "ID", "C.G. Code", "C.G. Name", "Category", "Description"
        ), show="headings", selectmode="extended")

        self.issued_cgs_tree_bulk.heading("ID", text="ID")
        self.issued_cgs_tree_bulk.heading("C.G. Code", text="C.G. Code")
        self.issued_cgs_tree_bulk.heading("C.G. Name", text="C.G. Name")
        self.issued_cgs_tree_bulk.heading("Category", text="Category")
        self.issued_cgs_tree_bulk.heading("Description", text="Description")

        self.issued_cgs_tree_bulk.column("ID", width=0, stretch=tk.NO)
        self.issued_cgs_tree_bulk.column("C.G. Code", width=100, anchor=tk.CENTER)
        self.issued_cgs_tree_bulk.column("C.G. Name", width=150)
        self.issued_cgs_tree_bulk.column("Category", width=100, anchor=tk.CENTER)
        self.issued_cgs_tree_bulk.column("Description", width=250)

        scrollbar = ttk.Scrollbar(table_frame, orient="vertical", command=self.issued_cgs_tree_bulk.yview, style="Vertical.TScrollbar")
        self.issued_cgs_tree_bulk.configure(yscrollcommand=scrollbar.set)
        
        self.issued_cgs_tree_bulk.grid(row=0, column=0, sticky="nsew", padx=(5,0), pady=5)
        scrollbar.grid(row=0, column=1, sticky="ns", padx=(0,5), pady=5)
        
        self.issued_cgs_tree_bulk.tag_configure("evenrow", background=COLORS["card_background"], foreground=COLORS["text_dark"])
        self.issued_cgs_tree_bulk.tag_configure("oddrow", background=COLORS["zebra_stripe"], foreground=COLORS["text_dark"])

        button_frame = ctk.CTkFrame(main_frame, fg_color="transparent")
        button_frame.grid(row=3, column=0, sticky="e", pady=(0, 10))

        return_btn = ctk.CTkButton(
            button_frame,
            text="Return Selected C.G.s",
            command=self._return_selected_cgs_bulk,
            height=45,
            font=FONTS["subheading"],
            text_color=COLORS["card_background"],
            corner_radius=CORNER_RADIUS,
            fg_color=COLORS["accent_success"],
            hover_color=COLORS["secondary_blue"]
        )
        return_btn.pack(side="left", padx=(0, 10))

        cancel_btn = ctk.CTkButton(
            button_frame,
            text="Cancel",
            command=self.destroy,
            height=45,
            font=FONTS["subheading"],
            text_color=COLORS["card_background"],
            corner_radius=CORNER_RADIUS,
            fg_color=COLORS["text_light"],
            hover_color=COLORS["text_dark"]
        )
        cancel_btn.pack(side="left")
        
        self.wait_window(self)

    def populate_employee_combobox(self):
        employees = get_all_employees_db()
        self.employee_data = {"-- Select Employee --": None}
        for emp in employees:
            self.employee_data[emp['employee_name']] = emp['employee_id']
        
        self.employee_combobox.configure(values=list(self.employee_data.keys()))
        self.employee_combobox.set("-- Select Employee --")

    def _on_employee_selected(self, selected_employee_name):
        selected_employee_id = self.employee_data.get(selected_employee_name)
        if selected_employee_id is None:
            self._clear_issued_cgs_tree()
            return
        
        self._populate_issued_cgs_tree(selected_employee_id)

    def _populate_issued_cgs_tree(self, employee_id):
        for i in self.issued_cgs_tree_bulk.get_children():
            self.issued_cgs_tree_bulk.delete(i)
        
        issued_cgs = get_cgs_issued_to_employee_db(employee_id)
        
        if not issued_cgs:
            self.issued_cgs_tree_bulk.insert("", "end", values=("", "", "No C.G.s currently issued to this employee.", "", ""), tags=("oddrow",))
            return

        for i, cg in enumerate(issued_cgs):
            code = cg['cg_code'] if cg['cg_code'] else "N/A"
            self.issued_cgs_tree_bulk.insert("", "end", values=(
                cg['cg_id'], 
                code,
                cg['cg_name'],
                cg['category_name'] if cg['category_name'] else "Uncategorized",
                cg['description']
            ), tags=("evenrow" if i % 2 == 0 else "oddrow",))

    def _clear_issued_cgs_tree(self):
        for i in self.issued_cgs_tree_bulk.get_children():
            self.issued_cgs_tree_bulk.delete(i)

    def _return_selected_cgs_bulk(self):
        selected_employee_name = self.employee_combobox.get()
        employee_id = self.employee_data.get(selected_employee_name)

        if employee_id is None:
            messagebox.showerror("Input Error", "Please select a valid employee.", icon="warning")
            return

        selected_items = self.issued_cgs_tree_bulk.selection()
        if not selected_items:
            messagebox.showerror("Selection Error", "Please select one or more C.G.s to return.", icon="warning")
            return

        condition_notes = self.condition_notes_text_bulk.get("1.0", tk.END).strip()
        if not condition_notes:
            condition_notes = "Good condition"

        user_id = self.db_manager.current_user['id'] if self.db_manager.current_user else None
        
        returned_count = 0
        failed_returns = []

        for item in selected_items:
            cg_id = self.issued_cgs_tree_bulk.item(item, 'values')[0]
            cg_code = self.issued_cgs_tree_bulk.item(item, 'values')[1]
            cg_name = self.issued_cgs_tree_bulk.item(item, 'values')[2]

            success, message = return_cg_db(cg_id, employee_id, condition_notes, user_id)
            if success:
                returned_count += 1
                self.db_manager.log_activity("C.G. returned (bulk)", f"C.G. '{cg_code if cg_code else 'N/A'}' ({cg_name}) returned by '{selected_employee_name}' (bulk operation). Notes: {condition_notes}")
            else:
                failed_returns.append(f"C.G. '{cg_code if cg_code else 'N/A'}' ({cg_name}): {message}")

        if returned_count > 0:
            success_message = f"Successfully returned {returned_count} C.G.s from '{selected_employee_name}'."
            if failed_returns:
                success_message += "\n\nHowever, some C.G.s could not be returned:\n" + "\n".join(failed_returns)
                messagebox.showwarning("Bulk Return Partial Success", success_message, icon="warning")
            else:
                messagebox.showinfo("Bulk Return Success", success_message, icon="info")
        else:
            messagebox.showerror("Bulk Return Failed", "No C.G.s were successfully returned.\n\n" + "\n".join(failed_returns), icon="error")

        self.parent.refresh_all_data()
        self.destroy()


class ExportTransactionLogDialog(ctk.CTkToplevel):
    def __init__(self, parent, db_manager, export_callback):
        super().__init__(parent)
        self.parent = parent
        self.db_manager = db_manager
        self.export_callback = export_callback

        self.title("Export Transaction Log")
        self.transient(parent)
        self.grab_set()
        self.resizable(False, False)

        parent_x = parent.winfo_x()
        parent_y = parent.winfo_y()
        parent_width = parent.winfo_width()
        parent_height = parent.winfo_height()

        self.update_idletasks()
        dialog_width = 450
        dialog_height = 400
        self.geometry(f"{dialog_width}x{dialog_height}+{parent_x + (parent_width - dialog_width) // 2}+{parent_y + (parent_height - dialog_height) // 2}")

        self.configure(fg_color=COLORS["card_background"], corner_radius=CORNER_RADIUS)

        main_frame = ctk.CTkFrame(self, fg_color="transparent")
        main_frame.pack(expand=True, fill="both", padx=20, pady=20)
        main_frame.grid_columnconfigure(0, weight=1)

        ctk.CTkLabel(main_frame, text="Select Export Period:", font=FONTS["heading_card"], text_color=COLORS["text_dark"]).grid(row=0, column=0, pady=(0, 15), sticky="w")

        periods = ["All Time", "Last 24 Hours", "Last 7 Days", "Last 30 Days", "Custom Range"]
        self.period_var = ctk.StringVar(value=periods[0])
        
        for i, period in enumerate(periods):
            radio_btn = ctk.CTkRadioButton(
                main_frame,
                text=period,
                variable=self.period_var,
                value=period,
                font=FONTS["body"],
                text_color=COLORS["text_dark"],
                fg_color=COLORS["primary_blue"],
                hover_color=COLORS["secondary_blue"],
                command=self._toggle_custom_date_inputs
            )
            radio_btn.grid(row=i + 1, column=0, padx=10, pady=5, sticky="w")

        self.custom_date_frame = ctk.CTkFrame(main_frame, fg_color="transparent")
        self.custom_date_frame.grid(row=len(periods) + 1, column=0, pady=(15, 0), sticky="ew")
        self.custom_date_frame.grid_columnconfigure(1, weight=1)

        ctk.CTkLabel(self.custom_date_frame, text="Start Date (YYYY-MM-DD):", font=FONTS["body"], text_color=COLORS["text_dark"]).grid(row=0, column=0, padx=10, pady=5, sticky="w")
        self.start_date_entry = ctk.CTkEntry(
            self.custom_date_frame,
            width=200,
            font=FONTS["body"],
            fg_color=COLORS["background_light"],
            text_color=COLORS["text_dark"],
            border_color=COLORS["border_subtle"],
            border_width=1,
            corner_radius=CORNER_RADIUS,
            placeholder_text="e.g., 2023-01-01"
        )
        self.start_date_entry.grid(row=0, column=1, padx=10, pady=5, sticky="ew")

        ctk.CTkLabel(self.custom_date_frame, text="End Date (YYYY-MM-DD):", font=FONTS["body"], text_color=COLORS["text_dark"]).grid(row=1, column=0, padx=10, pady=5, sticky="w")
        self.end_date_entry = ctk.CTkEntry(
            self.custom_date_frame,
            width=200,
            font=FONTS["body"],
            fg_color=COLORS["background_light"],
            text_color=COLORS["text_dark"],
            border_color=COLORS["border_subtle"],
            border_width=1,
            corner_radius=CORNER_RADIUS,
            placeholder_text="e.g., 2023-12-31"
        )
        self.end_date_entry.grid(row=1, column=1, padx=10, pady=5, sticky="ew")

        self._toggle_custom_date_inputs()

        export_btn = ctk.CTkButton(
            main_frame,
            text="Export Log",
            command=self._perform_export,
            height=45,
            font=FONTS["subheading"],
            text_color=COLORS["card_background"],
            corner_radius=CORNER_RADIUS,
            fg_color=COLORS["primary_blue"],
            hover_color=COLORS["secondary_blue"]
        )
        export_btn.grid(row=len(periods) + 2, column=0, pady=20, sticky="ew")

        self.wait_window(self)

    def _toggle_custom_date_inputs(self):
        if self.period_var.get() == "Custom Range":
            self.start_date_entry.configure(state="normal")
            self.end_date_entry.configure(state="normal")
        else:
            self.start_date_entry.configure(state="disabled")
            self.end_date_entry.configure(state="disabled")
            self.start_date_entry.delete(0, tk.END)
            self.end_date_entry.delete(0, tk.END)

    def _validate_date(self, date_str):
        if not date_str:
            return None
        try:
            date_obj = datetime.strptime(date_str, "%Y-%m-%d")
            return date_obj
        except ValueError:
            return None

    def _perform_export(self):
        period = self.period_var.get()
        start_date = None
        end_date = None
        current_time = datetime.now()

        if period == "Last 24 Hours":
            start_date = current_time - timedelta(hours=24)
            end_date = current_time
        elif period == "Last 7 Days":
            start_date = current_time - timedelta(days=7)
            end_date = current_time
        elif period == "Last 30 Days":
            start_date = current_time - timedelta(days=30)
            end_date = current_time
        elif period == "Custom Range":
            start_date_str = self.start_date_entry.get().strip()
            end_date_str = self.end_date_entry.get().strip()
            
            start_date = self._validate_date(start_date_str)
            end_date = self._validate_date(end_date_str)

            if not start_date and start_date_str:
                messagebox.showerror("Input Error", "Invalid Start Date format. Use YYYY-MM-DD.", icon="warning")
                return
            if not end_date and end_date_str:
                messagebox.showerror("Input Error", "Invalid End Date format. Use YYYY-MM-DD.", icon="warning")
                return
            
            if start_date and not end_date:
                end_date = start_date + timedelta(days=1) - timedelta(seconds=1)
            elif end_date and not start_date:
                start_date = end_date
            
            if start_date and end_date and start_date > end_date:
                messagebox.showerror("Input Error", "Start Date cannot be after End Date.", icon="warning")
                return
            
            if end_date and not self.end_date_entry.get().strip().endswith(" "):
                end_date = end_date.replace(hour=23, minute=59, second=59, microsecond=999999)

        self.export_callback(start_date, end_date)
        self.destroy()


class CGManagementApp(ctk.CTk):
    def __init__(self):
        super().__init__()

        self.db_manager = DatabaseManager()

        self.title("Nihar Technocrafts - Capital Goods Management System")
        
        app_width = 1400
        app_height = 850

        screen_width = self.winfo_screenwidth()
        screen_height = self.winfo_screenheight()

        x = (screen_width // 2) - (app_width // 2)
        y = (screen_height // 2) - (app_height // 2)

        self.geometry(f"{app_width}x{app_height}+{x}+{y}")
        
        self.update() 
        self.state('zoomed') 

        self.minsize(1200, 750) 
        self.configure(fg_color=COLORS["background_light"]) 

        self.grid_columnconfigure(1, weight=1)
        self.grid_rowconfigure(0, weight=1)

        self.current_user = None
        self.current_time_strvar = ctk.StringVar() 
        self.update_current_time_display() 
        self.current_user_strvar = ctk.StringVar(value="Not Logged In") 
        self.current_role_strvar = ctk.StringVar(value="") 

        self.main_content = ctk.CTkFrame(self, corner_radius=0, fg_color=COLORS["background_light"])
        self.main_content.grid(row=0, column=1, sticky="nsew")
        self.main_content.grid_columnconfigure(0, weight=1)
        self.main_content.grid_rowconfigure(0, weight=1)

        self.all_cgs_tree = None 
        self.cg_search_entry = None 
        self.cg_category_filter_combobox = None 

        self.current_allocations_tree = None
        self.current_allocations_employee_filter_combobox = None
        self.current_allocations_category_filter_combobox = None

        self.cg_transaction_log_tree = None 
        
        self.total_cgs_value_label = None 
        self.issued_cgs_value_label = None 
        self.available_cgs_value_label = None 

        self.current_time_value_label = None
        self.login_frame_instance = None 
        self.activity_tree = None 
        
        self.logged_in_user_label = None 
        self.logged_in_role_label = None 
        self.user_management_dashboard_section = None 
        self.activity_section_frame = None 
        
        self.frames = {} 
        self._create_main_content_frames() 

        self._build_dashboard_ui(self.frames["dashboard"])
        self._build_all_cgs_ui(self.frames["all_cgs"]) 
        self._build_current_allocations_ui(self.frames["current_allocations"])
        self._build_cg_transaction_log_ui(self.frames["cg_transaction_log"]) 

        self._configure_treeview_style() 

        self.create_sidebar_widgets()
        
        self.show_login_frame()
        self.protocol("WM_DELETE_WINDOW", self.logout_and_exit)

    def update_current_time_display(self):
        self.current_time_strvar.set(datetime.now().strftime("%d-%m-%y %H:%M:%S")) 
        self.after(1000, self.update_current_time_display)

    def create_sidebar_widgets(self):
        self.sidebar = ctk.CTkFrame(self, width=250, corner_radius=0, fg_color=COLORS["primary_blue"])
        self.sidebar.grid(row=0, column=0, sticky="nsew")
        
        self.sidebar.grid_rowconfigure(9, weight=1) 

        self.sidebar_top_section = ctk.CTkFrame(self.sidebar, fg_color=COLORS["card_background"], corner_radius=0)
        self.sidebar_top_section.grid(row=0, column=0, sticky="new", padx=0, pady=0)
        self.sidebar_top_section.grid_columnconfigure(0, weight=1)

        logo_path = resource_path("logo.png")
        if os.path.exists(logo_path):
            try:
                logo_image_pil = Image.open(logo_path)
                self.logo_ctk_image = ctk.CTkImage(light_image=logo_image_pil, dark_image=logo_image_pil, size=(100, 100))
                logo_label = ctk.CTkLabel(self.sidebar_top_section, image=self.logo_ctk_image, text="")
                logo_label.grid(row=0, column=0, padx=20, pady=(30, 10))
            except Exception as e:
                print(f"Error loading logo: {e}. Using text fallback.")
                ctk.CTkLabel(
                    self.sidebar_top_section,
                    text="<LOGO>",
                    font=FONTS["heading_main"],
                    text_color=COLORS["primary_blue"]
                ).grid(row=0, column=0, padx=20, pady=(30, 10))
        else:
            ctk.CTkLabel(
                self.sidebar_top_section,
                text="<LOGO>",
                font=FONTS["heading_main"],
                text_color=COLORS["primary_blue"]
            ).grid(row=0, column=0, padx=20, pady=(30, 10))

        company_label = ctk.CTkLabel(
            self.sidebar_top_section,
            text="NIHAR TECH.",
            font=FONTS["heading_main"],
            text_color=COLORS["primary_blue"]
        )
        company_label.grid(row=1, column=0, padx=20, pady=(0, 50))
        self.sidebar_top_section.grid_rowconfigure(2, weight=1)

        button_common_args = {
            "fg_color": "transparent",
            "hover_color": COLORS["secondary_blue"],
            "anchor": "w",
            "height": 55,
            "font": FONTS["subheading"],
            "text_color": COLORS["card_background"],
            "corner_radius": CORNER_RADIUS
        }

        self.dashboard_btn = ctk.CTkButton(
            self.sidebar, text=" Dashboard", command=lambda: self.show_frame("dashboard"), **button_common_args
        )
        self.all_cgs_btn = ctk.CTkButton( 
            self.sidebar, text=" All C.Gs", command=lambda: self.show_frame("all_cgs"), **button_common_args
        )
        self.current_allocations_btn = ctk.CTkButton(
            self.sidebar, text=" Current Allocations", command=lambda: self.show_frame("current_allocations"), **button_common_args
        )
        self.cg_transaction_log_btn = ctk.CTkButton( 
            self.sidebar, text=" C.G. Transaction Log", command=lambda: self.show_frame("cg_transaction_log"), **button_common_args
        )

        self.logout_btn = ctk.CTkButton(
            self.sidebar, text="Logout", command=self.logout,
            fg_color=COLORS["accent_error"], hover_color=COLORS["text_dark"],
            height=55, font=FONTS["subheading"], text_color=COLORS["card_background"],
            corner_radius=CORNER_RADIUS
        )

    def _create_main_content_frames(self):
        self.dashboard_frame = ctk.CTkFrame(self.main_content, fg_color=COLORS["background_light"])
        self.all_cgs_frame = ctk.CTkFrame(self.main_content, fg_color=COLORS["background_light"]) 
        self.current_allocations_frame = ctk.CTkFrame(self.main_content, fg_color=COLORS["background_light"])
        self.cg_transaction_log_frame = ctk.CTkFrame(self.main_content, fg_color=COLORS["background_light"]) 

        self.frames["dashboard"] = self.dashboard_frame
        self.frames["all_cgs"] = self.all_cgs_frame 
        self.frames["current_allocations"] = self.current_allocations_frame
        self.frames["cg_transaction_log"] = self.cg_transaction_log_frame 

        for frame in self.frames.values():
            frame.grid(row=0, column=0, sticky="nsew")
            frame.grid_remove() 

    def logout(self):
        if self.current_user:
            self.db_manager.log_activity("logout", f"User {self.current_user['username']} logged out")

        self.current_user = None
        self.db_manager.current_user = None
        self.current_user_strvar.set("Not Logged In")
        self.current_role_strvar.set("") 
        self.show_login_frame()

    def logout_and_exit(self):
        self.logout()
        self.destroy()
        sys.exit()

    def show_login_frame(self):
        for frame in self.frames.values():
            frame.grid_remove()

        if self.login_frame_instance and self.login_frame_instance.winfo_exists():
            self.login_frame_instance.destroy()

        self.dashboard_btn.grid_forget()
        self.all_cgs_btn.grid_forget() 
        self.current_allocations_btn.grid_forget()
        self.cg_transaction_log_btn.grid_forget() 
        self.logout_btn.grid_forget()

        self.login_frame_instance = ctk.CTkFrame(
            self.main_content,
            fg_color=COLORS["card_background"],
            corner_radius=CORNER_RADIUS,
            border_width=1,
            border_color=COLORS["border_subtle"]
        )
        self.login_frame_instance.pack(expand=True, padx=70, pady=70)

        title_label = ctk.CTkLabel(
            self.login_frame_instance,
            text="NTPL Capital Goods Management",
            font=FONTS["title"],
            text_color=COLORS["primary_blue"],
            wraplength=350
        )
        title_label.pack(pady=(40, 50))

        username_label = ctk.CTkLabel(self.login_frame_instance, text="Username:", text_color=COLORS["text_dark"], font=FONTS["subheading"])
        username_label.pack(anchor="w", padx=60)
        self.username_entry = ctk.CTkEntry(
            self.login_frame_instance,
            width=350,
            height=50,
            font=FONTS["body"],
            fg_color=COLORS["background_light"],
            text_color=COLORS["text_dark"],
            placeholder_text_color=COLORS["text_light"],
            border_color=COLORS["border_subtle"],
            border_width=1,
            corner_radius=CORNER_RADIUS
        )
        self.username_entry.pack(pady=(10, 30), padx=60)

        password_label = ctk.CTkLabel(self.login_frame_instance, text="Password:", text_color=COLORS["text_dark"], font=FONTS["subheading"])
        password_label.pack(anchor="w", padx=60)
        self.password_entry = ctk.CTkEntry(
            self.login_frame_instance,
            width=350,
            height=50,
            show="*",
            font=FONTS["body"],
            fg_color=COLORS["background_light"],
            text_color=COLORS["text_dark"],
            placeholder_text_color=COLORS["text_light"],
            border_color=COLORS["border_subtle"],
            border_width=1,
            corner_radius=CORNER_RADIUS
        )
        self.password_entry.pack(pady=(10, 50), padx=60)

        login_btn = ctk.CTkButton(
            self.login_frame_instance,
            text="Login",
            command=self.authenticate,
            width=250,
            height=55,
            font=FONTS["heading_card"],
            text_color=COLORS["card_background"],
            corner_radius=CORNER_RADIUS,
            fg_color=COLORS["primary_blue"],
            hover_color=COLORS["secondary_blue"],
            border_width=1,
            border_color=COLORS["secondary_blue"]
        )
        login_btn.pack(pady=(0, 40))

        self.password_entry.bind('<Return>', lambda event: self.authenticate())

    def authenticate(self):
        username = self.username_entry.get()
        password = self.password_entry.get()

        if not username or not password:
            messagebox.showerror("Authentication Error", "Please enter both username and password.", icon="warning")
            return

        conn = sqlite3.connect(DB_NAME) 
        cursor = conn.cursor()

        cursor.execute("SELECT id, username, role FROM users WHERE username=? AND password=?", (username, password))
        user = cursor.fetchone()
        conn.close()

        if user:
            self.current_user = {
                "id": user[0],
                "username": user[1],
                "role": user[2]
            }
            self.db_manager.current_user = self.current_user
            
            self.current_user_strvar.set(self.current_user["username"])
            self.current_role_strvar.set(f"({self.current_user['role'].capitalize()})")

            self.db_manager.check_and_perform_daily_backups()

            self.db_manager.log_activity("login", f"User {username} logged in")
            self.login_frame_instance.destroy() 
            self.show_main_interface()
        else:
            messagebox.showerror("Authentication Failed", "Invalid username or password.", icon="error")
            self.username_entry.delete(0, ctk.END)
            self.password_entry.delete(0, ctk.END)

    def show_main_interface(self):
        self.dashboard_btn.grid(row=2, column=0, padx=20, pady=(15, 8), sticky="ew")
        self.all_cgs_btn.grid(row=3, column=0, padx=20, pady=8, sticky="ew") 
        self.current_allocations_btn.grid(row=4, column=0, padx=20, pady=8, sticky="ew")
        self.cg_transaction_log_btn.grid(row=5, column=0, padx=20, pady=8, sticky="ew") 

        self.logout_btn.grid(row=6, column=0, padx=20, pady=(30, 20), sticky="ew") 

        self.show_frame("dashboard")

    def show_frame(self, frame_name):
        for frame in self.frames.values():
            frame.grid_remove() 

        target_frame = self.frames.get(frame_name)
        if target_frame:
            target_frame.grid(row=0, column=0, sticky="nsew", padx=30, pady=30)
            
            if frame_name == "dashboard":
                self._refresh_dashboard_data()
            elif frame_name == "all_cgs": 
                self.populate_all_cgs_treeview() 
                self.populate_category_filter_combobox() 
            elif frame_name == "current_allocations":
                self.populate_current_allocations_treeview()
                self.populate_current_allocations_filters()
            elif frame_name == "cg_transaction_log": 
                self.populate_cg_transaction_log_treeview() 
        else:
            print(f"Error: Frame '{frame_name}' not found.")

    def _build_dashboard_ui(self, dashboard_frame):
        dashboard_frame.grid_columnconfigure(0, weight=1, uniform="col_uniform")
        dashboard_frame.grid_rowconfigure(3, weight=1) 

        title_label = ctk.CTkLabel(
            dashboard_frame,
            text="Dashboard Overview",
            font=FONTS["heading_main"],
            text_color=COLORS["text_dark"],
            anchor="w",
            wraplength=800
        )
        title_label.grid(row=0, column=0, columnspan=2, pady=(0, 30), sticky="ew")

        stats_frame = ctk.CTkFrame(dashboard_frame, fg_color="transparent")
        stats_frame.grid(row=1, column=0, columnspan=2, sticky="ew", pady=(0, 30))
        stats_frame.grid_columnconfigure((0, 1, 2, 3, 4), weight=1, uniform="stat_col") 

        self.total_cgs_value_label = self._create_stat_card(stats_frame, "Total C.Gs", 0, 0, 0) 
        self.issued_cgs_value_label = self._create_stat_card(stats_frame, "Issued C.Gs", 0, 0, 1, "issued_count") 
        self.available_cgs_value_label = self._create_stat_card(stats_frame, "Available C.Gs", 0, 0, 2, "available_count") 
        self.current_time_value_label = self._create_stat_card(stats_frame, "Current Time", "", 0, 3, is_time=True)
        self.current_time_value_label.configure(textvariable=self.current_time_strvar)

        logged_in_as_frame = ctk.CTkFrame(
            stats_frame,
            fg_color=COLORS["card_background"],
            corner_radius=CORNER_RADIUS,
            height=130, 
            border_width=1,
            border_color=COLORS["border_subtle"]
        )
        logged_in_as_frame.grid(row=0, column=4, padx=15, pady=10, sticky="nsew") 
        logged_in_as_frame.pack_propagate(False) 

        ctk.CTkLabel(
            logged_in_as_frame,
            text="Logged in as:",
            font=FONTS["subheading"],
            text_color=COLORS["text_light"],
            justify="center"
        ).pack(pady=(20, 5))

        self.logged_in_user_label = ctk.CTkLabel(
            logged_in_as_frame,
            textvariable=self.current_user_strvar, 
            font=FONTS["heading_card"],
            text_color=COLORS["text_dark"],
            justify="center"
        )
        self.logged_in_user_label.pack()

        self.logged_in_role_label = ctk.CTkLabel(
            logged_in_as_frame,
            textvariable=self.current_role_strvar, 
            font=FONTS["body"],
            text_color=COLORS["text_light"],
            justify="center"
        )
        self.logged_in_role_label.pack(pady=(0, 20))


        self.activity_section_frame = ctk.CTkFrame( 
            dashboard_frame,
            fg_color=COLORS["card_background"],
            corner_radius=CORNER_RADIUS,
            border_width=1,
            border_color=COLORS["border_subtle"]
        )
        self.activity_section_frame.grid(row=2, column=0, columnspan=2, sticky="nsew", padx=15, pady=(20, 10))
        self.activity_section_frame.grid_columnconfigure(0, weight=1)
        self.activity_section_frame.grid_rowconfigure(1, weight=1) 

        activity_header_frame = ctk.CTkFrame(self.activity_section_frame, fg_color="transparent")
        activity_header_frame.grid(row=0, column=0, sticky="ew", padx=25, pady=(20, 15))
        activity_header_frame.grid_columnconfigure(0, weight=1)
        activity_header_frame.grid_columnconfigure(1, weight=0) 
        activity_header_frame.grid_columnconfigure(2, weight=0) 

        ctk.CTkLabel(
            activity_header_frame,
            text="Recent Activity Log",
            font=FONTS["heading_card"],
            text_color=COLORS["text_dark"],
            anchor="w"
        ).grid(row=0, column=0, sticky="w")

        download_logs_btn = ctk.CTkButton(
            activity_header_frame,
            text="Download Logs",
            command=self.export_activity_log, 
            fg_color=COLORS["secondary_blue"],
            hover_color=COLORS["primary_blue"],
            font=FONTS["subheading"],
            height=40,
            corner_radius=CORNER_RADIUS
        )
        download_logs_btn.grid(row=0, column=1, sticky="e", padx=(15, 0))

        self.dashboard_user_management_btn = ctk.CTkButton(
            activity_header_frame, 
            text="Manage Users",
            command=lambda: self._toggle_user_management_section(show_users=True), 
            fg_color=COLORS["primary_blue"],
            hover_color=COLORS["secondary_blue"],
            font=FONTS["subheading"],
            height=40,
            corner_radius=CORNER_RADIUS
        )
        self.dashboard_user_management_btn.grid(row=0, column=2, sticky="e", padx=(15, 0))
        self.dashboard_user_management_btn.grid_remove() 


        activity_table_frame = ctk.CTkFrame(self.activity_section_frame, fg_color=COLORS["card_background"])
        activity_table_frame.grid(row=1, column=0, sticky="nsew", padx=25, pady=(0, 25))
        activity_table_frame.grid_columnconfigure(0, weight=1)
        activity_table_frame.grid_rowconfigure(0, weight=1)
        
        self.activity_tree = ttk.Treeview(
            activity_table_frame,
            columns=("user", "action", "details", "timestamp"),
            show="headings",
            selectmode="browse"
        )

        self.activity_tree.heading("user", text="User")
        self.activity_tree.heading("action", text="Action")
        self.activity_tree.heading("details", text="Details")
        self.activity_tree.heading("timestamp", text="Timestamp")

        self.activity_tree.column("user", width=120, anchor="center")
        self.activity_tree.column("action", width=120, anchor="center")
        self.activity_tree.column("details", width=450, anchor="w")
        self.activity_tree.column("timestamp", width=200, anchor="center")

        scrollbar = ttk.Scrollbar(activity_table_frame, orient="vertical", command=self.activity_tree.yview, style="Vertical.TScrollbar")
        self.activity_tree.configure(yscrollcommand=scrollbar.set)
        scrollbar.pack(side="right", fill="y", padx=(0,5))
        self.activity_tree.pack(fill="both", expand=True, padx=(5,0), pady=5)

        self.activity_tree.tag_configure("evenrow", background=COLORS["card_background"], foreground=COLORS["text_dark"])
        self.activity_tree.tag_configure("oddrow", background=COLORS["zebra_stripe"], foreground=COLORS["text_dark"])

        self.user_management_dashboard_section = ctk.CTkFrame(
            dashboard_frame,
            fg_color=COLORS["background_light"] 
        )
        self.user_management_dashboard_section.grid(row=2, column=0, columnspan=2, sticky="nsew", padx=15, pady=(20, 10))
        self.user_management_dashboard_section.grid_columnconfigure(0, weight=1)
        self.user_management_dashboard_section.grid_rowconfigure(0, weight=1)
        
        self._build_user_management_ui(self.user_management_dashboard_section)
        
        self.user_management_dashboard_section.grid_remove() 


    def _toggle_user_management_section(self, show_users=None):
        if show_users is True: 
            self.activity_section_frame.grid_remove()
            self.user_management_dashboard_section.grid()
            self.load_users_data() 

        elif show_users is False: 
            self.user_management_dashboard_section.grid_remove()
            self.activity_section_frame.grid()
            self._refresh_dashboard_data() 

        else:
            if self.user_management_dashboard_section.winfo_ismapped():
                self._toggle_user_management_section(show_users=False) 
            else:
                self._toggle_user_management_section(show_users=True) 


    def _create_stat_card(self, parent, text, initial_value, row, column, color_value=None, is_time=False):
        card = ctk.CTkFrame(
            parent,
            fg_color=COLORS["card_background"],
            corner_radius=CORNER_RADIUS,
            height=130, 
            border_width=1,
            border_color=COLORS["border_subtle"]
        )
        card.grid(row=row, column=column, padx=15, pady=10, sticky="nsew")
        card.pack_propagate(False) 

        ctk.CTkLabel(
            card,
            text=text,
            font=FONTS["subheading"],
            text_color=COLORS["text_light"],
            justify="center",
            wraplength=card._current_width - 40 
        ).pack(pady=(20, 10))

        value_color = COLORS["text_dark"]
        if color_value == "issued_count":
            value_color = COLORS["accent_warning"] if initial_value > 0 else COLORS["accent_success"]
        elif color_value == "available_count":
            value_color = COLORS["accent_success"] if initial_value > 0 else COLORS["accent_error"]

        value_label = ctk.CTkLabel(
            card,
            text=str(initial_value), 
            font=FONTS["title"] if not is_time else FONTS["heading_card"],
            text_color=value_color,
            justify="center"
        )
        value_label.pack(pady=(0, 20))
        return value_label 

    def _refresh_dashboard_data(self):
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()

        cursor.execute("SELECT COUNT(*) FROM CapitalGoods") 
        total_cgs = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM CapitalGoods WHERE current_status = 'Issued'") 
        issued_cgs = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM CapitalGoods WHERE current_status = 'Available'") 
        available_cgs = cursor.fetchone()[0]
        conn.close()

        if self.total_cgs_value_label: 
            self.total_cgs_value_label.configure(text=str(total_cgs))
        if self.issued_cgs_value_label: 
            self.issued_cgs_value_label.configure(text=str(issued_cgs),
                                                      text_color=COLORS["accent_warning"] if issued_cgs > 0 else COLORS["accent_success"])
        if self.available_cgs_value_label: 
            self.available_cgs_value_label.configure(text=str(available_cgs),
                                                        text_color=COLORS["accent_success"] if available_cgs > 0 else COLORS["accent_error"])
        
        if self.activity_tree and self.activity_tree.winfo_exists():
            for item in self.activity_tree.get_children():
                self.activity_tree.delete(item)

            conn = sqlite3.connect(DB_NAME)
            cursor = conn.cursor()

            cursor.execute('''
                SELECT u.username, a.action, a.details, a.timestamp
                FROM activity_log a
                JOIN users u ON a.user_id = u.id
                ORDER BY a.timestamp DESC
                LIMIT 50
            ''')
            
            for i, row in enumerate(cursor.fetchall()):
                tag = "evenrow" if i % 2 == 0 else "oddrow"
                display_timestamp = ""
                try:
                    db_timestamp = datetime.strptime(row[3], "%Y-%m-%d %H:%M:%S")
                    display_timestamp = db_timestamp.strftime("%d-%m-%y %H:%M:%S")
                except ValueError:
                    display_timestamp = row[3]

                self.activity_tree.insert("", "end", values=(row[0], row[1], row[2], display_timestamp), tags=(tag,))
            conn.close()

        if self.current_user and self.current_user["role"] == "admin":
            self.dashboard_user_management_btn.grid() 
            if self.user_management_dashboard_section.winfo_ismapped():
                self.load_users_data() 
                self.activity_section_frame.grid_remove() 
            else: 
                self.activity_section_frame.grid()
        else:
            self.dashboard_user_management_btn.grid_remove() 
            self.user_management_dashboard_section.grid_remove() 
            self.activity_section_frame.grid() 


    def _configure_treeview_style(self):
        style = ttk.Style()
        style.theme_use("clam")
        
        style.configure("Treeview.Heading",
                        font=FONTS["subheading"],
                        background=COLORS["primary_blue"],
                        foreground=COLORS["card_background"],
                        padding=(10, 10, 10, 10),
                        relief="flat"
                        )
        style.map("Treeview.Heading",
                  background=[('active', COLORS["secondary_blue"])])

        style.configure("Treeview",
                        font=FONTS["body"], 
                        rowheight=35,
                        background=COLORS["card_background"],
                        foreground=COLORS["text_dark"],
                        fieldbackground=COLORS["card_background"],
                        bordercolor=COLORS["card_background"], 
                        borderwidth=0,
                        relief="flat"
                        )
        style.map("Treeview",
                  background=[('selected', COLORS["secondary_blue"])],
                  foreground=[('selected', COLORS["card_background"])]
                  )

        style.configure("Vertical.TScrollbar",
                        background=COLORS["border_subtle"],
                        troughcolor=COLORS["background_light"],
                        bordercolor=COLORS["border_subtle"],
                        arrowcolor=COLORS["text_light"],
                        gripcount=0,
                        gripcolor=COLORS["tertiary_blue"]
                        )
        style.map("Vertical.TScrollbar",
                  background=[('active', COLORS["tertiary_blue"])],
                  gripcolor=[('active', COLORS["primary_blue"])])


    def export_activity_log(self):
        file_path = filedialog.asksaveasfilename(
            defaultextension=".xlsx",
            filetypes=[("Excel files", "*.xlsx"), ("PDF files", "*.pdf")]
        )
        if not file_path:
            return

        try:
            conn = sqlite3.connect(DB_NAME)
            cursor = conn.cursor()

            cursor.execute('''
                SELECT u.username, a.action, a.details, a.timestamp
                FROM activity_log a
                JOIN users u ON a.user_id = u.id
                ORDER BY a.timestamp DESC
            ''')
            data = cursor.fetchall()
            conn.close()

            formatted_data = []
            for row in data:
                user, action, details, timestamp_str = row
                
                formatted_timestamp = ""
                try:
                    formatted_timestamp = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S").strftime("%d-%m-%y %H:%M:%S")
                except ValueError:
                    formatted_timestamp = timestamp_str 

                formatted_data.append((user, action, details, formatted_timestamp))


            columns = ["User", "Action", "Details", "Timestamp"]
            df = pd.DataFrame(formatted_data, columns=columns)

            if file_path.endswith('.xlsx'):
                df.to_excel(file_path, index=False, engine='openynpxl')
                messagebox.showinfo("Export Success", f"Activity log exported to Excel:\n{file_path}", icon="info")
            elif file_path.endswith('.pdf'):
                doc = SimpleDocTemplate(file_path, pagesize=A4,
                                        leftMargin=50, rightMargin=50,
                                        topMargin=50, bottomMargin=50)
                styles = getSampleStyleSheet()
                elements = []

                title_style = ParagraphStyle(
                    'TitleStyle',
                    parent=styles['h1'],
                    fontName='Helvetica-Bold', 
                    fontSize=24,
                    leading=28,
                    alignment=TA_CENTER,
                    textColor=colors.HexColor(COLORS["primary_blue"])
                )
                elements.append(Paragraph("General Activity Log Summary", title_style))
                elements.append(Spacer(1, 0.4 * A4[1]))
                
                table_headers = ["User", "Action Type", "Details", "Timestamp"]
                table_data = [table_headers] + formatted_data 

                table_style = TableStyle([
                    ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor(COLORS["primary_blue"])),
                    ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                    ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                    ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                    ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                    ('FONTSIZE', (0, 0), (-1, 0), 10),
                    ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                    ('BACKGROUND', (0, 1), (-1, -1), colors.HexColor(COLORS["card_background"])),
                    ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor(COLORS["border_subtle"])),
                    ('LEFTPADDING', (0, 0), (-1, -1), 8),
                    ('RIGHTPADDING', (0, 0), (-1, -1), 8),
                    ('TOPPADDING', (0, 0), (-1, -1), 8),
                    ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
                    ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.HexColor(COLORS["card_background"]), colors.HexColor(COLORS["zebra_stripe"])]),
                ])

                content_width = A4[0] - 60
                col_widths = [doc.width * 0.15, doc.width * 0.15, doc.width * 0.45, doc.width * 0.25]
                table = Table(table_data, colWidths=col_widths)
                table.setStyle(table_style)
                elements.append(table)
                doc.build(elements)
                messagebox.showinfo("Export Success", f"Activity log exported to PDF:\n{file_path}", icon="info")
        except Exception as e:
            messagebox.showerror("Export Error", f"Failed to export: {e}", icon="error")

    def _build_all_cgs_ui(self, all_cgs_frame):
        title_label = ctk.CTkLabel(
            all_cgs_frame,
            text="All Capital Goods",
            font=FONTS["heading_main"],
            text_color=COLORS["text_dark"],
            anchor="w",
            wraplength=800
        )
        title_label.pack(pady=(0, 30), padx=10, fill="x")

        filter_register_frame = ctk.CTkFrame(all_cgs_frame, fg_color="transparent")
        filter_register_frame.pack(pady=(0, 15), padx=10, fill="x")
        filter_register_frame.grid_columnconfigure((0,1,2,3,4,5), weight=1) 

        ctk.CTkLabel(filter_register_frame, text="Search C.G. (Code/Name):", font=FONTS["body"], text_color=COLORS["text_dark"]).grid(row=0, column=0, padx=5, pady=5, sticky="w")
        self.cg_search_entry = ctk.CTkEntry(
            filter_register_frame, width=200, font=FONTS["body"],
            fg_color=COLORS["background_light"], text_color=COLORS["text_dark"],
            border_color=COLORS["border_subtle"], border_width=1, corner_radius=CORNER_RADIUS,
            placeholder_text="Enter code or name"
        )
        self.cg_search_entry.grid(row=0, column=1, padx=5, pady=5, sticky="ew")
        self.cg_search_entry.bind("<KeyRelease>", self.filter_all_cgs_by_search) 

        ctk.CTkLabel(filter_register_frame, text="Filter by Category:", font=FONTS["body"], text_color=COLORS["text_dark"]).grid(row=1, column=0, padx=5, pady=5, sticky="w")
        self.cg_category_filter_combobox = ctk.CTkComboBox(
            filter_register_frame, state="readonly", width=200, font=FONTS["body"],
            fg_color=COLORS["background_light"], text_color=COLORS["text_dark"],
            dropdown_fg_color=COLORS["card_background"], dropdown_text_color=COLORS["text_dark"],
            dropdown_hover_color=COLORS["hover_light"], button_color=COLORS["primary_blue"],
            button_hover_color=COLORS["secondary_blue"], border_color=COLORS["border_subtle"],
            border_width=1, corner_radius=CORNER_RADIUS,
            command=self.filter_all_cgs_by_category
        )
        self.cg_category_filter_combobox.grid(row=1, column=1, padx=5, pady=5, sticky="ew")

        register_cg_btn = ctk.CTkButton(
            filter_register_frame,
            text="Register New C.G.",
            command=self.show_register_cg_dialog,
            height=40,
            width=180,
            font=FONTS["subheading"],
            text_color=COLORS["card_background"],
            corner_radius=CORNER_RADIUS,
            fg_color=COLORS["primary_blue"],
            hover_color=COLORS["secondary_blue"]
        )
        register_cg_btn.grid(row=0, column=2, padx=5, pady=5, sticky="ew")

        issue_multiple_btn = ctk.CTkButton(
            filter_register_frame,
            text="Issue Multiple C.G.s",
            command=self.show_bulk_issue_dialog,
            height=40,
            width=180,
            font=FONTS["subheading"],
            text_color=COLORS["card_background"],
            corner_radius=CORNER_RADIUS,
            fg_color=COLORS["primary_blue"],
            hover_color=COLORS["secondary_blue"]
        )
        issue_multiple_btn.grid(row=0, column=3, padx=5, pady=5, sticky="ew")

        return_multiple_btn = ctk.CTkButton(
            filter_register_frame,
            text="Return Multiple C.G.s",
            command=self.show_bulk_return_dialog,
            height=40,
            width=180,
            font=FONTS["subheading"],
            text_color=COLORS["card_background"],
            corner_radius=CORNER_RADIUS,
            fg_color=COLORS["accent_success"],
            hover_color=COLORS["primary_blue"]
        )
        return_multiple_btn.grid(row=0, column=4, padx=5, pady=5, sticky="ew")

        add_delete_category_btn = ctk.CTkButton(
            filter_register_frame,
            text="Add / Delete Category",
            command=self.show_add_delete_category_dialog,
            height=40,
            width=180,
            font=FONTS["subheading"],
            text_color=COLORS["card_background"],
            corner_radius=CORNER_RADIUS,
            fg_color=COLORS["secondary_blue"],
            hover_color=COLORS["primary_blue"]
        )
        add_delete_category_btn.grid(row=1, column=2, padx=5, pady=5, sticky="ew")

        add_remove_employee_btn = ctk.CTkButton(
            filter_register_frame,
            text="Add / Remove Employee",
            command=self.show_add_remove_employee_dialog,
            height=40,
            width=180,
            font=FONTS["subheading"],
            text_color=COLORS["card_background"],
            corner_radius=CORNER_RADIUS,
            fg_color=COLORS["secondary_blue"],
            hover_color=COLORS["primary_blue"]
        )
        add_remove_employee_btn.grid(row=1, column=3, padx=5, pady=5, sticky="ew")

        export_db_btn = ctk.CTkButton(
            filter_register_frame,
            text="Export Database",
            command=self.db_manager.export_full_database_to_excel,
            height=40,
            width=180,
            font=FONTS["subheading"],
            text_color=COLORS["card_background"],
            corner_radius=CORNER_RADIUS,
            fg_color=COLORS["secondary_blue"],
            hover_color=COLORS["primary_blue"]
        )
        export_db_btn.grid(row=1, column=4, padx=5, pady=5, sticky="ew")

        table_frame = ctk.CTkFrame(
            all_cgs_frame,
            fg_color=COLORS["card_background"],
            corner_radius=CORNER_RADIUS,
            border_width=1,
            border_color=COLORS["border_subtle"]
        )
        table_frame.pack(expand=True, fill="both", padx=10, pady=(10, 10))
        table_frame.grid_columnconfigure(0, weight=1)
        table_frame.grid_rowconfigure(0, weight=1)

        self.all_cgs_tree = ttk.Treeview(table_frame, columns=( 
            "ID", "C.G. Code", "C.G. Name", "Description", "Category", "Current Status", "Acquisition Date"
        ), show="headings")

        self.all_cgs_tree.heading("ID", text="ID")
        self.all_cgs_tree.heading("C.G. Code", text="C.G. Code") 
        self.all_cgs_tree.heading("C.G. Name", text="C.G. Name") 
        self.all_cgs_tree.heading("Description", text="Description")
        self.all_cgs_tree.heading("Category", text="Category") 
        self.all_cgs_tree.heading("Current Status", text="Current Status")
        self.all_cgs_tree.heading("Acquisition Date", text="Acquisition Date")

        self.all_cgs_tree.column("ID", width=0, stretch=tk.NO) 
        self.all_cgs_tree.column("C.G. Code", width=120, anchor=tk.CENTER) 
        self.all_cgs_tree.column("C.G. Name", width=180) 
        self.all_cgs_tree.column("Description", width=250) 
        self.all_cgs_tree.column("Category", width=120, anchor=tk.CENTER) 
        self.all_cgs_tree.column("Current Status", width=130, anchor=tk.CENTER) 
        self.all_cgs_tree.column("Acquisition Date", width=160, anchor=tk.CENTER) 

        scrollbar = ttk.Scrollbar(table_frame, orient="vertical", command=self.all_cgs_tree.yview, style="Vertical.TScrollbar") 
        self.all_cgs_tree.configure(yscrollcommand=scrollbar.set)
        
        self.all_cgs_tree.grid(row=0, column=0, sticky="nsew", padx=(5,0), pady=5) 
        scrollbar.grid(row=0, column=1, sticky="ns", padx=(0,5), pady=5)
        
        self.all_cgs_tree.tag_configure("evenrow", background=COLORS["card_background"], foreground=COLORS["text_dark"])
        self.all_cgs_tree.tag_configure("oddrow", background=COLORS["zebra_stripe"], foreground=COLORS["text_dark"])

        self.all_cgs_tree.bind("<Double-1>", self.on_double_click_cg)
        self.all_cgs_tree.bind("<Button-3>", self._on_right_click_cg_table)


        action_buttons_frame = ctk.CTkFrame(all_cgs_frame, fg_color="transparent")
        action_buttons_frame.pack(pady=(15, 10), padx=10, fill="x", anchor="e")
        action_buttons_frame.grid_columnconfigure((0,1,2), weight=1)

        issue_cg_btn = ctk.CTkButton(
            action_buttons_frame,
            text="Issue Selected C.G.",
            command=self.issue_selected_cg,
            height=40,
            font=FONTS["subheading"],
            text_color=COLORS["card_background"],
            corner_radius=CORNER_RADIUS,
            fg_color=COLORS["primary_blue"],
            hover_color=COLORS["secondary_blue"]
        )
        issue_cg_btn.grid(row=0, column=0, padx=5, sticky="ew")

        return_cg_btn = ctk.CTkButton(
            action_buttons_frame,
            text="Return Selected C.G.",
            command=self.return_selected_cg,
            height=40,
            font=FONTS["subheading"],
            text_color=COLORS["card_background"],
            corner_radius=CORNER_RADIUS,
            fg_color=COLORS["accent_success"],
            hover_color=COLORS["secondary_blue"]
        )
        return_cg_btn.grid(row=0, column=1, padx=5, sticky="ew")

        delete_cg_btn = ctk.CTkButton(
            action_buttons_frame,
            text="Delete Selected C.G.",
            command=self.delete_selected_cg,
            height=40,
            font=FONTS["subheading"],
            text_color=COLORS["card_background"],
            corner_radius=CORNER_RADIUS,
            fg_color=COLORS["accent_error"],
            hover_color=COLORS["text_dark"]
        )
        delete_cg_btn.grid(row=0, column=2, padx=5, sticky="ew")


    def populate_all_cgs_treeview(self, search_term="", category_id=None):
        if self.all_cgs_tree is None or not self.all_cgs_tree.winfo_exists():
            return
        for i in self.all_cgs_tree.get_children():
            self.all_cgs_tree.delete(i)
        
        cgs = get_all_cgs_db(search_term, category_id) 
        for i, cg in enumerate(cgs):
            tag = "evenrow" if i % 2 == 0 else "oddrow"
            display_cg_code = cg['cg_code'] if cg['cg_code'] else "N/A"
            self.all_cgs_tree.insert("", "end", values=(
                cg['cg_id'], 
                display_cg_code,
                cg['cg_name'],
                cg['description'],
                cg['category_name'] if cg['category_name'] else "Uncategorized", 
                cg['current_status'],
                cg['acquisition_date']
            ), tags=(tag,))

    def filter_all_cgs_by_search(self, event=None):
        search_term = self.cg_search_entry.get().strip()
        selected_category_name = self.cg_category_filter_combobox.get()
        category_id = self.category_name_to_id.get(selected_category_name) if selected_category_name and selected_category_name != "All Categories" else None
        self.populate_all_cgs_treeview(search_term=search_term, category_id=category_id)

    def populate_category_filter_combobox(self):
        categories = get_all_categories_db()
        self.category_name_to_id = {"All Categories": None}
        for cat in categories:
            self.category_name_to_id[cat['category_name']] = cat['category_id']
        
        self.cg_category_filter_combobox.configure(values=list(self.category_name_to_id.keys()))
        self.cg_category_filter_combobox.set("All Categories")

    def filter_all_cgs_by_category(self, selected_category_name):
        search_term = self.cg_search_entry.get().strip()
        category_id = self.category_name_to_id.get(selected_category_name)
        self.populate_all_cgs_treeview(search_term=search_term, category_id=category_id)

    def show_register_cg_dialog(self):
        RegisterCGDialog(self, self.db_manager)

    def show_add_delete_category_dialog(self):
        AddDeleteCategoryDialog(self, self.db_manager)

    def show_add_remove_employee_dialog(self):
        AddRemoveEmployeeDialog(self, self.db_manager)

    def show_bulk_issue_dialog(self):
        BulkIssueCGDialog(self, self.db_manager)

    def show_bulk_return_dialog(self):
        BulkReturnCGDialog(self, self.db_manager)

    def _on_right_click_cg_table(self, event):
        item = self.all_cgs_tree.identify_row(event.y)
        if item:
            self.all_cgs_tree.selection_set(item)
            self.all_cgs_tree.focus(item)

            context_menu = tk.Menu(self, tearoff=0)
            context_menu.add_command(label="Edit C.G.", command=self._edit_selected_cg)
            context_menu.add_command(label="Delete C.G.", command=self.delete_selected_cg)
            
            try:
                context_menu.tk_popup(event.x_root, event.y_root)
            finally:
                context_menu.grab_release()

    def _edit_selected_cg(self):
        selected_item = self.all_cgs_tree.focus()
        if not selected_item:
            messagebox.showerror("Selection Error", "Please select a C.G. to edit.", icon="warning")
            return
        
        item_data = self.all_cgs_tree.item(selected_item)
        cg_id = item_data['values'][0]
        
        EditCGDialog(self, self.db_manager, cg_id)

    def issue_selected_cg(self):
        selected_item = self.all_cgs_tree.focus()
        if not selected_item:
            messagebox.showerror("Selection Error", "Please select a C.G. to issue.", icon="warning")
            return
        
        item_data = self.all_cgs_tree.item(selected_item)
        cg_id = item_data['values'][0]
        cg_code = item_data['values'][1]
        cg_name = item_data['values'][2]
        current_status = item_data['values'][5]

        if current_status != 'Available':
            messagebox.showerror("Status Error", f"C.G. '{cg_code if cg_code else 'N/A'}' is not Available for issue. Current status: {current_status}.", icon="warning")
            return

        IssueReturnCGDialog(self, cg_id, cg_code, cg_name, current_status, self.db_manager, is_issue_action=True)

    def return_selected_cg(self):
        selected_item = self.all_cgs_tree.focus()
        if not selected_item:
            messagebox.showerror("Selection Error", "Please select a C.G. to return.", icon="warning")
            return
        
        item_data = self.all_cgs_tree.item(selected_item)
        cg_id = item_data['values'][0]
        cg_code = item_data['values'][1]
        cg_name = item_data['values'][2]
        current_status = item_data['values'][5]

        if current_status != 'Issued':
            messagebox.showerror("Status Error", f"C.G. '{cg_code if cg_code else 'N/A'}' is not currently Issued. Current status: {current_status}.", icon="warning")
            return

        last_issued_employee_id = get_last_issued_employee_id_for_cg(cg_id)
        
        IssueReturnCGDialog(self, cg_id, cg_code, cg_name, current_status, self.db_manager, is_issue_action=False, initial_employee_id=last_issued_employee_id)

    def on_double_click_cg(self, event):
        item = self.all_cgs_tree.identify_row(event.y)
        if not item:
            return

        self.all_cgs_tree.selection_set(item)
        item_data = self.all_cgs_tree.item(item)
        cg_id = item_data['values'][0]
        cg_code = item_data['values'][1]
        cg_name = item_data['values'][2]
        current_status = item_data['values'][5]

        if current_status == 'Available':
            self.issue_selected_cg()
        elif current_status == 'Issued':
            self.return_selected_cg()
        else:
            messagebox.showinfo("Status Info", f"C.G. '{cg_code if cg_code else 'N/A'}' is '{current_status}'. Cannot issue or return.", icon="info")


    def delete_selected_cg(self):
        selected_item = self.all_cgs_tree.focus()
        if not selected_item:
            messagebox.showerror("Selection Error", "Please select a C.G. to delete.", icon="warning")
            return
        
        item_data = self.all_cgs_tree.item(selected_item)
        cg_id = item_data['values'][0] 
        cg_code = item_data['values'][1] 

        self.show_custom_confirm_dialog(
            title="Confirm C.G. Deletion",
            message=f"Are you sure you want to delete C.G.:\n'{cg_code}'?\n\nThis action cannot be undone and will delete all associated transactions.",
            on_yes=lambda: self._perform_delete_cg(cg_id, cg_code)
        )

    def _perform_delete_cg(self, cg_id, cg_code):
        success, message = delete_cg_db(cg_id, cg_code)
        self.show_message(message, is_error=not success)
        if success:
            self.db_manager.log_activity("cg_deleted", f"Deleted C.G.: {cg_code} (ID: {cg_id})")
            self.refresh_all_data()

    def _build_current_allocations_ui(self, current_allocations_frame):
        title_label = ctk.CTkLabel(
            current_allocations_frame,
            text="Currently Issued C.Gs", 
            font=FONTS["heading_main"],
            text_color=COLORS["text_dark"],
            anchor="w",
            wraplength=800
        )
        title_label.pack(pady=(0, 30), padx=10, fill="x")

        filter_frame = ctk.CTkFrame(current_allocations_frame, fg_color="transparent")
        filter_frame.pack(pady=(0, 15), padx=10, fill="x")
        filter_frame.grid_columnconfigure((0, 1, 2, 3), weight=1)

        ctk.CTkLabel(filter_frame, text="Filter by Employee:", font=FONTS["body"], text_color=COLORS["text_dark"]).grid(row=0, column=0, padx=5, pady=5, sticky="w")
        self.current_allocations_employee_filter_combobox = ctk.CTkComboBox(
            filter_frame, state="readonly", width=200, font=FONTS["body"],
            fg_color=COLORS["background_light"], text_color=COLORS["text_dark"],
            dropdown_fg_color=COLORS["card_background"], dropdown_text_color=COLORS["text_dark"],
            dropdown_hover_color=COLORS["hover_light"], button_color=COLORS["primary_blue"],
            button_hover_color=COLORS["secondary_blue"], border_color=COLORS["border_subtle"],
            border_width=1, corner_radius=CORNER_RADIUS,
            command=self.filter_current_allocations
        )
        self.current_allocations_employee_filter_combobox.grid(row=0, column=1, padx=5, pady=5, sticky="ew")

        ctk.CTkLabel(filter_frame, text="Filter by Category:", font=FONTS["body"], text_color=COLORS["text_dark"]).grid(row=1, column=0, padx=5, pady=5, sticky="w")
        self.current_allocations_category_filter_combobox = ctk.CTkComboBox(
            filter_frame, state="readonly", width=200, font=FONTS["body"],
            fg_color=COLORS["background_light"], text_color=COLORS["text_dark"],
            dropdown_fg_color=COLORS["card_background"], dropdown_text_color=COLORS["text_dark"],
            dropdown_hover_color=COLORS["hover_light"], button_color=COLORS["primary_blue"],
            button_hover_color=COLORS["secondary_blue"], border_color=COLORS["border_subtle"],
            border_width=1, corner_radius=CORNER_RADIUS,
            command=self.filter_current_allocations
        )
        self.current_allocations_category_filter_combobox.grid(row=1, column=1, padx=5, pady=5, sticky="ew")


        table_frame = ctk.CTkFrame(
            current_allocations_frame,
            fg_color=COLORS["card_background"],
            corner_radius=CORNER_RADIUS,
            border_width=1,
            border_color=COLORS["border_subtle"]
        )
        table_frame.pack(expand=True, fill="both", padx=10, pady=(10, 10))
        table_frame.grid_columnconfigure(0, weight=1)
        table_frame.grid_rowconfigure(0, weight=1)

        self.current_allocations_tree = ttk.Treeview(table_frame, columns=(
            "C.G. Code", "C.G. Name", "Category", "Issued To Employee", "Issue Timestamp" 
        ), show="headings")

        self.current_allocations_tree.heading("C.G. Code", text="C.G. Code") 
        self.current_allocations_tree.heading("C.G. Name", text="C.G. Name") 
        self.current_allocations_tree.heading("Category", text="Category")
        self.current_allocations_tree.heading("Issued To Employee", text="Issued To Employee") 
        self.current_allocations_tree.heading("Issue Timestamp", text="Issue Timestamp")

        self.current_allocations_tree.column("C.G. Code", width=120, anchor=tk.CENTER) 
        self.current_allocations_tree.column("C.G. Name", width=180) 
        self.current_allocations_tree.column("Category", width=120, anchor=tk.CENTER)
        self.current_allocations_tree.column("Issued To Employee", width=180) 
        self.current_allocations_tree.column("Issue Timestamp", width=160, anchor=tk.CENTER) 

        scrollbar = ttk.Scrollbar(table_frame, orient="vertical", command=self.current_allocations_tree.yview, style="Vertical.TScrollbar")
        self.current_allocations_tree.configure(yscrollcommand=scrollbar.set)
        
        self.current_allocations_tree.grid(row=0, column=0, sticky="nsew", padx=(5,0), pady=5)
        scrollbar.grid(row=0, column=1, sticky="ns", padx=(0,5), pady=5)
        
        self.current_allocations_tree.tag_configure("evenrow", background=COLORS["card_background"], foreground=COLORS["text_dark"])
        self.current_allocations_tree.tag_configure("oddrow", background=COLORS["zebra_stripe"], foreground=COLORS["text_dark"])

    def populate_current_allocations_filters(self):
        employees = get_all_employees_db()
        self.current_allocations_employee_data = {"All Employees": None}
        for emp in employees:
            self.current_allocations_employee_data[emp['employee_name']] = emp['employee_id']

        self.current_allocations_employee_filter_combobox.configure(values=list(self.current_allocations_employee_data.keys()))
        self.current_allocations_employee_filter_combobox.set("All Employees")

        categories = get_all_categories_db()
        self.current_allocations_category_name_to_id = {"All Categories": None}
        for cat in categories:
            self.current_allocations_category_name_to_id[cat['category_name']] = cat['category_id']
        
        self.current_allocations_category_filter_combobox.configure(values=list(self.current_allocations_category_name_to_id.keys()))
        self.current_allocations_category_filter_combobox.set("All Categories")


    def populate_current_allocations_treeview(self):
        if self.current_allocations_tree is None or not self.current_allocations_tree.winfo_exists():
            return

        for i in self.current_allocations_tree.get_children():
            self.current_allocations_tree.delete(i)
        
        selected_employee_name = self.current_allocations_employee_filter_combobox.get() if self.current_allocations_employee_filter_combobox.winfo_exists() else "All Employees"
        selected_employee_id = self.current_allocations_employee_data.get(selected_employee_name) if hasattr(self, 'current_allocations_employee_data') else None

        selected_category_name = self.current_allocations_category_filter_combobox.get() if self.current_allocations_category_filter_combobox.winfo_exists() else "All Categories"
        category_id = self.current_allocations_category_name_to_id.get(selected_category_name) if hasattr(self, 'current_allocations_category_name_to_id') else None

        allocations = get_current_cg_allocations_db(selected_employee_id, category_id) 
        for i, alloc in enumerate(allocations):
            tag = "evenrow" if i % 2 == 0 else "oddrow"
            display_cg_code = alloc['cg_code'] if alloc['cg_code'] else "N/A" 
            self.current_allocations_tree.insert("", "end", values=(
                display_cg_code,
                alloc['cg_name'], 
                alloc['category_name'] if alloc['category_name'] else 'Uncategorized',
                alloc['employee_name'] if alloc['employee_name'] else 'N/A', 
                alloc['issue_timestamp']
            ), tags=(tag,))

    def filter_current_allocations(self, event=None):
        self.populate_current_allocations_treeview()

    def _build_cg_transaction_log_ui(self, cg_transaction_log_frame): 
        title_label = ctk.CTkLabel(
            cg_transaction_log_frame,
            text="C.G. Transaction Log", 
            font=FONTS["heading_main"],
            text_color=COLORS["text_dark"],
            anchor="w",
            wraplength=800
        )
        title_label.pack(pady=(0, 30), padx=10, fill="x")

        table_frame = ctk.CTkFrame(
            cg_transaction_log_frame,
            fg_color=COLORS["card_background"],
            corner_radius=CORNER_RADIUS,
            border_width=1,
            border_color=COLORS["border_subtle"]
        )
        table_frame.pack(expand=True, fill="both", padx=10, pady=(10, 10))
        table_frame.grid_columnconfigure(0, weight=1)
        table_frame.grid_rowconfigure(0, weight=1)

        self.cg_transaction_log_tree = ttk.Treeview(table_frame, columns=( 
            "Timestamp", "C.G. Code", "C.G. Name", "Employee", "Type", "Condition Notes", "Logged By" 
        ), show="headings")

        self.cg_transaction_log_tree.heading("Timestamp", text="Timestamp")
        self.cg_transaction_log_tree.heading("C.G. Code", text="C.G. Code") 
        self.cg_transaction_log_tree.heading("C.G. Name", text="C.G. Name") 
        self.cg_transaction_log_tree.heading("Employee", text="Employee") 
        self.cg_transaction_log_tree.heading("Type", text="Transaction Type")
        self.cg_transaction_log_tree.heading("Condition Notes", text="Condition Notes")
        self.cg_transaction_log_tree.heading("Logged By", text="Logged By")

        self.cg_transaction_log_tree.column("Timestamp", width=160, anchor=tk.CENTER) 
        self.cg_transaction_log_tree.column("C.G. Code", width=120, anchor=tk.CENTER) 
        self.cg_transaction_log_tree.column("C.G. Name", width=180) 
        self.cg_transaction_log_tree.column("Employee", width=150) 
        self.cg_transaction_log_tree.column("Type", width=120, anchor=tk.CENTER) 
        self.cg_transaction_log_tree.column("Condition Notes", width=250) 
        self.cg_transaction_log_tree.column("Logged By", width=120, anchor=tk.CENTER) 

        scrollbar = ttk.Scrollbar(table_frame, orient="vertical", command=self.cg_transaction_log_tree.yview, style="Vertical.TScrollbar") 
        self.cg_transaction_log_tree.configure(yscrollcommand=scrollbar.set)
        
        self.cg_transaction_log_tree.grid(row=0, column=0, sticky="nsew", padx=(5,0), pady=5) 
        scrollbar.grid(row=0, column=1, sticky="ns", padx=(0,5), pady=5)
        
        self.cg_transaction_log_tree.tag_configure("evenrow", background=COLORS["card_background"], foreground=COLORS["text_dark"])
        self.cg_transaction_log_tree.tag_configure("oddrow", background=COLORS["zebra_stripe"], foreground=COLORS["text_dark"])

        export_cg_log_btn = ctk.CTkButton( 
            cg_transaction_log_frame,
            text="Export C.G. Log", 
            command=self.export_cg_transaction_log, 
            height=40,
            font=FONTS["subheading"],
            text_color=COLORS["card_background"],
            corner_radius=CORNER_RADIUS,
            fg_color=COLORS["secondary_blue"],
            hover_color=COLORS["primary_blue"]
        )
        export_cg_log_btn.pack(pady=(15, 10), padx=10, anchor="e")

    def populate_cg_transaction_log_treeview(self): 
        if self.cg_transaction_log_tree is None or not self.cg_transaction_log_tree.winfo_exists(): 
            return
        for i in self.cg_transaction_log_tree.get_children(): 
            self.cg_transaction_log_tree.delete(i) 
        log_entries = get_cg_transaction_log_db() 
        for i, log in enumerate(log_entries):
            tag = "evenrow" if i % 2 == 0 else "oddrow"
            display_cg_code = log['cg_code'] if log['cg_code'] else "N/A" 
            self.cg_transaction_log_tree.insert("", "end", values=( 
                log['timestamp'],
                display_cg_code,
                log['cg_name'], 
                log['employee_name'] if log['employee_name'] else 'N/A', 
                log['transaction_type'],
                log['condition_notes'] if log['condition_notes'] else 'N/A',
                log['logged_by_username'] if log['logged_by_username'] else 'N/A'
            ), tags=(tag,))

    def export_cg_transaction_log(self): 
            ExportTransactionLogDialog(self, self.db_manager, self._perform_export_cg_log_with_dates)

    def _perform_export_cg_log_with_dates(self, start_date, end_date):
        file_path = filedialog.asksaveasfilename(
            defaultextension=".xlsx",
            filetypes=[("Excel files", "*.xlsx"), ("PDF files", "*.pdf")]
        )
        if not file_path:
            return

        try:
            data = get_cg_transaction_log_db(start_date=start_date, end_date=end_date) 

            formatted_data = []
            for row in data:
                timestamp_str, cg_code, cg_name, employee_name, tx_type, notes, logged_by = (
                    row['timestamp'], row['cg_code'], row['cg_name'], row['employee_name'],
                    row['transaction_type'], row['condition_notes'], row['logged_by_username']
                )
                
                formatted_timestamp = ""
                try:
                    formatted_timestamp = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S").strftime("%d-%m-%y %H:%M:%S")
                except ValueError:
                    formatted_timestamp = timestamp_str 

                formatted_data.append((formatted_timestamp, 
                                       cg_code if cg_code else 'N/A', 
                                       cg_name, 
                                       employee_name if employee_name else 'N/A', 
                                       tx_type, 
                                       notes if notes else 'N/A', 
                                       logged_by if logged_by else 'N/A'))


            columns = ["Timestamp", "C.G. Code", "C.G. Name", "Employee", "Type", "Condition Notes", "Logged By"] 
            df = pd.DataFrame(formatted_data, columns=columns)

            if file_path.endswith('.xlsx'):
                df.to_excel(file_path, index=False, engine='openpyxl')
                messagebox.showinfo("Export Success", f"C.G. log exported to Excel:\n{file_path}", icon="info") 
            elif file_path.endswith('.pdf'):
                doc = SimpleDocTemplate(file_path, pagesize=A4,
                                        leftMargin=30, rightMargin=30,
                                        topMargin=50, bottomMargin=50)
                styles = getSampleStyleSheet()
                elements = []

                title_style = ParagraphStyle(
                    'TitleStyle',
                    parent=styles['h1'],
                    fontName='Helvetica-Bold', 
                    fontSize=24,
                    leading=28,
                    alignment=TA_CENTER,
                    textColor=colors.HexColor(COLORS["primary_blue"])
                )
                elements.append(Paragraph("Capital Goods Transaction Log Report", title_style)) 
                elements.append(Spacer(1, 0.4 * A4[1]) )
                
                table_headers = ["Timestamp", "C.G. Code", "C.G. Name", "Employee", "Type", "Condition Notes", "Logged By"] 
                table_data = [table_headers] + formatted_data 

                table_style = TableStyle([
                    ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor(COLORS["primary_blue"])),
                    ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                    ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                    ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                    ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                    ('FONTSIZE', (0, 0), (-1, 0), 8),
                    ('BOTTOMPADDING', (0, 0), (-1, 0), 8),
                    ('BACKGROUND', (0, 1), (-1, -1), colors.HexColor(COLORS["card_background"])),
                    ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor(COLORS["border_subtle"])),
                    ('LEFTPADDING', (0, 0), (-1, -1), 4),
                    ('RIGHTPADDING', (0, 0), (-1, -1), 4),
                    ('TOPPADDING', (0, 0), (-1, -1), 4),
                    ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
                    ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.HexColor(COLORS["card_background"]), colors.HexColor(COLORS["zebra_stripe"])]),
                ])

                content_width = A4[0] - 60
                col_widths = [
                    content_width * 0.15, 
                    content_width * 0.10, 
                    content_width * 0.15, 
                    content_width * 0.10, 
                    content_width * 0.10, 
                    content_width * 0.20, 
                    content_width * 0.10, 
                ]
                
                table = Table(table_data, colWidths=col_widths)
                table.setStyle(table_style)
                elements.append(table)
                doc.build(elements)
                messagebox.showinfo("Export Success", f"C.G. log exported to PDF:\n{file_path}", icon="info") 
        except Exception as e:
            messagebox.showerror("Export Error", f"Failed to export: {e}", icon="error")

    def _build_user_management_ui(self, user_management_frame):
        user_management_frame.grid_columnconfigure(0, weight=1)
        user_management_frame.grid_rowconfigure(2, weight=1)

        header_frame = ctk.CTkFrame(user_management_frame, fg_color="transparent")
        header_frame.grid(row=0, column=0, columnspan=2, pady=(0, 30), sticky="ew")
        header_frame.grid_columnconfigure(0, weight=1)

        ctk.CTkLabel(
            header_frame,
            text="User Management",
            font=FONTS["heading_main"],
            text_color=COLORS["text_dark"],
            anchor="w",
            wraplength=800
        ).grid(row=0, column=0, sticky="ew")

        back_btn = ctk.CTkButton(
            header_frame,
            text="< Back to Activity Log",
            command=lambda: self._toggle_user_management_section(show_users=False),
            fg_color=COLORS["secondary_blue"],
            hover_color=COLORS["primary_blue"],
            font=FONTS["subheading"],
            height=35,
            corner_radius=CORNER_RADIUS
        )
        back_btn.grid(row=0, column=1, sticky="e", padx=(15,0))


        button_frame = ctk.CTkFrame(
            user_management_frame,
            fg_color=COLORS["card_background"],
            corner_radius=CORNER_RADIUS,
            border_width=1,
            border_color=COLORS["border_subtle"]
        )
        button_frame.grid(row=1, column=0, columnspan=2, sticky="ew", padx=15, pady=(0, 20))
        button_frame.grid_columnconfigure((0,1,2,3), weight=1) 

        add_user_btn = ctk.CTkButton(
            button_frame,
            text="Add New User",
            command=self.show_add_user_dialog,
            height=50,
            font=FONTS["subheading"],
            text_color=COLORS["card_background"],
            corner_radius=CORNER_RADIUS,
            fg_color=COLORS["primary_blue"],
            hover_color=COLORS["secondary_blue"],
            border_width=1,
            border_color=COLORS["secondary_blue"]
        )
        add_user_btn.grid(row=0, column=0, padx=20, pady=20, sticky="ew")

        edit_user_btn = ctk.CTkButton( 
            button_frame,
            text="Edit Selected User",
            command=self.show_edit_user_dialog,
            height=50,
            font=FONTS["subheading"],
            text_color=COLORS["card_background"],
            corner_radius=CORNER_RADIUS,
            fg_color=COLORS["secondary_blue"],
            hover_color=COLORS["primary_blue"],
            border_width=1,
            border_color=COLORS["secondary_blue"]
        )
        edit_user_btn.grid(row=0, column=1, padx=20, pady=20, sticky="ew")

        delete_user_btn = ctk.CTkButton(
            button_frame,
            text="Delete Selected User",
            command=self.delete_selected_user,
            height=50,
            font=FONTS["subheading"],
            text_color=COLORS["card_background"],
            corner_radius=CORNER_RADIUS,
            fg_color=COLORS["accent_error"],
            hover_color=COLORS["text_dark"]
        )
        delete_user_btn.grid(row=0, column=2, padx=20, pady=20, sticky="ew")
        
        if self.current_user and self.current_user["role"] == "admin":
            delete_all_data_btn = ctk.CTkButton(
                button_frame,
                text="🚨 DELETE ALL APPLICATION DATA ?",
                command=self._show_delete_all_data_dialog,
                height=50,
                font=FONTS["subheading"],
                text_color=COLORS["card_background"],
                corner_radius=CORNER_RADIUS,
                fg_color=COLORS["accent_error"],
                hover_color=COLORS["text_dark"]
            )
            delete_all_data_btn.grid(row=0, column=3, padx=20, pady=20, sticky="ew")


        table_frame = ctk.CTkFrame(
            user_management_frame,
            fg_color=COLORS["card_background"],
            corner_radius=CORNER_RADIUS,
            border_width=1,
            border_color=COLORS["border_subtle"]
        )
        table_frame.grid(row=2, column=0, columnspan=2, sticky="nsew", padx=15, pady=(10, 10))
        table_frame.grid_columnconfigure(0, weight=1)
        table_frame.grid_rowconfigure(0, weight=1)

        self.users_tree = ttk.Treeview(
            table_frame,
            columns=("id", "username", "role", "created"),
            show="headings",
            selectmode="browse"
        )

        self.users_tree.heading("id", text="ID")
        self.users_tree.heading("username", text="Username")
        self.users_tree.heading("role", text="Role")
        self.users_tree.heading("created", text="Created At")

        self.users_tree.column("id", width=50, anchor="center")
        self.users_tree.column("username", width=200, anchor="center")
        self.users_tree.column("role", width=150, anchor="center")
        self.users_tree.column("created", width=200, anchor="center")

        scrollbar = ttk.Scrollbar(table_frame, orient="vertical", command=self.users_tree.yview, style="Vertical.TScrollbar")
        self.users_tree.configure(yscrollcommand=scrollbar.set)
        scrollbar.pack(side="right", fill="y", padx=(0,5))
        self.users_tree.pack(fill="both", expand=True, padx=(5,0), pady=5)

        self.users_tree.tag_configure("evenrow", background=COLORS["card_background"], foreground=COLORS["text_dark"])
        self.users_tree.tag_configure("oddrow", background=COLORS["zebra_stripe"], foreground=COLORS["text_dark"])

    def show_add_user_dialog(self):
        add_window = ctk.CTkToplevel(self)
        add_window.title("Add New User")
        add_window.grab_set()
        add_window.resizable(False, False)
        add_window.configure(fg_color=COLORS["background_light"])
        add_window.transient(self)

        self.update_idletasks() 
        x = self.winfo_x() + (self.winfo_width() // 2) - (400 // 2)
        y = self.winfo_y() + (self.winfo_height() // 2) - (390 // 2)
        add_window.geometry(f"400x390+{x}+{y}")

        add_frame = ctk.CTkFrame(
            add_window,
            fg_color=COLORS["card_background"],
            corner_radius=CORNER_RADIUS,
            border_width=1,
            border_color=COLORS["border_subtle"]
        )
        add_frame.pack(padx=35, pady=35, fill="both", expand=True)

        ctk.CTkLabel(add_frame, text="Username:", font=FONTS["body"], text_color=COLORS["text_dark"], wraplength=260).pack(pady=(15, 5), anchor="w")
        username_entry = ctk.CTkEntry(
            add_frame,
            width=260,
            height=40,
            font=FONTS["body"],
            fg_color=COLORS["background_light"],
            text_color=COLORS["text_dark"],
            placeholder_text="Enter new username",
            border_color=COLORS["border_subtle"],
            border_width=1,
            corner_radius=CORNER_RADIUS
        )
        username_entry.pack(pady=(0, 10))

        ctk.CTkLabel(add_frame, text="Password:", font=FONTS["body"], text_color=COLORS["text_dark"], wraplength=260).pack(pady=(15, 5), anchor="w")
        password_entry = ctk.CTkEntry(
            add_frame,
            width=260,
            height=40,
            show="*",
            font=FONTS["body"],
            fg_color=COLORS["background_light"],
            text_color=COLORS["text_dark"],
            placeholder_text="Enter password",
            border_color=COLORS["border_subtle"],
            border_width=1,
            corner_radius=CORNER_RADIUS
        )
        password_entry.pack(pady=(0, 10))

        ctk.CTkLabel(add_frame, text="Confirm Password:", font=FONTS["body"], text_color=COLORS["text_dark"], wraplength=260).pack(pady=(15, 5), anchor="w")
        confirm_password_entry = ctk.CTkEntry(
            add_frame,
            width=260,
            height=40,
            show="*",
            font=FONTS["body"],
            fg_color=COLORS["background_light"],
            text_color=COLORS["text_dark"],
            placeholder_text="Confirm password",
            border_color=COLORS["border_subtle"],
            border_width=1,
            corner_radius=CORNER_RADIUS
        )
        confirm_password_entry.pack(pady=(0, 10))

        def add_user_logic():
            username = username_entry.get().strip()
            password = password_entry.get().strip()
            confirm_password = confirm_password_entry.get().strip()
            role = "user"

            if not username or not password or not confirm_password:
                messagebox.showerror("Input Error", "Please fill in all fields.", icon="warning")
                return

            if password != confirm_password:
                messagebox.showerror("Input Error", "Passwords do not match.", icon="warning")
                return

            try:
                conn = sqlite3.connect(DB_NAME) 
                cursor = conn.cursor()

                cursor.execute("SELECT COUNT(*) FROM users WHERE username=?", (username,))
                if cursor.fetchone()[0] > 0:
                    messagebox.showerror("Validation Error", "Username already exists. Please choose a different username.", icon="warning")
                    conn.close()
                    return

                cursor.execute(
                    "INSERT INTO users (username, password, role, created_at) VALUES (?, ?, ?, ?)",
                    (username, password, role, datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                )

                conn.commit()
                conn.close()

                self.db_manager.log_activity( 
                    "user_added",
                    f"Added new {role} user: {username}"
                )

                add_window.destroy()
                messagebox.showinfo("Success", "User added successfully.", icon="info")
                self.load_users_data()

            except sqlite3.Error as e:
                messagebox.showerror("Database Error", f"Failed to add user: {e}", icon="error")
            except Exception as e:
                messagebox.showerror("Error", f"An unexpected error occurred: {e}", icon="error")

        add_btn = ctk.CTkButton(
            add_frame,
            text="Add User",
            command=add_user_logic, 
            width=180,
            height=50,
            font=FONTS["heading_card"],
            text_color=COLORS["card_background"],
            corner_radius=CORNER_RADIUS,
            fg_color=COLORS["primary_blue"],
            hover_color=COLORS["secondary_blue"],
            border_width=1,
            border_color=COLORS["secondary_blue"]
        )
        add_btn.pack(pady=(20, 0))

    def show_edit_user_dialog(self):
        selected_item = self.users_tree.focus()
        if not selected_item:
            messagebox.showerror("Selection Error", "Please select a user to edit.", icon="warning")
            return
        
        item_data = self.users_tree.item(selected_item)
        user_id = item_data['values'][0]
        current_username = item_data['values'][1]
        
        edit_window = ctk.CTkToplevel(self)
        edit_window.title(f"Edit User: {current_username}")
        edit_window.grab_set()
        edit_window.resizable(False, False)
        edit_window.configure(fg_color=COLORS["background_light"])
        edit_window.transient(self)

        self.update_idletasks() 
        x = self.winfo_x() + (self.winfo_width() // 2) - (400 // 2)
        y = self.winfo_y() + (self.winfo_height() // 2) - (390 // 2)
        edit_window.geometry(f"400x390+{x}+{y}")

        edit_frame = ctk.CTkFrame(
            edit_window,
            fg_color=COLORS["card_background"],
            corner_radius=CORNER_RADIUS,
            border_width=1,
            border_color=COLORS["border_subtle"]
        )
        edit_frame.pack(padx=35, pady=35, fill="both", expand=True)

        ctk.CTkLabel(edit_frame, text="Username:", font=FONTS["body"], text_color=COLORS["text_dark"], wraplength=260).pack(pady=(15, 5), anchor="w")
        username_entry = ctk.CTkEntry(
            edit_frame,
            width=260,
            height=40,
            font=FONTS["body"],
            fg_color=COLORS["background_light"],
            text_color=COLORS["text_dark"],
            border_color=COLORS["border_subtle"],
            border_width=1,
            corner_radius=CORNER_RADIUS
        )
        username_entry.insert(0, current_username)
        username_entry.pack(pady=(0, 10))

        ctk.CTkLabel(edit_frame, text="New Password (leave blank to keep current):", font=FONTS["body"], text_color=COLORS["text_dark"], wraplength=260).pack(pady=(15, 5), anchor="w")
        password_entry = ctk.CTkEntry(
            edit_frame,
            width=260,
            height=40,
            show="*",
            font=FONTS["body"],
            fg_color=COLORS["background_light"],
            text_color=COLORS["text_dark"],
            placeholder_text="Enter new password",
            border_color=COLORS["border_subtle"],
            border_width=1,
            corner_radius=CORNER_RADIUS
        )
        password_entry.pack(pady=(0, 10))

        ctk.CTkLabel(edit_frame, text="Confirm New Password:", font=FONTS["body"], text_color=COLORS["text_dark"], wraplength=260).pack(pady=(15, 5), anchor="w")
        confirm_password_entry = ctk.CTkEntry(
            edit_frame,
            width=260,
            height=40,
            show="*",
            font=FONTS["body"],
            fg_color=COLORS["background_light"],
            text_color=COLORS["text_dark"],
            placeholder_text="Confirm new password",
            border_color=COLORS["border_subtle"],
            border_width=1,
            corner_radius=CORNER_RADIUS
        )
        confirm_password_entry.pack(pady=(0, 10))

        def edit_user_logic():
            new_username = username_entry.get().strip()
            new_password = password_entry.get().strip()
            confirm_new_password = confirm_password_entry.get().strip()

            if not new_username:
                messagebox.showerror("Input Error", "Username cannot be empty.", icon="warning")
                return
            
            if new_password:
                if new_password != confirm_new_password:
                    messagebox.showerror("Input Error", "New passwords do not match.", icon="warning")
                    return
                password_to_update = new_password
            else:
                conn = get_db_connection()
                cursor = conn.cursor()
                cursor.execute("SELECT password FROM users WHERE id = ?", (user_id,))
                current_password_db = cursor.fetchone()['password']
                conn.close()
                password_to_update = current_password_db


            success, message = update_user_credentials_db(user_id, new_username, password_to_update)
            self.show_message(message, is_error=not success)
            if success:
                self.db_manager.log_activity("user_updated", f"Updated user ID {user_id}: {new_username}")
                edit_window.destroy()
                self.load_users_data()

        edit_btn = ctk.CTkButton(
            edit_frame,
            text="Save Changes",
            command=edit_user_logic,
            width=180,
            height=50,
            font=FONTS["heading_card"],
            text_color=COLORS["card_background"],
            corner_radius=CORNER_RADIUS,
            fg_color=COLORS["primary_blue"],
            hover_color=COLORS["secondary_blue"],
            border_width=1,
            border_color=COLORS["secondary_blue"]
        )
        edit_btn.pack(pady=(20, 0))


    def delete_selected_user(self):
        selected_item = self.users_tree.focus()
        if not selected_item:
            messagebox.showerror("Selection Error", "Please select a user to delete.", icon="warning")
            return

        item_data = self.users_tree.item(selected_item)
        user_id = item_data['values'][0]
        username = item_data['values'][1]
        role = item_data['values'][2] 

        if self.current_user and user_id == self.current_user["id"]:
            messagebox.showerror("Deletion Error", "You cannot delete your own account.", icon="warning")
            return

        if role == "admin":
            try:
                conn = sqlite3.connect(DB_NAME) 
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM users WHERE role='admin'")
                admin_count = cursor.fetchone()[0]
                conn.close()

                if admin_count <= 1:
                    messagebox.showerror("Deletion Error", "Cannot delete the last admin user. At least one admin must remain.", icon="warning")
                    return
            except Exception as e:
                messagebox.showerror("Error", f"Failed to check admin count: {e}", icon="error")
                return

        self.show_custom_confirm_dialog(
            title="Confirm User Deletion",
            message=f"Are you sure you want to delete user:\n'{username}' (ID: {user_id})?\n\nThis action cannot be undone.",
            on_yes=lambda: self._perform_delete_user(user_id, username)
        )

    def _perform_delete_user(self, user_id, username):
        try:
            conn = sqlite3.connect(DB_NAME) 
            cursor = conn.cursor()

            cursor.execute("UPDATE CGTransactions SET logged_by_user_id = NULL WHERE logged_by_user_id = ?", (user_id,))
            cursor.execute("DELETE FROM activity_log WHERE user_id = ?", (user_id,))
            cursor.execute("DELETE FROM users WHERE id=?", (user_id,))
            conn.commit()
            conn.close()

            self.db_manager.log_activity( 
                "user_deleted",
                f"Deleted user: {username}"
            )

            messagebox.showinfo("Success", "User deleted successfully.", icon="info")
            self.load_users_data()


        except sqlite3.Error as e:
            messagebox.showerror("Database Error", f"Failed to delete user: {e}", icon="error")
        except Exception as e:
            messagebox.showerror("Error", f"An unexpected error occurred: {e}", icon="error")

    def load_users_data(self):
        try:
            if self.users_tree is None or not self.users_tree.winfo_exists():
                return 

            for item in self.users_tree.get_children():
                self.users_tree.delete(item)

            conn = sqlite3.connect(DB_NAME) 
            cursor = conn.cursor()
            cursor.execute("SELECT id, username, role, created_at FROM users ORDER BY username")
            
            for i, row in enumerate(cursor.fetchall()):
                tag = "evenrow" if i % 2 == 0 else "oddrow"
                display_created_at = ""
                try:
                    display_created_at = datetime.strptime(row[3], "%Y-%m-%d %H:%M:%S").strftime("%d-%m-%y %H:%M:%S")
                except ValueError:
                    display_created_at = row[3] 

                self.users_tree.insert("", "end", values=(row[0], row[1], row[2], display_created_at), tags=(tag,))
        except sqlite3.Error as e:
            messagebox.showerror("Database Error", f"Failed to load user data: {e}", icon="error")
        except Exception as e:
            messagebox.showerror("Error", f"An unexpected error occurred: {e}", icon="error")
        finally:
            if 'conn' in locals():
                conn.close()

    def _show_delete_all_data_dialog(self):
        delete_all_window = ctk.CTkToplevel(self)
        delete_all_window.title("Confirm Data Deletion")
        delete_all_window.grab_set()
        delete_all_window.resizable(False, False)
        delete_all_window.configure(fg_color=COLORS["card_background"])
        delete_all_window.transient(self)

        self.update_idletasks()
        x = self.winfo_x() + (self.winfo_width() // 2) - (450 // 2)
        y = self.winfo_y() + (self.winfo_height() // 2) - (250 // 2)
        delete_all_window.geometry(f"450x250+{x}+{y}")

        frame = ctk.CTkFrame(delete_all_window, fg_color="transparent")
        frame.pack(expand=True, fill="both", padx=20, pady=20)

        message_label = ctk.CTkLabel(
            frame,
            text="WARNING: This will permanently delete ALL data and backups.\nThis action cannot be undone.",
            font=FONTS["subheading"],
            text_color=COLORS["accent_error"],
            wraplength=400,
            justify="center"
        )
        message_label.pack(pady=(10, 20))

        password_label = ctk.CTkLabel(frame, text="Enter Password to Confirm:", font=FONTS["body"], text_color=COLORS["text_dark"])
        password_label.pack(anchor="w", padx=20)
        password_entry = ctk.CTkEntry(
            frame,
            width=300,
            height=40,
            show="*",
            font=FONTS["body"],
            fg_color=COLORS["background_light"],
            text_color=COLORS["text_dark"],
            border_color=COLORS["border_subtle"],
            border_width=1,
            corner_radius=CORNER_RADIUS
        )
        password_entry.pack(pady=(5, 20))

        def confirm_delete_all():
            entered_password = password_entry.get()
            if entered_password == DELETE_ALL_PASSWORD:
                delete_all_window.destroy()
                self._perform_delete_all_data()
            else:
                messagebox.showerror("Authentication Failed", "Incorrect password. Data not deleted.", icon="error")
                password_entry.delete(0, tk.END)

        confirm_btn = ctk.CTkButton(
            frame,
            text="Confirm Delete",
            command=confirm_delete_all,
            width=150,
            height=45,
            font=FONTS["subheading"],
            text_color=COLORS["card_background"],
            corner_radius=CORNER_RADIUS,
            fg_color=COLORS["accent_error"],
            hover_color=COLORS["text_dark"]
        )
        confirm_btn.pack(side="left", padx=(0, 10))

        cancel_btn = ctk.CTkButton(
            frame,
            text="Cancel",
            command=delete_all_window.destroy,
            width=150,
            height=45,
            font=FONTS["subheading"],
            text_color=COLORS["card_background"],
            corner_radius=CORNER_RADIUS,
            fg_color=COLORS["primary_blue"],
            hover_color=COLORS["secondary_blue"]
        )
        cancel_btn.pack(side="right", padx=(10, 0))

        delete_all_window.wait_window(delete_all_window)

    def _perform_delete_all_data(self):
        self.show_message("Deleting all data and backups. Please wait...", is_error=False) 

        try:
            conn = sqlite3.connect(DB_NAME)
            conn.close() 

            if os.path.exists(DB_NAME):
                os.remove(DB_NAME)
                print(f"Deleted database file: {DB_NAME}")

            if os.path.exists(BACKUP_DIR):
                shutil.rmtree(BACKUP_DIR)
                print(f"Deleted backup directory: {BACKUP_DIR}")
            
            create_new_tables() 
            print("Database re-initialized.")

            messagebox.showinfo("Data Deletion Complete", "All application data and backups have been permanently deleted.\nThe application will now restart.", icon="info")
            
            self.destroy() 
            sys.exit()

        except Exception as e:
            messagebox.showerror("Error Deleting Data", f"An error occurred during mass data deletion: {e}", icon="error")
            if not os.path.exists(DB_NAME):
                create_new_tables() 
            self.logout() 

    def show_custom_confirm_dialog(self, title, message, on_yes):
        dialog = CustomDialog(self, title, message, dialog_type="confirm", on_yes=on_yes)

    def show_message(self, message, is_error=False):
        dialog_type = "error" if is_error else "info"
        CustomDialog(self, "Error" if is_error else "Information", message, dialog_type=dialog_type)

    def refresh_all_data(self):
        self._refresh_dashboard_data()
        self.populate_all_cgs_treeview() 
        self.populate_category_filter_combobox() 
        self.populate_current_allocations_treeview() 
        self.populate_current_allocations_filters()
        self.populate_cg_transaction_log_treeview() 

if __name__ == '__main__':
    if not os.path.exists(DB_NAME):
        create_new_tables() 
        print("Database initialized with only default admin user.")
    else:
        pass

    app = CGManagementApp() 
    app.mainloop() 

    if not app.winfo_exists(): 
        print("Application window destroyed. Exiting.")
