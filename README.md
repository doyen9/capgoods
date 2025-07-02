Capital Goods Management System (CGMS)
A robust desktop application built with Python and CustomTkinter for managing capital goods (assets) within an organization. This system provides functionalities for tracking C.G. registration, allocation (issue/return), detailed transaction logs, employee management, category management, and data export. It uses SQLite for local data storage and includes automated backup features.


🚀 Getting Started
Follow these instructions to get a copy of the project up and running on your local machine.


Prerequisites

Python 3.8+
You can download Python from python.org.

Installation

Clone the repository:
https://github.com/doyen9/capgoods.git



(Replace YOUR_USERNAME and your-repo-name with your actual GitHub username and repository name.)
Install dependencies:


The application relies on several Python libraries. Install them using pip:

pip install Pillow==10.3.0 pandas==2.2.2 openpyxl==3.1.2 ReportLab==4.1.0 customtkinter==0.6.1

(Note: Specific versions are provided for stability. You can try newer versions, but ensure compatibility.)
🏃‍♀️ Usage
Run the application:
Navigate to the project root directory (where cgapp.py is located) in your terminal and run:
python cgapp.py


✨ Features
User Authentication & Roles: Secure login with admin and user roles. Admins have full access, including user management and data deletion.
Dashboard Overview: At-a-glance statistics on total, issued, and available capital goods.

Capital Goods Management:
First Create new categories.
Register new capital goods with unique codes (optional), names, descriptions, and categories.
Edit existing capital good details.
Issue capital goods to employees.
Return capital goods from employees, with condition notes.
Bulk issue and return of multiple capital goods.
Delete capital goods (only if not currently issued).
Search and filter capital goods by code, name, and category.
Category Management: Add and delete categories for organizing capital goods. Deleting a category automatically reassigns its associated C.G.s to an "Unassigned Category".
Employee Management: Add and remove employees. Removing an employee automatically returns any C.G.s issued to them.
Transaction Log: Comprehensive log of all C.G. issues, returns, and acquisitions, including timestamps and the user who logged the transaction.
Activity Log: Tracks all user actions within the application (login, logout, C.G. registration, updates, deletions, etc.).
Data Export:
Export the full database to an Excel file (.xlsx).
Export C.G. transaction logs to Excel (.xlsx) or PDF (.pdf), with options for custom date ranges.
Export the general activity log to Excel (.xlsx) or PDF (.pdf).
Automated Backups: Daily morning and afternoon backups of the SQLite database to a dedicated cg_backups directory, with a 30-day retention policy.
Data Deletion (Admin Only): A powerful, password-protected feature to permanently delete all application data and backups, re-initializing the database.
Modern UI: Built with CustomTkinter for a clean, professional, and responsive graphical user interface.


Login:
Upon first launch, the application will create a new SQLite database (cg_management.db) and a default admin user.
Username: admin
Password: admin123

(It is highly recommended to change the admin password after your first login for security reasons.)
Navigation:
Use the sidebar on the left to navigate between different sections:
Dashboard: View overall statistics and recent activity. Admins can also access User Management from here.
All C.Gs: Manage all capital goods (register, edit, issue, return, delete, search, filter).
Current Allocations: See which capital goods are currently issued to which employees.
C.G. Transaction Log: View a detailed history of all C.G. transactions.

🗄️ Database
The application uses an SQLite database named cg_management.db to store all its data. This file will be created automatically in the same directory as cgapp.py if it doesn't exist.
💾 Backups & Exports
Automated Backups: The system automatically creates daily backups of cg_management.db in a cg_backups directory. Backups are retained for 30 days.
Manual Export: You can export comprehensive reports (full database, transaction logs) to Excel (.xlsx) or PDF (.pdf) from the respective sections within the application.
👤 User Management
The application supports two user roles:
Admin: Full access to all features, including adding/editing/deleting users and performing the irreversible "Delete All Application Data" action.
User: Can manage C.G.s, employees, categories, and view logs, but cannot manage other users or delete all data.
Admins can manage users from the "User Management" section accessible via the Dashboard.

