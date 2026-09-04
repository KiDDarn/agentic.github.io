import gspread
from google.oauth2.service_account import Credentials

# Configuration
SERVICE_ACCOUNT_FILE = 'service_account.json'
USER_EMAIL = 'bahrabadikevin@gmail.com'
SHEET_TITLE = 'texas marketing agency leaads'

SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]

def create_and_share_sheet():
    try:
        print("Authenticating...")
        creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
        client = gspread.authorize(creds)

        print(f"Creating spreadsheet: '{SHEET_TITLE}'...")
        sh = client.create(SHEET_TITLE)
        
        print(f"Sharing with {USER_EMAIL}...")
        sh.share(USER_EMAIL, perm_type='user', role='writer')
        
        print(f"Success! Spreadsheet created and shared.")
        print(f"URL: {sh.url}")
        print(f"ID: {sh.id}")
        
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    create_and_share_sheet()
