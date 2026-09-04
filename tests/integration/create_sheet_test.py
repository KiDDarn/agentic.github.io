import os
import pytest

# Configuration
SERVICE_ACCOUNT_FILE = 'service_account.json'
USER_EMAIL = 'bahrabadikevin@gmail.com'
SHEET_TITLE = 'texas marketing agency leaads'

SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]


def create_and_share_sheet():
    import gspread
    from google.oauth2.service_account import Credentials

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
    return sh


def test_create_and_share_sheet():
    if not os.path.exists(SERVICE_ACCOUNT_FILE):
        pytest.skip(f"{SERVICE_ACCOUNT_FILE} not found; skipping Google Sheets integration test")

    try:
        import gspread
        from google.oauth2.service_account import Credentials
    except ImportError as e:
        pytest.skip(f"Integration dependencies not installed ({e}); skipping")

    sh = create_and_share_sheet()
    assert sh is not None
    assert getattr(sh, 'id', None) is not None


if __name__ == "__main__":
    create_and_share_sheet()
