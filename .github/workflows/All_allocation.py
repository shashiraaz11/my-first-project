name: Google Sheet Auto Update

on:
  schedule:
    - cron: "30 3 * * *"   # 9:00 AM IST (3:30 UTC)
    - cron: "30 4 * * *"   # 10:00 AM IST (4:30 UTC)
  workflow_dispatch:       # Manual run option

jobs:
  run-script:
    runs-on: ubuntu-latest

    steps:
      - name: Checkout repo
        uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.10'

      - name: Install dependencies
        run: |
          python -m pip install --upgrade pip
          pip install -r requirements.txt

      - name: Create credentials file
        run: |
          echo "$GOOGLE_CREDENTIALS" > credentials.json

      - name: Run Google Sheet updater
        env:
          GOOGLE_APPLICATION_CREDENTIALS: credentials.json
        run: |
          python sheet_updater.py
