name: All_allocation

on:
  push:
  schedule:
    - cron: '30 2 * * *'   # 9:00 AM IST
    - cron: '30 3 * * *'   # 10:00 AM IST 
    - cron: '30 4 * * *'   # 11:00 AM IST
    - cron: '30 6 * * *'   # 12:00 PM IST
  workflow_dispatch:

jobs:
  run-All_allocation:
    runs-on: ubuntu-latest

    steps:
      - name: Checkout code
        uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.10'

      - name: Install dependencies
        run: |
          python -m pip install --upgrade pip
          pip install pandas gspread google-auth

      - name: Run All_allocation.py
        env:
          ACCOUNT_KEY_JSON: ${{ secrets.ACCOUNT_KEY_JSON }}
        run: |
          python All_allocation.py
