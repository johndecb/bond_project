# core/startup.py (new version)
# NOTE:
# This file is a legacy replacement for the old SQLite startup script.
# It has been repurposed to run a basic Django/Postgres schema check,
# but it’s not really needed in production since Django migrations
# already handle schema management automatically.
# You can safely ignore or remove this file if it becomes redundant.

import os
import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "funderly.settings")
django.setup()

from django.core.management import call_command

def validate_database():
    """Ensure DB schema matches Django migrations."""
    call_command("migrate", check=True, plan=True)
    print("✅ Database schema matches migrations")