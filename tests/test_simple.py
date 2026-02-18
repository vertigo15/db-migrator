"""Simple test to verify .env loading works."""
import os
from dotenv import load_dotenv

# Same path logic as in the page
base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
env_path = os.path.join(base_dir, '.env')

print(f'Loading from: {env_path}')
print(f'File exists: {os.path.exists(env_path)}')

load_dotenv(env_path, override=True)

print(f'SOURCE_DB_HOST: {os.getenv("SOURCE_DB_HOST", "NOT_SET")}')
print(f'SOURCE_DB_DATABASE: {os.getenv("SOURCE_DB_DATABASE", "NOT_SET")}')
print(f'SOURCE_DB_USERNAME: {os.getenv("SOURCE_DB_USERNAME", "NOT_SET")}')
print(f'TABLE_PREFIX: {os.getenv("TABLE_PREFIX", "NOT_SET")}')
