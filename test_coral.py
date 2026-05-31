import asyncio
from backend.services.coral_client import query_coral

async def main():
    print("Testing Coral Integration...")
    try:
        # Query the pagerduty source configured locally
        results = await query_coral("SELECT id, title, status FROM pagerduty.incidents LIMIT 2")
        print(f"Success! Retrieved {len(results)} incidents:")
        for r in results:
            print(f" - [{r.get('status')}] {r.get('id')}: {r.get('title')}")
    except Exception as e:
        print(f"Failed to query Coral: {e}")

if __name__ == "__main__":
    asyncio.run(main())
