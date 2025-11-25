from ingestion.ingest_google import ingest_google
from ingestion.ingest_traveloka import ingest_traveloka
from ingestion.ingest_facebook import ingest_facebook

def run_pipeline():
    print("🚀 Starting data ingestion pipeline...")

    steps = [
        ("Google Reviews", ingest_google),
        ("Traveloka Reviews", ingest_traveloka),
        ("Facebook Reviews", ingest_facebook),
    ]

    for name, func in steps:
        print(f"\n▶ Running step: {name}")
        try:
            func()
            print(f"✔ Step completed: {name}")
        except Exception as e:
            print(f"❌ Error in step {name}: {e}")

    print("\n🎉 Pipeline finished.")

if __name__ == "__main__":
    run_pipeline()