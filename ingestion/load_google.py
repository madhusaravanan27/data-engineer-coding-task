import json
import pandas as pd
from datetime import datetime, timezone
from psycopg2.extras import execute_batch
from db_utils import get_connection
import os



def extract_google():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(base_dir, "google_ads_api.json")

    with open(file_path, "r") as f:
        payload = json.load(f)

    rows = []
    for campaign in payload["campaigns"]:
        for metrics in campaign["daily_metrics"]:
            rows.append({
                "campaign_id": campaign["campaign_id"],
                "campaign_name": campaign["campaign_name"],
                "campaign_type": campaign["campaign_type"],
                "date": metrics["date"],
                "impressions": metrics["impressions"],
                "clicks": metrics["clicks"],
                "cost_micros": metrics["cost_micros"],
                "conversions": metrics["conversions"],
                "conversion_value": metrics["conversion_value"],
            })

    return pd.DataFrame(rows)


def run_google_dq(df):

    required_cols = ["campaign_id", "date","impressions", "clicks","cost_micros", "conversion_value"]

    invalid_missing = df[df[required_cols].isnull().any(axis=1)].copy()

    metric_cols = ["impressions", "clicks","cost_micros", "conversions","conversion_value"]

    invalid_negative = df[(df[metric_cols] < 0).any(axis=1)].copy()

    duplicate_mask = df.duplicated(subset=["campaign_id", "date"],keep=False)

    invalid_duplicates = df[duplicate_mask].copy()

    dq_issues = (pd.concat([invalid_missing,invalid_negative,invalid_duplicates]).drop_duplicates() )

    valid_df = df[~df.index.isin(dq_issues.index)].copy()

    print("Total rows:", len(df))
    print("Invalid rows:", len(dq_issues))
    print("Valid rows:", len(valid_df))

    return valid_df


def transform_google(df):
    df["date"] = pd.to_datetime(df["date"])
    df["cost"] = df["cost_micros"] / 1_000_000
    df["source_system"] = "google"
    df = df.drop(columns=["cost_micros"])


    df["ingested_at"] = datetime.now(timezone.utc)
    return df

def load_google(df):
    conn = get_connection()
    cur = conn.cursor()

    rows = list(df.itertuples(index=False, name=None))

    insert_sql = """
    INSERT INTO stg_google_ads (
        campaign_id, campaign_name, campaign_type, date,
        impressions, clicks, cost,
        conversions, conversion_value,
        source_system, ingested_at
    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT (campaign_id, date)
    DO UPDATE SET
    impressions = EXCLUDED.impressions,
    clicks = EXCLUDED.clicks,
    cost = EXCLUDED.cost,
    conversions = EXCLUDED.conversions,
    conversion_value = EXCLUDED.conversion_value,
    source_system = EXCLUDED.source_system,
    ingested_at = EXCLUDED.ingested_at;
    """

    execute_batch(cur, insert_sql, rows)
    conn.commit()
    conn.close()

    print(f"Inserted {len(rows)} google rows")



if __name__ == "__main__":
    df = extract_google()
    df = run_google_dq(df)
    df = transform_google(df)
    load_google(df)
