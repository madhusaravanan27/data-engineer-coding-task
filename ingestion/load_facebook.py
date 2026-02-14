import pandas as pd
import os
from datetime import datetime, timezone
from psycopg2.extras import execute_batch
from db_utils import get_connection



def extract_facebook():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(base_dir, "facebook_export.csv")

    df = pd.read_csv(file_path)
    print("Raw rows:", len(df))
    return df

def run_facebook_dq(df):

    
    df = df.dropna(how="all")

    df["date"] = pd.to_datetime(df["date"], errors="coerce")

    required_cols = ["campaign_id", "date","impressions", "clicks","spend", "purchases", "purchase_value"]

    missing_required = df[df[required_cols].isnull().any(axis=1)].copy()

    metric_cols = ["impressions", "clicks","spend", "purchases", "purchase_value"]

    negative_values = df[(df[metric_cols] < 0).any(axis=1)].copy()

    duplicates = df[df.duplicated(subset=["campaign_id", "date"],keep=False)].copy()

    dq_issues = (pd.concat([missing_required,negative_values,duplicates]).drop_duplicates())

    valid_df = df[~df.index.isin(dq_issues.index)].copy()

    print("Total:", len(df))
    print("Rejected:", len(dq_issues))
    print(dq_issues)
    print("Valid:", len(valid_df))

    return valid_df


def transform_facebook(df):
    df["source_system"] = "facebook"
    df["ingested_at"] = datetime.now(timezone.utc)
    return df


def load_facebook(df):

    conn = get_connection()
    cur = conn.cursor()

    rows = list(df.itertuples(index=False, name=None))

    insert_sql = """
    INSERT INTO stg_facebook_ads (
        campaign_id, campaign_name, date,
        impressions, clicks, spend,
        purchases, purchase_value, reach, frequency,
        source_system, ingested_at
    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT (campaign_id, date)
    DO UPDATE SET
    campaign_name = EXCLUDED.campaign_name,
    impressions = EXCLUDED.impressions,
    clicks = EXCLUDED.clicks,
    spend = EXCLUDED.spend,
    purchases = EXCLUDED.purchases,
    purchase_value = EXCLUDED.purchase_value,
    reach = EXCLUDED.reach,
    frequency = EXCLUDED.frequency,
    source_system = EXCLUDED.source_system,
    ingested_at = EXCLUDED.ingested_at;
    """

    execute_batch(cur, insert_sql, rows)
    conn.commit()
    conn.close()

    print(f"Inserted {len(rows)} facebook rows")


if __name__ == "__main__":
    df = extract_facebook()
    df = run_facebook_dq(df)
    df = transform_facebook(df)
    load_facebook(df)
