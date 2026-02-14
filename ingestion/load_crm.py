import csv
import os
import pandas as pd
from datetime import datetime, timezone
from psycopg2.extras import execute_batch
from db_utils import get_connection


def extract_crm():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(base_dir, "crm_revenue.csv")

    bad_rows = []
    good_rows = []

    with open(file_path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)
        expected_len = len(header)

        for line_num, row in enumerate(reader, start=2):
            if len(row) != expected_len:
                bad_rows.append({
                    "line_number": line_num,
                    "field_count": len(row)
                })
            else:
                good_rows.append(row)

    print("Structurally bad rows:", len(bad_rows))

    df = pd.DataFrame(good_rows, columns=header)
    return df


def run_crm_dq(df):

    
    df = df.replace('', pd.NA)

    df["customer_id"] = (df["customer_id"].astype("string").str.strip())

    df["customer_id"] = df["customer_id"].replace(["", "nan", "None", "<NA>"],pd.NA)

    
    df["date"] = pd.to_datetime(df["order_date"], errors="coerce")

    mask = df["date"].isna()
    df.loc[mask, "date"] = pd.to_datetime(df.loc[mask, "order_date"],errors="coerce", dayfirst=True)

    
    df["channel_attributed"] = (df["channel_attributed"].astype(str).str.strip().str.lower())

    df["revenue"] = pd.to_numeric(df["revenue"], errors="coerce")


    required_cols = ["order_id","customer_id","date","revenue","channel_attributed"]

    missing_required = df[df[required_cols].isnull().any(axis=1)].copy()
    missing_required["_dq_reason"] = "missing_required"

    valid_channels = {"google", "facebook"}
    invalid_channel = df[~df["channel_attributed"].isin(valid_channels)].copy()
    invalid_channel["_dq_reason"] = "invalid_channel"

    negative_revenue = df[df["revenue"] < 0].copy()
    negative_revenue["_dq_reason"] = "negative_revenue"


    revenue_non_null = df[df["revenue"].notnull()]

    q1 = revenue_non_null["revenue"].quantile(0.25)
    q3 = revenue_non_null["revenue"].quantile(0.75)
    iqr = q3 - q1

    lower_bound = q1 - 1.5 * iqr
    upper_bound = q3 + 1.5 * iqr

    outliers = df[(df["revenue"] < lower_bound) | (df["revenue"] > upper_bound)].copy()
    outliers["_dq_reason"] = "revenue_outlier"

    print("Revenue bounds:")
    print("Lower:", lower_bound)
    print("Upper:", upper_bound)
    print("Outliers found:", len(outliers))


    dq_issues = pd.concat([missing_required,invalid_channel,negative_revenue,outliers]).drop_duplicates()

    valid_df = df[~df.index.isin(dq_issues.index)].copy()


    valid_df = valid_df.sort_values("date")
    valid_df = valid_df.drop_duplicates(subset=["order_id"], keep="last")

    print("Valid rows after DQ:", len(valid_df))
    print("Unique order_ids after dedupe:", valid_df["order_id"].nunique())


    valid_df["ingested_at"] = datetime.now(timezone.utc)

   
    print("Parsed rows:", len(df))
    print("Rejected rows:", len(dq_issues))
    print("Rows with null customer_id:")
    print(df[df["customer_id"].isnull()][["order_id", "customer_id"]])
    print(dq_issues)
    print("Valid rows:", len(valid_df))
    print("Valid unique order_ids:", valid_df["order_id"].nunique())

    

    return valid_df

def transform_crm(df):
     

    df = df.copy()

    df["revenue"] = pd.to_numeric(df["revenue"], errors="coerce")
    df = df[df["revenue"].notnull()]
    df = df[df["revenue"] >= 0]

    df["ingested_at"] = datetime.now(timezone.utc)

    return df


def load_crm(df):

    conn = get_connection()
    cur = conn.cursor()

    cols = ["order_id","customer_id","date","revenue","channel_attributed","campaign_source","product_category","region","ingested_at"]

    rows = list(df[cols].itertuples(index=False, name=None))

    print("Rows to upsert:", len(rows))
    print("Unique order_ids before load:", df["order_id"].nunique())

    insert_sql = """
    INSERT INTO stg_crm_revenue (
        order_id, customer_id, date, revenue,
        channel_attributed, campaign_source,
        product_category, region,
        ingested_at
    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT (order_id)
    DO UPDATE SET
        customer_id = EXCLUDED.customer_id,
        date = EXCLUDED.date,
        revenue = EXCLUDED.revenue,
        channel_attributed = EXCLUDED.channel_attributed,
        campaign_source = EXCLUDED.campaign_source,
        product_category = EXCLUDED.product_category,
        region = EXCLUDED.region,
        ingested_at = EXCLUDED.ingested_at;
    """

    execute_batch(cur, insert_sql, rows)
    conn.commit()
    conn.close()

    print(f"Upserted {len(rows)} CRM rows")


if __name__ == "__main__":
    df = extract_crm()
    df = run_crm_dq(df)
    df = transform_crm(df)
    load_crm(df)
