{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 123,
   "id": "548a6cdd-26d7-4760-a4d1-947a477515cf",
   "metadata": {},
   "outputs": [],
   "source": [
    "import csv\n",
    "import pandas as pd\n",
    "from datetime import datetime"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 124,
   "id": "7099ed3f-5d1d-4653-b07c-76a92f55792b",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Total lines in file: 86\n",
      "Rows loaded into DataFrame: 85\n"
     ]
    }
   ],
   "source": [
    "# with open(\"crm_revenue.csv\", newline=\"\") as f:\n",
    "#     total_rows = sum(1 for _ in f)\n",
    "\n",
    "# print(\"Total lines in file:\", total_rows)\n",
    "# print(\"Rows loaded into DataFrame:\", len(df_crm) + 1)  # +1 for header\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 125,
   "id": "ef6cca34-3c12-4004-8196-76ca144d96cd",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Malformed rows: 1\n",
      "First malformed row example: {'line_number': 20, 'raw_row': ['ORD-10019', 'CUST-6543', 'January 4', ' 2024', '98.75', 'google', 'goog_camp_001', 'Apparel', 'Asia Pacific'], 'field_count': 9}\n"
     ]
    }
   ],
   "source": [
    "\n",
    "bad_rows = []\n",
    "good_rows = []\n",
    "\n",
    "with open(\"crm_revenue.csv\", newline=\"\", encoding=\"utf-8\") as f:\n",
    "    reader = csv.reader(f)\n",
    "    header = next(reader)\n",
    "    expected_len = len(header)\n",
    "\n",
    "    for line_num, row in enumerate(reader, start=2):\n",
    "        if len(row) != expected_len:\n",
    "            bad_rows.append({\n",
    "                \"line_number\": line_num,\n",
    "                \"raw_row\": row,\n",
    "                \"field_count\": len(row)\n",
    "            })\n",
    "        else:\n",
    "            good_rows.append(row)\n",
    "\n",
    "print(\"Malformed rows:\", len(bad_rows))\n",
    "print(\"First malformed row example:\", bad_rows[0] if bad_rows else \"None\")\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 126,
   "id": "a3d86b07-3ef6-4a8a-9a5a-2949dbcd7c79",
   "metadata": {},
   "outputs": [],
   "source": [
    "crmdf= pd.read_csv(\"crm_revenue.csv\",engine=\"python\",on_bad_lines=\"skip\")\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 127,
   "id": "0d31eff2-47c4-49bd-b9b6-cafc4d491b83",
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "order_id              0\n",
       "customer_id           1\n",
       "order_date            0\n",
       "revenue               1\n",
       "channel_attributed    0\n",
       "campaign_source       1\n",
       "product_category      0\n",
       "region                0\n",
       "dtype: int64"
      ]
     },
     "execution_count": 127,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "# Null check for all the columns\n",
    "crmdf.isnull().sum()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 128,
   "id": "6fee584e-b846-4d31-99f9-ba2c3fdfab13",
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/html": [
       "<div>\n",
       "<style scoped>\n",
       "    .dataframe tbody tr th:only-of-type {\n",
       "        vertical-align: middle;\n",
       "    }\n",
       "\n",
       "    .dataframe tbody tr th {\n",
       "        vertical-align: top;\n",
       "    }\n",
       "\n",
       "    .dataframe thead th {\n",
       "        text-align: right;\n",
       "    }\n",
       "</style>\n",
       "<table border=\"1\" class=\"dataframe\">\n",
       "  <thead>\n",
       "    <tr style=\"text-align: right;\">\n",
       "      <th></th>\n",
       "      <th>order_id</th>\n",
       "      <th>customer_id</th>\n",
       "      <th>order_date</th>\n",
       "      <th>revenue</th>\n",
       "      <th>channel_attributed</th>\n",
       "      <th>campaign_source</th>\n",
       "      <th>product_category</th>\n",
       "      <th>region</th>\n",
       "    </tr>\n",
       "  </thead>\n",
       "  <tbody>\n",
       "    <tr>\n",
       "      <th>0</th>\n",
       "      <td>ORD-10001</td>\n",
       "      <td>CUST-5234</td>\n",
       "      <td>2024-01-01</td>\n",
       "      <td>125.50</td>\n",
       "      <td>google</td>\n",
       "      <td>goog_camp_002</td>\n",
       "      <td>Electronics</td>\n",
       "      <td>North America</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>1</th>\n",
       "      <td>ORD-10002</td>\n",
       "      <td>CUST-8921</td>\n",
       "      <td>2024-01-01</td>\n",
       "      <td>89.99</td>\n",
       "      <td>facebook</td>\n",
       "      <td>fb_camp_002</td>\n",
       "      <td>Apparel</td>\n",
       "      <td>Europe</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>2</th>\n",
       "      <td>ORD-10003</td>\n",
       "      <td>CUST-3456</td>\n",
       "      <td>01/01/2024</td>\n",
       "      <td>234.00</td>\n",
       "      <td>google</td>\n",
       "      <td>goog_camp_001</td>\n",
       "      <td>Home &amp; Garden</td>\n",
       "      <td>North America</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>3</th>\n",
       "      <td>ORD-10004</td>\n",
       "      <td>CUST-7823</td>\n",
       "      <td>2024-01-01</td>\n",
       "      <td>67.50</td>\n",
       "      <td>facebook</td>\n",
       "      <td>fb_camp_001</td>\n",
       "      <td>Apparel</td>\n",
       "      <td>North America</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>4</th>\n",
       "      <td>ORD-10005</td>\n",
       "      <td>CUST-2341</td>\n",
       "      <td>2024-01-01</td>\n",
       "      <td>445.00</td>\n",
       "      <td>google</td>\n",
       "      <td>goog_camp_002</td>\n",
       "      <td>Electronics</td>\n",
       "      <td>Europe</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>5</th>\n",
       "      <td>ORD-10006</td>\n",
       "      <td>CUST-9012</td>\n",
       "      <td>2024-01-02</td>\n",
       "      <td>178.25</td>\n",
       "      <td>facebook</td>\n",
       "      <td>fb_camp_002</td>\n",
       "      <td>Home &amp; Garden</td>\n",
       "      <td>North America</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>6</th>\n",
       "      <td>ORD-10007</td>\n",
       "      <td>CUST-4567</td>\n",
       "      <td>2024-01-02</td>\n",
       "      <td>56.00</td>\n",
       "      <td>google</td>\n",
       "      <td>goog_camp_003</td>\n",
       "      <td>Apparel</td>\n",
       "      <td>Asia Pacific</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>7</th>\n",
       "      <td>ORD-10008</td>\n",
       "      <td>CUST-6789</td>\n",
       "      <td>2024-01-02</td>\n",
       "      <td>NaN</td>\n",
       "      <td>facebook</td>\n",
       "      <td>fb_camp_001</td>\n",
       "      <td>Electronics</td>\n",
       "      <td>North America</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>8</th>\n",
       "      <td>ORD-10009</td>\n",
       "      <td>CUST-1234</td>\n",
       "      <td>2024-01-02</td>\n",
       "      <td>312.75</td>\n",
       "      <td>google</td>\n",
       "      <td>goog_camp_002</td>\n",
       "      <td>Electronics</td>\n",
       "      <td>Europe</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>9</th>\n",
       "      <td>ORD-10010</td>\n",
       "      <td>CUST-8456</td>\n",
       "      <td>2024-01-02</td>\n",
       "      <td>89.00</td>\n",
       "      <td>facebook</td>\n",
       "      <td>fb_camp_002</td>\n",
       "      <td>Apparel</td>\n",
       "      <td>North America</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>10</th>\n",
       "      <td>ORD-10011</td>\n",
       "      <td>CUST-2345</td>\n",
       "      <td>2024-01-03</td>\n",
       "      <td>156.50</td>\n",
       "      <td>Google</td>\n",
       "      <td>goog_camp_001</td>\n",
       "      <td>Home &amp; Garden</td>\n",
       "      <td>North America</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>11</th>\n",
       "      <td>ORD-10012</td>\n",
       "      <td>CUST-7890</td>\n",
       "      <td>2024-01-03</td>\n",
       "      <td>78.25</td>\n",
       "      <td>FACEBOOK</td>\n",
       "      <td>fb_camp_003</td>\n",
       "      <td>Apparel</td>\n",
       "      <td>Europe</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>12</th>\n",
       "      <td>ORD-10013</td>\n",
       "      <td>CUST-3456</td>\n",
       "      <td>2024-01-03</td>\n",
       "      <td>234.00</td>\n",
       "      <td>google</td>\n",
       "      <td>goog_camp_002</td>\n",
       "      <td>Electronics</td>\n",
       "      <td>North America</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>13</th>\n",
       "      <td>ORD-10014</td>\n",
       "      <td>CUST-5678</td>\n",
       "      <td>2024-01-03</td>\n",
       "      <td>445.75</td>\n",
       "      <td>facebook</td>\n",
       "      <td>fb_camp_002</td>\n",
       "      <td>Electronics</td>\n",
       "      <td>Asia Pacific</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>14</th>\n",
       "      <td>ORD-10015</td>\n",
       "      <td>CUST-9123</td>\n",
       "      <td>2024-01-03</td>\n",
       "      <td>67.00</td>\n",
       "      <td>google</td>\n",
       "      <td>goog_camp_003</td>\n",
       "      <td>Apparel</td>\n",
       "      <td>North America</td>\n",
       "    </tr>\n",
       "  </tbody>\n",
       "</table>\n",
       "</div>"
      ],
      "text/plain": [
       "     order_id customer_id  order_date  revenue channel_attributed  \\\n",
       "0   ORD-10001   CUST-5234  2024-01-01   125.50             google   \n",
       "1   ORD-10002   CUST-8921  2024-01-01    89.99           facebook   \n",
       "2   ORD-10003   CUST-3456  01/01/2024   234.00             google   \n",
       "3   ORD-10004   CUST-7823  2024-01-01    67.50           facebook   \n",
       "4   ORD-10005   CUST-2341  2024-01-01   445.00             google   \n",
       "5   ORD-10006   CUST-9012  2024-01-02   178.25           facebook   \n",
       "6   ORD-10007   CUST-4567  2024-01-02    56.00             google   \n",
       "7   ORD-10008   CUST-6789  2024-01-02      NaN           facebook   \n",
       "8   ORD-10009   CUST-1234  2024-01-02   312.75             google   \n",
       "9   ORD-10010   CUST-8456  2024-01-02    89.00           facebook   \n",
       "10  ORD-10011   CUST-2345  2024-01-03   156.50             Google   \n",
       "11  ORD-10012   CUST-7890  2024-01-03    78.25           FACEBOOK   \n",
       "12  ORD-10013   CUST-3456  2024-01-03   234.00             google   \n",
       "13  ORD-10014   CUST-5678  2024-01-03   445.75           facebook   \n",
       "14  ORD-10015   CUST-9123  2024-01-03    67.00             google   \n",
       "\n",
       "   campaign_source product_category         region  \n",
       "0    goog_camp_002      Electronics  North America  \n",
       "1      fb_camp_002          Apparel         Europe  \n",
       "2    goog_camp_001    Home & Garden  North America  \n",
       "3      fb_camp_001          Apparel  North America  \n",
       "4    goog_camp_002      Electronics         Europe  \n",
       "5      fb_camp_002    Home & Garden  North America  \n",
       "6    goog_camp_003          Apparel   Asia Pacific  \n",
       "7      fb_camp_001      Electronics  North America  \n",
       "8    goog_camp_002      Electronics         Europe  \n",
       "9      fb_camp_002          Apparel  North America  \n",
       "10   goog_camp_001    Home & Garden  North America  \n",
       "11     fb_camp_003          Apparel         Europe  \n",
       "12   goog_camp_002      Electronics  North America  \n",
       "13     fb_camp_002      Electronics   Asia Pacific  \n",
       "14   goog_camp_003          Apparel  North America  "
      ]
     },
     "execution_count": 128,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "crmdf.head(15)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 129,
   "id": "7af1fe2a-019a-45bd-9fb3-008ed36d518c",
   "metadata": {},
   "outputs": [],
   "source": [
    "# Date Formatting\n",
    "\n",
    "crmdf[\"date\"] = pd.to_datetime(crmdf[\"order_date\"],errors=\"coerce\",dayfirst=True)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 131,
   "id": "14ea5bb6-676c-49ca-b0e3-032151daa0e3",
   "metadata": {},
   "outputs": [],
   "source": [
    "# Required Columns check\n",
    "required_cols = [\"order_id\", \"customer_id\",\"date\",\"revenue\", \"channel_attributed\"]\n",
    "\n",
    "missing_required = crmdf[crmdf[required_cols].isnull().any(axis=1)].copy()\n",
    "\n",
    "missing_required[\"_dq_reason\"] = \"missing_required\"\n",
    "missing_required[\"_failed_rule\"] = \"required_fields_not_null\"\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 132,
   "id": "b15f1b79-e420-47dd-83ff-739f1515631e",
   "metadata": {},
   "outputs": [],
   "source": [
    "# Invalid Dates\n",
    "invalid_dates = crmdf[crmdf[\"date\"].isnull()].copy()\n",
    "\n",
    "invalid_dates[\"_dq_reason\"] = \"invalid_format\"\n",
    "invalid_dates[\"_failed_rule\"] = \"order_date_parseable\"\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 133,
   "id": "63d9a69b-036b-4334-b041-a4de12967202",
   "metadata": {},
   "outputs": [],
   "source": [
    "crmdf[\"channel_attributed\"] = (crmdf[\"channel_attributed\"].str.strip().str.lower())\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 134,
   "id": "da11cc97-fcbe-46ae-9753-62c733571e07",
   "metadata": {},
   "outputs": [],
   "source": [
    "valid_channels = {\"google\", \"facebook\"}\n",
    "\n",
    "invalid_channel = crmdf[~crmdf[\"channel_attributed\"].isin(valid_channels)].copy()\n",
    "\n",
    "invalid_channel[\"_dq_reason\"] = \"invalid_value\"\n",
    "invalid_channel[\"_failed_rule\"] = \"channel_known\"\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 135,
   "id": "2d5c9feb-61eb-4060-b1b3-d6a7d9bd80e3",
   "metadata": {},
   "outputs": [],
   "source": [
    "duplicate_orders = crmdf[crmdf.duplicated(subset=[\"order_id\"], keep=False)].copy()\n",
    "\n",
    "duplicate_orders[\"_dq_reason\"] = \"duplicate\"\n",
    "duplicate_orders[\"_failed_rule\"] = \"order_id_unique\"\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 136,
   "id": "25f65f1f-0301-401a-954d-da7bffcf937d",
   "metadata": {},
   "outputs": [],
   "source": [
    "crmrejects = pd.concat([missing_required,invalid_dates,invalid_channel,duplicate_orders],ignore_index=True).drop_duplicates()\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 137,
   "id": "3a0005a1-45b8-4969-adce-c5249e9e60be",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "    order_id customer_id  order_date  revenue channel_attributed  \\\n",
      "0  ORD-10008   CUST-6789  2024-01-02      NaN           facebook   \n",
      "1  ORD-10030         NaN  2024-01-06   189.75           facebook   \n",
      "2  ORD-10021   CUST-5432  2024-01-05   178.00             google   \n",
      "4  ORD-10056   CUST-5432  2024-01-12   389.25           facebook   \n",
      "6  ORD-10071   CUST-9876  2024-01-15   567.00             google   \n",
      "\n",
      "  campaign_source product_category         region       date  \\\n",
      "0     fb_camp_001      Electronics  North America 2024-01-02   \n",
      "1     fb_camp_003    Home & Garden         Europe 2024-01-06   \n",
      "2   goog_camp_002      Electronics  North America 2024-01-05   \n",
      "4     fb_camp_001          Apparel         Europe 2024-01-12   \n",
      "6   goog_camp_002      Electronics  North America 2024-01-15   \n",
      "\n",
      "         _dq_reason              _failed_rule  \n",
      "0  missing_required  required_fields_not_null  \n",
      "1  missing_required  required_fields_not_null  \n",
      "2         duplicate           order_id_unique  \n",
      "4         duplicate           order_id_unique  \n",
      "6         duplicate           order_id_unique  \n"
     ]
    }
   ],
   "source": [
    "print(crmrejects)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 138,
   "id": "205b9da9-da9d-4a52-988a-29c1c0500665",
   "metadata": {},
   "outputs": [],
   "source": [
    "valid_crm = crmdf[ ~crmdf.index.isin(crmrejects.index)].copy()\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 140,
   "id": "b92261cb-9b58-4ce7-850b-4e9499b31254",
   "metadata": {},
   "outputs": [],
   "source": [
    "valid_crm=valid_crm.dropna()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 141,
   "id": "31d6d590-f0db-4a6a-833e-43e72a33a261",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "     order_id customer_id  order_date  revenue channel_attributed  \\\n",
      "3   ORD-10004   CUST-7823  2024-01-01    67.50           facebook   \n",
      "5   ORD-10006   CUST-9012  2024-01-02   178.25           facebook   \n",
      "8   ORD-10009   CUST-1234  2024-01-02   312.75             google   \n",
      "9   ORD-10010   CUST-8456  2024-01-02    89.00           facebook   \n",
      "10  ORD-10011   CUST-2345  2024-01-03   156.50             google   \n",
      "..        ...         ...         ...      ...                ...   \n",
      "78  ORD-10078   CUST-9876  2024-01-15   156.25           facebook   \n",
      "79  ORD-10079   CUST-3210  2024-01-15   534.00             google   \n",
      "80  ORD-10080   CUST-7654  15/01/2024    67.75           facebook   \n",
      "82  ORD-10082   CUST-6543  2024-01-15   178.50           facebook   \n",
      "83  ORD-10071   CUST-9876  2024-01-15   567.00             google   \n",
      "\n",
      "   campaign_source product_category         region       date  \n",
      "3      fb_camp_001          Apparel  North America 2024-01-01  \n",
      "5      fb_camp_002    Home & Garden  North America 2024-01-02  \n",
      "8    goog_camp_002      Electronics         Europe 2024-01-02  \n",
      "9      fb_camp_002          Apparel  North America 2024-01-02  \n",
      "10   goog_camp_001    Home & Garden  North America 2024-01-03  \n",
      "..             ...              ...            ...        ...  \n",
      "78     fb_camp_002      Electronics  North America 2024-01-15  \n",
      "79   goog_camp_001          Apparel   Asia Pacific 2024-01-15  \n",
      "80     fb_camp_003    Home & Garden         Europe 2024-01-15  \n",
      "82     fb_camp_001          Apparel  North America 2024-01-15  \n",
      "83   goog_camp_002      Electronics  North America 2024-01-15  \n",
      "\n",
      "[76 rows x 9 columns]\n"
     ]
    }
   ],
   "source": [
    "print(valid_crm)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 86,
   "id": "54a8570c-a3e4-46df-8833-7c44bad5c80b",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Structurally bad rows: 1\n",
      "Parsed rows: 84\n",
      "Rejected rows: 5\n",
      "Valid rows: 79\n"
     ]
    }
   ],
   "source": [
    "print(\"Structurally bad rows:\", len(bad_rows))\n",
    "print(\"Parsed rows:\", len(crmdf))\n",
    "print(\"Rejected rows:\", len(crmrejects))\n",
    "print(\"Valid rows:\", len(valid_crm))\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 85,
   "id": "4367fc3d-f1aa-48b4-ac66-172bad8cc2d1",
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/html": [
       "<div>\n",
       "<style scoped>\n",
       "    .dataframe tbody tr th:only-of-type {\n",
       "        vertical-align: middle;\n",
       "    }\n",
       "\n",
       "    .dataframe tbody tr th {\n",
       "        vertical-align: top;\n",
       "    }\n",
       "\n",
       "    .dataframe thead th {\n",
       "        text-align: right;\n",
       "    }\n",
       "</style>\n",
       "<table border=\"1\" class=\"dataframe\">\n",
       "  <thead>\n",
       "    <tr style=\"text-align: right;\">\n",
       "      <th></th>\n",
       "      <th>order_id</th>\n",
       "      <th>customer_id</th>\n",
       "      <th>order_date</th>\n",
       "      <th>revenue</th>\n",
       "      <th>channel_attributed</th>\n",
       "      <th>campaign_source</th>\n",
       "      <th>product_category</th>\n",
       "      <th>region</th>\n",
       "      <th>_dq_reason</th>\n",
       "      <th>_failed_rule</th>\n",
       "    </tr>\n",
       "  </thead>\n",
       "  <tbody>\n",
       "    <tr>\n",
       "      <th>0</th>\n",
       "      <td>ORD-10008</td>\n",
       "      <td>CUST-6789</td>\n",
       "      <td>2024-01-02</td>\n",
       "      <td>NaN</td>\n",
       "      <td>facebook</td>\n",
       "      <td>fb_camp_001</td>\n",
       "      <td>Electronics</td>\n",
       "      <td>North America</td>\n",
       "      <td>missing_required</td>\n",
       "      <td>required_fields_not_null</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>1</th>\n",
       "      <td>ORD-10030</td>\n",
       "      <td>NaN</td>\n",
       "      <td>2024-01-06</td>\n",
       "      <td>189.75</td>\n",
       "      <td>facebook</td>\n",
       "      <td>fb_camp_003</td>\n",
       "      <td>Home &amp; Garden</td>\n",
       "      <td>Europe</td>\n",
       "      <td>missing_required</td>\n",
       "      <td>required_fields_not_null</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>2</th>\n",
       "      <td>ORD-10021</td>\n",
       "      <td>CUST-5432</td>\n",
       "      <td>2024-01-05</td>\n",
       "      <td>178.00</td>\n",
       "      <td>google</td>\n",
       "      <td>goog_camp_002</td>\n",
       "      <td>Electronics</td>\n",
       "      <td>North America</td>\n",
       "      <td>duplicate</td>\n",
       "      <td>order_id_unique</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>4</th>\n",
       "      <td>ORD-10056</td>\n",
       "      <td>CUST-5432</td>\n",
       "      <td>2024-01-12</td>\n",
       "      <td>389.25</td>\n",
       "      <td>facebook</td>\n",
       "      <td>fb_camp_001</td>\n",
       "      <td>Apparel</td>\n",
       "      <td>Europe</td>\n",
       "      <td>duplicate</td>\n",
       "      <td>order_id_unique</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>6</th>\n",
       "      <td>ORD-10071</td>\n",
       "      <td>CUST-9876</td>\n",
       "      <td>2024-01-15</td>\n",
       "      <td>567.00</td>\n",
       "      <td>google</td>\n",
       "      <td>goog_camp_002</td>\n",
       "      <td>Electronics</td>\n",
       "      <td>North America</td>\n",
       "      <td>duplicate</td>\n",
       "      <td>order_id_unique</td>\n",
       "    </tr>\n",
       "  </tbody>\n",
       "</table>\n",
       "</div>"
      ],
      "text/plain": [
       "    order_id customer_id order_date  revenue channel_attributed  \\\n",
       "0  ORD-10008   CUST-6789 2024-01-02      NaN           facebook   \n",
       "1  ORD-10030         NaN 2024-01-06   189.75           facebook   \n",
       "2  ORD-10021   CUST-5432 2024-01-05   178.00             google   \n",
       "4  ORD-10056   CUST-5432 2024-01-12   389.25           facebook   \n",
       "6  ORD-10071   CUST-9876 2024-01-15   567.00             google   \n",
       "\n",
       "  campaign_source product_category         region        _dq_reason  \\\n",
       "0     fb_camp_001      Electronics  North America  missing_required   \n",
       "1     fb_camp_003    Home & Garden         Europe  missing_required   \n",
       "2   goog_camp_002      Electronics  North America         duplicate   \n",
       "4     fb_camp_001          Apparel         Europe         duplicate   \n",
       "6   goog_camp_002      Electronics  North America         duplicate   \n",
       "\n",
       "               _failed_rule  \n",
       "0  required_fields_not_null  \n",
       "1  required_fields_not_null  \n",
       "2           order_id_unique  \n",
       "4           order_id_unique  \n",
       "6           order_id_unique  "
      ]
     },
     "execution_count": 85,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "crmrejects.head(5)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 87,
   "id": "43661335-727f-4949-96eb-f05e6d33b919",
   "metadata": {},
   "outputs": [],
   "source": [
    "valid_crm[\"ingested_at\"] = datetime.utcnow()\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 89,
   "id": "6aee594c-7214-44c8-a2b6-379eb91ebd69",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "79\n"
     ]
    }
   ],
   "source": [
    "print(len(valid_crm))"
   ]
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3 (ipykernel)",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.9.6"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 5
}
