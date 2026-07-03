import requests
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime
import os
import time
import logging

from google.cloud import bigquery
from google.oauth2 import service_account

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
    handlers=[
        logging.FileHandler('pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

load_dotenv()
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
if not GITHUB_TOKEN:
    logger.info("GITHUB_TOKEN not found. Please fill in .env file")

HEADERS = {
    "Authorization": f"Bearer {GITHUB_TOKEN}",
    "Accept": "application/vnd.github+json"
}

SEARCH_URL = "https://api.github.com/search/repositories"

BIGQUERY_KEY_PATH = "./include/inlaid-particle-359102-118bdae159e1.json"
BIGQUERY_PROJECT_ID = "inlaid-particle-359102"
BIGQUERY_DATASET = "github_data"
BIGQUERY_TABLE = "repositories"

logger.info("[INFO]: Setup is completed...")
def extract(language="python", min_stars=1000, max_pages=2, per_page=10):
    """
        Extract repos from Github API
        Return list of repo dictionaries
    """
    logger.info("[INFO]: Starting Extraction...")

    try:
        repos = []

        params = {
            "q": f"language:{language} stars:>{min_stars}",
            "sort": "stars",
            "order": "desc",
            "per_page": per_page
        }

        for page in range (1, max_pages+1):
            params["page"] = page
            logger.info(f"[INFO]: Fetching page: {page}")

            response = requests.get(SEARCH_URL, headers=HEADERS, params=params)
            response.raise_for_status()

            remaining = int(response.headers.get("X-RateLimit-Remaining", 0))
            logger.info(f"[INFO]: Rate limit remaining: {remaining}")

            if response.status_code != 200:
                logger.info(f"[ERROR]: {response.status_code} {response.text}")
                break
            
            data = response.json()
            items = data.get("items", [])
            repos.extend(items)

            if len(items) == 0:
                break

            time.sleep(1)
        
        logger.info(f"[INFO]: Extracted {len(repos)} repos")
        return repos
    
    except requests.exceptions.RequestException as e:
        logger.error(f"API request failed: {e}")
        return []

    except Exception as e:
        logger.exception(f"Unexpected error in extract(): {e}")
        return []

def transform(repos):
    """
    Transform raw API into clean DataFrame
    Returns DataFrame
    """
    try:
        if not repos:
            logger.info("[INFO]: No data to transform")
            return pd.DataFrame()

        # create dataframe from repos
        df = pd.DataFrame([{
            "id": r["id"],
            "name": r["name"],
            "full_name": r["full_name"],
            "owner": r["owner"]["login"],
            "language": r["language"],
            "stars": r["stargazers_count"],
            "forks": r["forks_count"],
            "open_issues": r["open_issues_count"],
            "description": r["description"],
            "created_at": pd.to_datetime(r["created_at"]),
            "updated_at": pd.to_datetime(r["updated_at"]),
            "url": r["html_url"]
        } for r in repos])

        initial_count = len(df)
        df = df.drop_duplicates(subset="id")
        logger.info(f"[INFO]: Removed {initial_count - len(df)} duplicate id")

        df["description"] = df["description"].fillna("No Description")
        df["fetched_at"] = datetime.now()

        def stars_category(stars):
            if stars >= 100000:
                return "legendary"
            elif stars >= 10000:
                return "popular"
            else:
                return "rising"

        df["stars_category"] = df["stars"].apply(stars_category)
        df = df.sort_values("stars", ascending=False)

        logger.info(f"Transform completed. {len(df)} rows, {len(df.columns)} columns")
        return df
    
    except Exception as e:
        logger.exception(f"Unexpected error in transform(): {e}")

def get_bigq_client():
    credentials = service_account.Credentials.from_service_account_file(
        BIGQUERY_KEY_PATH,
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    return bigquery.Client(credentials=credentials, project=BIGQUERY_PROJECT_ID)

# def test_bigq_connection():
#     """
#     Test connection to BigQuery by querying public dataset
#     """
#     client = get_bigq_client()

#     # # credentials from bigquery
#     # key_path = "./inlaid-particle-359102-118bdae159e1.json"

#     # # create credentials from the json
#     # credentials = service_account.Credentials.from_service_account_file(
#     #     key_path,
#     #     scopes = ["https://www.googleapis.com/auth/cloud-platform"],
#     # )

#     # # create object client
#     # client = bigquery.Client(credentials=credentials, project=credentials.project_id)

#     # simple query to bigq dataset
#     query = """
#         SELECT name, sum(number) as total
#         FROM `bigquery-public-data.usa_names.usa_1910_current`
#         WHERE state = 'TX'
#         GROUP BY name
#         ORDER BY total DESC
#         LIMIT 5
#     """

#     print("[INFO]: Running query from bigquery")

#     # running query and convert into pandas DF
#     df_test = client.query(query).to_dataframe()

#     print("Connection Sucessful! Query Result: ")
#     print(df_test)
#     return True

def load_to_bigq(df):
    """
    Load to DataFrame BigQuery
    """
    try:
        if df.empty:
            logger.info("No data to load")
            return

        client = get_bigq_client()

        dataset_id = f"{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET}"
        table_id = f"{dataset_id}.{BIGQUERY_TABLE}"

        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "asia-southeast2"
        client.create_dataset(dataset, exists_ok=True)

        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        )

        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()

        logger.info(f"[INFO]: Load {len(df)} rows into {table_id}")
        logger.info(f"[INFO]: Data successfully loaded to BigQuery!")

    except Exception as e:
        logger.exception(f"Unexpected error in transform(): {e}")
    

if __name__ == "__main__":
    repos = extract()
    df = transform(repos)
    load_to_bigq(df)
    logger.info("[INFO]: Pipeline completed successfully")