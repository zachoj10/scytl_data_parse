import argparse
from datetime import datetime
from io import BytesIO
import json
import xml.etree.ElementTree as ET
import zipfile

import requests

from google.cloud import bigquery
from google.api_core.exceptions import Conflict
from google.oauth2 import service_account


def parse_file(file):
    """Parse xml based file to extract fields of interest"""
    output = None

    parse_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    root = ET.fromstring(file)

    county_dict = {}
    county_votes = []

    for child in root:
        if child.tag == "Timestamp":
            child.text

        if child.tag == "ElectionVoterTurnout":
            counties = child[0]

            for county in counties:
                data = county.attrib
                name = data["name"]

                county_dict[name] = data
        if child.tag == "Contest" and child.attrib["text"] in (
            "Governor and Lieutenant Governor",
            "Secretary of State",
            "President and Vice President of the United States",
            "US Senator",
        ):

            office = child.attrib["text"]

            for choice in child:
                cand_votes = {}

                if choice.tag == "ParticipatingCounties":
                    continue

                cand_name = choice.attrib["text"].title()
                cand_party = choice.attrib["party"]

                if "WITHDREW" in cand_name:
                    continue

                for vote_type in choice:
                    for county in vote_type:
                        county_name = county.attrib["name"]
                        if county_name in cand_votes.keys():
                            cand_votes[county_name] += int(county.attrib["votes"])
                        else:
                            cand_votes[county_name] = int(county.attrib["votes"])

                        result = {
                            "scraped_time": parse_time,
                            "office": office,
                            "ballots_cast": county_dict[county_name]["ballotsCast"],
                            "reg_voters": county_dict[county_name]["totalVoters"],
                            "cand_name": cand_name,
                            "cand_party": cand_party,
                            "county_name": county_name,
                            "votes": cand_votes[county_name],
                        }

                        county_votes.append(result)

    output = county_votes

    return output


def get_current_version(election_id: str, state_code: str) -> str:
    """Checks the base website to get the current data version number for later
    data download"""

    current_version_url = f"https://results.enr.clarityelections.com//{state_code}/{election_id}/current_ver.txt"

    r = requests.get(current_version_url)
    r.text

    return r.text


def download_summary_file(election_id: str, state_code: str, current_ver: str):
    """Downloads zip file with most recent county-level election results"""

    summary_url = f"https://results.enr.clarityelections.com//{state_code}//{election_id}/{current_ver}/reports/detailxml.zip"

    r = requests.get(summary_url)
    data = r.content

    return data


def unzip_data(source_data):
    """Unzip source data and get detail.xml file (the only file ever present)"""
    zip_data = BytesIO()
    zip_data.write(source_data)
    results_zip = zipfile.ZipFile(zip_data)
    results_file = results_zip.open("detail.xml")

    return results_file.read()


def get_bq_client():
    """Get BigQuery Client"""

    key_path = "key.json"

    credentials = service_account.Credentials.from_service_account_file(
        key_path,
        scopes=["https://www.googleapis.com/auth/bigquery"],
    )
    client = bigquery.Client(
        credentials=credentials,
        project=credentials.project_id,
    )

    return client


def create_ensure_bq_table(client, table_id):
    """Attempt to create table in BQ if it does not exist"""

    schema = [
        bigquery.SchemaField("scraped_time", "TIMESTAMP"),
        bigquery.SchemaField("office", "STRING"),
        bigquery.SchemaField("ballots_cast", "NUMERIC"),
        bigquery.SchemaField("reg_voters", "NUMERIC"),
        bigquery.SchemaField("cand_name", "STRING"),
        bigquery.SchemaField("cand_party", "STRING"),
        bigquery.SchemaField("county_name", "STRING"),
        bigquery.SchemaField("votes", "NUMERIC"),
    ]

    table = bigquery.Table(table_id, schema=schema)

    try:
        table = client.create_table(table)
        print(
            "Created table {}.{}.{}".format(
                table.project, table.dataset_id, table.table_id
            )
        )
    except Conflict as e:
        if "Already Exists" in str(e):
            print(f"{table_id} already exists")
        else:
            raise e


def upload_to_bq(client, data, table_id):
    """Streaming insert of scraped data to BQ"""
    to_upload = json.dumps(data[0:10])

    errors = client.insert_rows_json(table_id, data)
    if errors == []:
        print("New rows have been added.")
    else:
        print("Encountered errors while inserting rows: {}".format(errors))


def main():
    parser = argparse.ArgumentParser(description="Process some integers.")
    parser.add_argument(
        "--election_id", required=True, help="Election ID to Scrape. Ex: 106379"
    )
    parser.add_argument(
        "--state_code", required=True, help="State Code to Scrape. Ex: KY"
    )

    args = parser.parse_args()

    election_id = args.election_id
    state_code = args.state_code

    current_version = get_current_version(election_id, state_code)

    zip_data = download_summary_file(election_id, state_code, current_version)

    unzipped = unzip_data(zip_data)

    parsed = parse_file(unzipped)

    bq_client = get_bq_client()

    state_code_lower = state_code.lower()
    table_id = f"scytl-test-data.election_results_data.{state_code_lower}_elec_results"

    create_ensure_bq_table(bq_client, table_id)

    upload_to_bq(bq_client, parsed, table_id)


if __name__ == "__main__":

    main()
