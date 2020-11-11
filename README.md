# SCTYL Election Results Parser
## Background
Many states/counties use software from [sctyl](https://www.scytl.com/en/) to display live election results at the precinct or county level on election night. This script is designed to access these sites for a given state/election, download the county-level data for the state and upload to BQ

## Steps
1. Everytime new data is uploaded, it gets a new version number, so the script needs to get the most recent version number
2. With the version number, the script gets the most recent county-level data xml zip
3. Unzips data
4. Parses xml
5. Creates table in BQ for the state if it does not yet exist
6. Inserts data into BQ

Example site: https://results.enr.clarityelections.com/KY/106379/web.264614/#/summary
Example script call: `python parse_county_results.py --election_id 106379 --state_code KY`