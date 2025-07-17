import requests
import csv
from google.cloud import storage

from google.oauth2 import service_account

credentials = service_account.Credentials.from_service_account_file(
    r'C:/Users/User/Desktop/Projects/data-engineering/service-account-key.json'
)
storage_client = storage.Client(credentials=credentials, project='endless-theorem-465911-v8')

url = url = "https://cricbuzz-cricket.p.rapidapi.com/stats/v1/rankings/batsmen"

headers = {
	"x-rapidapi-key": "eb287e3449msha85d2835e1b3e10p1b7b0cjsn43fd4272ea2f",
	"x-rapidapi-host": "cricbuzz-cricket.p.rapidapi.com"
}

params = {
    'formatType':'odi'
}


response = requests.get(url, headers=headers, params=params)


if response.status_code == 200:
    data = response.json().get('rank', []) #extracting the rank of the data
    csv_filename = 'batsmen_ranking.csv'

    if data:
        field_names = ['rank','name','country']

        #write data to csv file with only the specified field names
        with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile,fieldnames=field_names)
           # writer.writeheader()
            
            for entry in data:
                writer.writerow({field:entry.get(field) for field in field_names})
        print(f"Data fetched successfully and written to '{csv_filename}'")

        #upload the csv file to GCS
        bucket_name = 'nj-bkt-ranking-data'
        bucket = storage_client.bucket(bucket_name)
        destination_blob_name = f'{csv_filename}'

        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(csv_filename)
        print(f"Uploaded '{csv_filename}' to bucket '{bucket_name}' as '{destination_blob_name}'")
    else:
        print("No data available from the API")
else:
    print("Failed to fetch data:", response.status_code)