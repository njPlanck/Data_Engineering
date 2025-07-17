import requests
import csv

#url = url = "https://cricbuzz-cricket.p.rapidapi.com/stats/v1/rankings/batsmen"

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
    else:
        print("No data available from the API")
else:
    print("Failed to fetch data:", response.status_code)