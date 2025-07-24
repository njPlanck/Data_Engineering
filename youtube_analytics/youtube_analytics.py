import requests
from constants import YOUTUBE_API_KEY

if __name__ == "__main__":
    response = requests.get("https://www.googleapis.com/youtube/v3/videos",
                            {
                                'key':YOUTUBE_API_KEY,
                                'id':'Wi5hWa_XEck',
                                'part':'snippet,statistics,status'
                            })

    print(response.text)