import requests

def trigger_send_reviews():
    url = 'http://kafka_spark_structured_streaming-webapp-1:5000/send_reviews_to_kafka'
    response = requests.post(url)
    print(response.text)
    return response.json()

if __name__ == "__main__":
    response = trigger_send_reviews()
    print(response)
