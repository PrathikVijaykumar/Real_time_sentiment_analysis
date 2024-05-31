import time
from kafka import KafkaProducer
from flask import Flask, request, jsonify, render_template,redirect, url_for
import json
import requests

app = Flask(__name__)
reviews=[]

def create_kafka_producer():
    """
    Creates the Kafka producer object
    """
    return KafkaProducer(bootstrap_servers=['kafka1:19092', 'kafka2:19093', 'kafka3:19094'])

producer = create_kafka_producer()

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/submit_review', methods=['POST'])
def submit_review():
    product = request.form['product']
    review_text = request.form['review']
    if not review_text:
        return jsonify({'status': 'error', 'message': 'No review text provided'}), 400

  
    review = {'product': product, 'review_text': review_text}

    reviews.append(review)
    #return jsonify({'status': 'success', 'message': 'Review submitted successfully', 'review': review})
    return redirect(url_for('index', success_message='Review submitted successfully!'))


@app.route('/send_reviews_to_kafka', methods=['POST'])
def send_reviews_to_kafka():
    for review in reviews:
        review_json = json.dumps(review).encode('utf-8')
        producer.send('reviews', review_json)
    producer.flush()
    reviews.clear()
    return jsonify({'status': 'success', 'message': 'Reviews sent to Kafka successfully'})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)