import json
import pandas as pd
from flask import Flask,request
import numpy as np
from utils import get_hotel_tones, reformat_hotel_reviews, initialize_es_instance

app = Flask(__name__)                    

@app.route('/get_hotel_tone_analyzer', methods=['GET'])
def get_hotel_tone_analyzer():
	hotel_name = request.get_json().get('hotel_name')
	all_reviews_df = pd.read_csv('7282_1.csv')

	# Ask for only Hotels or just to be mentioned
	hotel_reviews_df = all_reviews_df[all_reviews_df['name']==hotel_name]
	
	#REMOVE IT AFTER U FINISH IT ALL AS IT'S JUST FOR TESTING INSTEAD OF RUNNING ON ALL
	# hotel_reviews_df = hotel_reviews_df.head(1)
	
	tones_scores = get_hotel_tones(hotel_reviews_df)
	return json.dumps(tones_scores)

@app.route('/index_hotels_into_es', methods=['GET'])
def index_into_es():

	es = initialize_es_instance()
	for idx in es.indices.get_alias().keys():
		es.indices.delete(index=idx, ignore=[400, 404])

	all_reviews_df = pd.read_csv('7282_1.csv')
	hotels_reviews_df = all_reviews_df[all_reviews_df['categories']=='Hotels']
	
	for hname, hotel_reviews_df in hotels_reviews_df.groupby('name'):
		reformatted_hotel_obj = reformat_hotel_reviews(hotel_reviews_df)

		print('Indexing hotel: %s' %hname)
		es.index(index='hotel-reviews-index', body=reformatted_hotel_obj)
		
	msg = 'Finished indexing all hotels data into ES.'
	return msg

# @app.route('/index_hotels_into_es', methods=['GET'])
@app.route('/get_hotel_data', methods=['GET'])
def get_hotel_from_es():
	hotel_name = request.get_json().get('hotel_name')

	es = initialize_es_instance()
	r = es.search(index="hotel-reviews-index", body={"query": {"match": {"name": hotel_name}}})
	return json.dumps(r["hits"]["hits"][0]["_source"])

app.run(debug=True, port=5000)