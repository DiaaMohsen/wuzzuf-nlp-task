from ibm_watson import ToneAnalyzerV3
from ibm_watson.tone_analyzer_v3 import ToneInput
from ibm_cloud_sdk_core.authenticators import IAMAuthenticator
import json
import pandas as pd
from flask import Flask,request
from elasticsearch import Elasticsearch
import numpy as np

app = Flask(__name__)                    


authenticator = IAMAuthenticator('')
url = 'https://api.eu-gb.tone-analyzer.watson.cloud.ibm.com'

service = ToneAnalyzerV3(version='2017-09-21', authenticator=authenticator)
service.set_service_url(url)

def ibm_tone_analyzer(text):
	res_dir = service.tone(tone_input=text, content_type="text/plain").get_result()
	return res_dir

def get_hotel_tones(hotel_reviews_df):
	tones_dir = {}
	for review in hotel_reviews_df['reviews.text']:
		tones_analyzer_res = ibm_tone_analyzer(review)
		tones_tup = tones_analyzer_res['document_tone']['tones']

		for t in tones_tup:
			if t['tone_name'] not in tones_dir.keys():
				tones_dir[t['tone_name']] = []
			tones_dir[t['tone_name']].append(t['score'])

	tones_scores = {}
	for k in tones_dir.keys():
		tones_scores[k] = sum(tones_dir[k])/len(tones_dir[k])
	
	return tones_scores

@app.route('/get_hotel_tone_analyzer', methods=['GET'])
def get_hotel_tone_analyzer():
	hotel_name = request.get_json().get('hotel_name')
	all_reviews_df = pd.read_csv('7282_1.csv')

	# Ask for only Hotels or just to be mentioned
	hotel_reviews_df = all_reviews_df[all_reviews_df['name']==hotel_name]
	
	#REMOVE IT AFTER U FINISH IT ALL
	hotel_reviews_df = hotel_reviews_df.head(1)
	
	tones_scores = get_hotel_tones(hotel_reviews_df)
	return json.dumps(tones_scores)


###############################################################################

#DON"T FOTGET TO UNCOMMENT CALLING IBM WATSON HERE
def reformat_hotel_reviews(hotel_reviews_df):#'Hotel Olcott'):
	modified_hotel_obj = {}
	
	# adding hotel data to the new hotel obj and handling missed data just by setting it either to 0.0s or empty str for nw
	col_names = ['address', 'categories', 'city', 'country', 'latitude', 'longitude', 'name', 'postalCode', 'province']
	for cn in col_names:
		if cn in ['latitude', 'longitude'] and np.isnan(hotel_reviews_df.iloc[0][cn]):
			modified_hotel_obj[cn] = 0.0
		elif cn in ['address', 'categories', 'city', 'country', 'name', 'postalCode', 'province'] and type(hotel_reviews_df.iloc[0][cn]) == float:
			modified_hotel_obj[cn] = ''
		else:
			modified_hotel_obj[cn] = hotel_reviews_df.iloc[0][cn]
	
	# getting hotel tones using ibm_watson
	modified_hotel_obj['tones'] = get_hotel_tones(hotel_reviews_df)

	# calculate avg_mean_ratings for a hotel for missed values and hotel has no rates then set it to 0.0s
	avg_rating_for_missed_values = hotel_reviews_df['reviews.rating'].mean(skipna=True) if hotel_reviews_df['reviews.rating'].count() else 0.0
	
	# iterate over all hotel reviews and create list of it to be later added under hotel obj as reviews
	# handling missed values just by set it to empty str
	reviews = []
	for i, r in hotel_reviews_df.iterrows():
		review = {
			'date': r['reviews.date'] if type(r['reviews.date']) != float else r['reviews.dateAdded'], 
			'dateAdded': r['reviews.dateAdded'], 
			'doRecommend': r['reviews.doRecommend'] if type(r['reviews.doRecommend']) != float else '', 
			'id': r['reviews.id'] if type(r['reviews.id']) != float else '', 
			'rating': r['reviews.rating'] if not np.isnan(r['reviews.rating']) else avg_rating_for_missed_values, 
			'text': r['reviews.text'] if type(r['reviews.text']) != float else '', 
			'title': r['reviews.title'] if type(r['reviews.title']) != float else '', 
			'userCity': r['reviews.userCity'] if type(r['reviews.userCity']) != float else '', 
			'username': r['reviews.username'] if type(r['reviews.username']) != float else '', 
			'userProvince': r['reviews.userProvince'] if type(r['reviews.userProvince']) != float else '',  
		}
		reviews.append(review)
	
	modified_hotel_obj['reviews'] = reviews

	res = json.dumps(modified_hotel_obj)
	return res

def initialize_create_es_instance():
	es=Elasticsearch(['localhost:9200'])
	for idx in es.indices.get_alias().keys():
		es.indices.delete(index=idx, ignore=[400, 404])

	return es

@app.route('/index_hotels_into_es', methods=['GET'])
def index_into_es():

	es = initialize_create_es_instance()

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

	es=Elasticsearch(['localhost:9200'])
	r = es.search(index="hotel-reviews-index", body={"query": {"match": {"name": hotel_name}}})
	return json.dumps(r["hits"]["hits"][0]["_source"])

app.run(debug=True, port=5000)