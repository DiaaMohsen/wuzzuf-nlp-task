from ibm_watson import ToneAnalyzerV3
from ibm_watson.tone_analyzer_v3 import ToneInput
from ibm_cloud_sdk_core.authenticators import IAMAuthenticator

from elasticsearch import Elasticsearch

import pandas as pd
import numpy as np
import json


authenticator = IAMAuthenticator('ADD-URS')
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
	#ALERT: UNCOMMENT NEXT LINE
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

	return json.dumps(modified_hotel_obj)

def initialize_es_instance():
	return Elasticsearch(['localhost:9200'])