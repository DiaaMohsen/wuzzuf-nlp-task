This repo is for wuzzuf-task for ml/nlp engineer vacancy


This repo contains 3 end-points in flask_server.py:
    - `/get_hotel_tone_analyzer`: call ibm_watson tone-analyzer for a GIVEN hotel reviews
    - `/index_hotels_into_es`: index hotel data + tones from the above api into elasticseach
    - `/get_hotel_data`: retrieve GIVEN hotel data from elasticsearch

This repo plays with ibm_watson, elasticsearch and hotels-reviews dataset (https://www.kaggle.com/datafiniti/hotel-reviews)

TODO:
    - Instead of filtering data with `categories==Hotels`, use regex to include any other rows which contains `Hotels` combined with other values
    - Replace missed values with other approaches
    - Play with hotel-reviews dataset and elasticseach
    
I followed this link to install ES: https://jee-appy.blogspot.com/2019/09/python-elasticsearch-example.html and running conda env. with py_37 and u would just need to install flask, elasticsearch

