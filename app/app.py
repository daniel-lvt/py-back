#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# standard python imports
import json
from flask import Flask, request
from flask_cors import CORS
import traceback
import os
import logging
from logging.handlers import RotatingFileHandler
import redis
from flask_restful import Api
import pandas as pd
from itertools import groupby
import re

redisIns = redis.Redis(host='localhost', port=6379, db=0)

app = Flask(__name__)
CORS(app)

api = Api(app)
handler = RotatingFileHandler('flask.log', maxBytes=10000, backupCount=1)
handler.setLevel(logging.INFO)
app.logger.addHandler(handler)
app.logger.setLevel(logging.INFO)


def filter_data(data_input, filter_by):
    filtered_data = []
    match = True
    for entry in data_input:
        for key, value in filter_by.items():
            if isinstance(value, str):
                value = value.encode('latin-1', errors='ignore').decode('latin-1')
            regex = re.search("^{}+".format(str(value)), str(entry[key]))
            if regex:
                match = True
                continue
            if str(entry[key]) != str(value):
                match = False
                break
        if match:
            filtered_data.append(entry)
    return filtered_data


@app.before_first_request
def init():
    try:
        data_search = redisIns.get('dataset')
        if data_search is None:
            path = ('{}/app/data.xlsx'.format(os.getcwd()))
            df = pd.read_excel(path)
            app.logger.info('[low] Data is saved an redis')
            redisIns.set('dataset', df.to_json(orient="records"))
    except:
        app.logger.info('[high] Error validate info redis')
        app.logger.info(traceback.format_exc())


@app.route('/api/data', methods=['GET'])
def data():
    if request.method == 'GET':
        data_redis = redisIns.get('dataset')
        if len(request.args):
            result = filter_data(json.loads(data_redis.decode()),
                                 dict(request.args))
            print(dict(request.args))
            return {"status": 200,
                    "filters": dict(request.args),
                    "data": result}
        return {'status': 200,
                "filters": [],
                "data": json.loads(data_redis.decode())}


@app.route('/api/group', methods=['GET'])
def data_group():
    if request.method == 'GET':
        data_redis = redisIns.get('dataset')
        if len(request.args) == 1:
            grouped_data = {}
            key = request.args.get('filter')
            for y, y_data in groupby(json.loads(data_redis.decode()),
                                     key=lambda x: x[key]):
                grouped_data[y] = y
            return {"status": 200,
                    "filter": dict(request.args),
                    "data": list(grouped_data.keys())}
        if len(request.args) > 1:
            return {'status': 200,
                    "data": [],
                    "message": "just one argument"}
        return {'status': 404, "data": []}, 404


if __name__ == '__main__':
    app.run(debug=True)
