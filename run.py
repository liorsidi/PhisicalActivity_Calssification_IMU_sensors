import os
from math import log
import pandas as pd
from pyspark.sql import SparkSession
from sklearn.datasets import load_iris
from operator import add
import numpy as np

from pyspark.ml.classification import DecisionTreeClassifier, RandomForestClassifier, GBTClassifier, LinearSVC, \
    NaiveBayes


def extract_features_subject(spark, subject_file):
    '''
    the function compute features and return dataframe
    :param spark:
    :param subject_file:
    :return: Dataframe
    '''    
    df = spark.read.csv(subject_file, header=False, sep=' ')
    return df


def split_dataset(spark, subject_features, split_rate, subject_file):
    '''
    check if file exist if so load, other use subject feature for split
    :param spark:
    :param subject_features:
    :param split_rate:
    :param subject_file:
    :return: train, test
    '''
    num = subject_features.count()
    train = subject_features[0:round(num*split_rate)]
    tests = subject_features[round(num*split_rate):]
    return train, test


def evaluate_model(test, preds):
    """
    returns metrics of the predictions
    :param test:
    :param preds:
    :return:
    """
    pass



os.environ[
    'HADOOP_USER_NAME'] = 'lior'  # to avoid Permissio denied: user=root, access=WRITE, inode="/user":hdfs:supergroup:dr
spark = SparkSession \
    .builder \
    .appName('dstamp') \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .config("spark.dynamicAllocation.maxExecutors", "6") \
    .enableHiveSupport() \
    .getOrCreate()

home_path = os.path.join('res', 'data')
subjects = ['subject101']
models = [dict(name='DT', params={}, model_class=DecisionTreeClassifier),
          dict(name='RF', params={}, model_class=RandomForestClassifier),
          dict(name='GBT', params={}, model_class=GBTClassifier),
          dict(name='SVC', params={}, model_class=LinearSVC),
          dict(name='NB', params={}, model_class=NaiveBayes)]
subjects_similarity_data = ['SubjectInformation.csv', 'activitySummary.csv']
split_rate = 0.8
results = []
subjects_best_models = {}
subjects_models_class_results = {}

for subject in subjects:
    subject_file = home_path + subject
    subject_features = extract_features_subject(spark, subject_file)
    train, test = split_dataset(spark, subject_features, split_rate, subject_file)
    best_w_f1 = 0
    best_w_auc = 0
    for model in models:

        subject_model = model['model_class'](**model['params'])
        subject_model.fit(train)
        subject_model.save(subject_file)

        preds = subject_model.predict(test)
        result = evaluate_model(test, preds)
        result['subjectId'] = subject
        result['model'] = model['name']
        subjects_models_class_results[subject] = result['classes_res']
        results.append(result)

        if result['best_w_auc'] > best_w_auc:
            subjects_best_models[subject] = {}
            subjects_best_models[subject]['subject_model'] = subject_model
            subjects_best_models[subject]['w_auc'] = best_w_auc
            subjects_best_models[subject]['auc_per_class'] = best_w_auc


def calc_users_similarity():
    """
    compute similarity values for each users
    :return:
    """
    pass


subjcts_similarity = calc_users_similarity()
for subject in subjects:
    train, test = split_dataset(spark, subject_features, split_rate)

    # Weigted confidence = class_confidence*class_performance*user_similarit take max

    # majority voting =

    # top 3 majority voting



subjects_groups = ['subject101']
