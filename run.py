import os
from math import log
import pandas as pd
from pyspark.sql import SparkSession
from sklearn.datasets import load_iris
from operator import add
import numpy as np
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.functions import  max, avg, variance
from pyspark.ml.classification import DecisionTreeClassifier, RandomForestClassifier, GBTClassifier, LinearSVC, \
    NaiveBayes


def extract_features_subject(spark, subject_file):
    '''
    the function compute features and return dataframe

    remove all 0 activity
    remove all accelometer 6
    fillna with last value

    extract simple features: x, y, z, x^2,  y^2, z^2, xy, xz, y*z
    extract window features: for each feature copute: min, max, avg, std
    :param spark:
    :param subject_file:
    :return: Dataframe
    '''
    df = spark.read.csv(subject_file + '.dat', header=False, sep=' ',inferSchema=True)
    # Change NaN to null
    cols = [F.when(~F.col(x).isin("NULL", "NA", "NaN"), F.col(x)).alias(x)  for x in df.columns]
    df = df.select(*cols)
    df = df.withColumnRenamed('_c3', 'hand_temp')
    df = df.withColumnRenamed('_c20', 'chest_temp')
    df = df.withColumnRenamed('_c37', 'ankle_temp')

    #remove 0 activity
    df = df.filter(df['_c1'] != 0)
    #fill heart rate
    df = df.fillna(-1.0, subset=['_c2'])
    #   df = df.fillna('null')
    window = Window.partitionBy('_c1').orderBy('_c0').rowsBetween(-10,10)
    hr_filled = max(df['_c2']).over(window)
    df = df.withColumn('hr', hr_filled)
    accels = ['_c4','_c5', '_c6', '_c21' ,'_c22','_c23','_c38','_c39','_c40']
    gyros = ['_c10','_c11','_c12','_c27','_c28','_c29','_c44','_c45','_c46']
    mags = ['_c13','_c14','_c15','_c30','_c31','_c32','_c47','_c48','_c49']
    lab = ['hand_x', 'hand_y', 'hand_z','chest_x', 'chest_y', 'chest_z','ankle_x', 'ankle_y', 'ankle_z']
    for i, accel in enumerate(accels):
    df = df.withColumn('accel_{}_avg'.format(lab[i]),avg(df[accel]).over(window))
    for i, accel in enumerate(accels):
    df = df.withColumn('accel_{}_var'.format(lab[i]), variance(df[accel]).over(window))   
    for i, gyro in enumerate(gyros):
    df = df.withColumn('gyro_{}_avg'.format(lab[i]), avg(df[gyro]).over(window))
    for i, gyro in enumerate(gyros):
    df = df.withColumn('gyro_{}_var'.format(lab[i]), variance(df[gyro]).over(window))
    for i, mag in enumerate(mags):
    df = df.withColumn('mag_{}_avg'.format(lab[i]), avg(df[mag]).over(window))
    for i, mag in enumerate(mags):
    df = df.withColumn('mag_{}_var'.format(lab[i]), variance(df[mag]).over(window))
    out_cols = ['_c0', '_c1', 'hand_temp', 'chest_temp', 'ankle_temp'] +df.columns[54:]
    new_df = df.select(out_cols)
    new_df = new_df.withColumnRenamed('_c0', 'timestamp')
    new_df = new_df.withColumnRenamed('_c1', 'label')
    return new_df


def split_dataset(spark, subject_features, split_rate, subject_file):
    '''
    check if file exist if so load, other use subject feature for split
    :param spark:
    :param subject_features:
    :param split_rate:
    :param subject_file:
    :return: train, test
    '''
    train, test = a.randomSplit([split_rate, 1-split_rate])
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
    'HADOOP_USER_NAME'] = 'sidi'  # to avoid Permissio denied: user=root, access=WRITE, inode="/user":hdfs:supergroup:dr
spark = SparkSession \
    .builder \
    .appName('dstamp') \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .config("spark.dynamicAllocation.maxExecutors", "6") \
    .enableHiveSupport() \
    .getOrCreate()

home_path = os.path.join('PhisicalActivity_Calssification_IMUsensors','res', 'data')
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
    subject_file = os.path.join(home_path,subject)
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
