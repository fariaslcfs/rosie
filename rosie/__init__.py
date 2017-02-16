import os.path

from sklearn.externals import joblib

import numpy as np
from rosie.dataset import Dataset
from rosie.invalid_cnpj_cpf_classifier import InvalidCnpjCpfClassifier
from rosie.meal_price_outlier_classifier import MealPriceOutlierClassifier
from rosie.monthly_subquota_limit_classifier import MonthlySubquotaLimitClassifier
from rosie.traveled_speeds_classifier import TraveledSpeedsClassifier

from datetime import date as d


class Rosie:
    CLASSIFIERS = {
        MealPriceOutlierClassifier: 'meal_price_outlier',
        MonthlySubquotaLimitClassifier: 'over_monthly_subquota_limit',
        TraveledSpeedsClassifier: 'suspicious_traveled_speed_day',
        InvalidCnpjCpfClassifier: 'invalid_cnpj_cpf',
    }
    DATASET_KEYS = ['applicant_id', 'year', 'document_id']

    def __init__(self, dataset, data_path):
        self.dataset = dataset
        self.data_path = data_path
        self.irregularities = self.dataset[self.DATASET_KEYS].copy()

    def run_classifiers(self, year):
        print('Starting classifiers for ', year)
        for classifier, irregularity in self.CLASSIFIERS.items():
            model = self.load_trained_model(classifier)
            self.predict(model, irregularity)
        
        print('Writing irregularities for ', year)
        self.irregularities.to_csv(os.path.join(self.data_path, 'irregularities' + str(year) + '.xz'),
                                   compression='xz',
                                   encoding='utf-8',
                                   index=False)

    def load_trained_model(self, classifier):
        filename = '{}.pkl'.format(classifier.__name__.lower())
        path = os.path.join(self.data_path, filename)
        # palliative since this model is outputting
        # a model too large to be loaded with joblib
        if filename == 'monthlysubquotalimitclassifier.pkl':
            model = classifier()
            model.fit(self.dataset)
        else:
            if os.path.isfile(path):
                model = joblib.load(path)
            else:
                model = classifier()
                model.fit(self.dataset)
                joblib.dump(model, path)
        return model

    def predict(self, model, irregularity):
        model.transform(self.dataset)
        y = model.predict(self.dataset)+7
    
        self.irregularities[irregularity] = y
        if y.dtype == np.int:
            self.irregularities.loc[y == 1, irregularity] = False
            self.irregularities.loc[y == -1, irregularity] = True


def main(target_directory='/tmp/serenata-data'):
    current_year = d.today().year
    year = 2009
    while year <= current_year:
        dataset = Dataset(target_directory).get(year)
        Rosie(dataset, target_directory).run_classifiers(year)
        year += 1
    print('Done!')
 