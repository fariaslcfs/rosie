import os
from serenata_toolbox.datasets import fetch
from serenata_toolbox import ceap_dataset as c
from serenata_toolbox import reimbursements as r
from datetime import date as d

import numpy as np
import pandas as pd
from os.path import isfile


class Dataset:
    
    COMPANIES_DATASET = '2016-09-03-companies.xz'
    
    FILE_BASE_NAME = 'reimbursements.xz'

    def __init__(self, path):
        self.path = path
        pd.options.mode.chained_assignment = None  # default='warn'

    def get(self, year):
        self.update_datasets()    
        reimbursements = self.get_reimbursements(year)
        companies = self.get_companies()
        ds = pd.merge(reimbursements, companies,
                      how='left',
                      left_on='cnpj_cpf',
                      right_on='cnpj')
        return ds

    def update_datasets(self):
        os.makedirs(self.path, exist_ok=True)
        ceap = c.CEAPDataset(self.path)
        # if we don't check below, for every loop in the main method of Rosie all those methods will be repeated
        # necessary to check if files were already fetched
        # necessary to know if files were already converted
        # necessary to know if files were already translated
        # necessary to know if reimbursements were already generated
        # necessary to know if companies data were already fetched
        counter = 0
        for f in ['AnoAtual.xml', 'AnoAnterior.xml', 'AnosAnteriores.xml']:
            f_path = os.path.join(self.path, f)
            if os.path.exists(f_path):
                counter += 1
        if counter < 3:
            ceap.fetch()
        
        counter = 0
        for f in ['AnoAtual.csv', 'AnoAnterior.csv', 'AnosAnteriores.csv']:
            f_path = os.path.join(self.path, f)
            if os.path.exists(f_path):
                counter += 1
        if counter < 3:
            ceap.convert_to_csv()
        
        counter = 0
        for f in ['current-year.xz', 'last-year.xz', 'previous-years.xz']:
            f_path = os.path.join(self.path, f)
            if os.path.exists(f_path):
                counter += 1
        if counter < 3:
            ceap.translate()
             
        ceap.clean()
        
        f_path = os.path.join(self.path, self.COMPANIES_DATASET)
        if not os.path.exists(f_path):
            fetch(self.COMPANIES_DATASET, self.path)

    def get_reimbursements(self, year):
        if year == None:
            file_name = self.FILE_BASE_NAME
        else:
            file_name = self.FILE_BASE_NAME[:-3] + '_' + str(year) + '.xz'
        dataset = pd.read_csv(os.path.join(self.path, file_name),
                    dtype={'applicant_id': np.str,
                           'cnpj_cpf': np.str,
                           'congressperson_id': np.str,
                           'subquota_number': np.str},
                    low_memory=False)
        dataset['issue_date'] = pd.to_datetime(dataset['issue_date'], errors='coerce')
        return dataset

    def get_companies(self):
        is_in_brazil = ('(-73.992222 < longitude < -34.7916667) & '
                        '(-33.742222 < latitude < 5.2722222)')
        dataset = pd.read_csv(os.path.join(self.path, self.COMPANIES_DATASET),
                              dtype={'cnpj': np.str},
                              low_memory=False)
        dataset = dataset.query(is_in_brazil)
        dataset['cnpj'] = dataset['cnpj'].str.replace(r'\D', '')
        return dataset
