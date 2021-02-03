#!/usr/bin/env python
# coding: utf-8

import numpy as np
import pandas as pd
import scipy.io as sio
import os
import curl
import requests
import re
from bs4 import BeautifulSoup

if not os.path.isdir(os.path.realpath('data_healthy')):
    os.mkdir(os.path.realpath('data_healthy'))

if not os.path.isdir(os.path.realpath('data_faulty')):
    os.mkdir(os.path.realpath('data_faulty'))

def download_cwru_data(dir='.'):
    """
    Download CWRU Bearing dataset
    
    Arguments
    ---------
    dir: str
        Directory where the dataset should be stored
    
    """
    
    healthy_assets_url = [
        "http://csegroups.case.edu/sites/default/files/bearingdatacenter/files/Datafiles/97.mat",
        "http://csegroups.case.edu/sites/default/files/bearingdatacenter/files/Datafiles/98.mat",
        "http://csegroups.case.edu/sites/default/files/bearingdatacenter/files/Datafiles/99.mat",
        "http://csegroups.case.edu/sites/default/files/bearingdatacenter/files/Datafiles/100.mat"
    ]
    
    for url in healthy_assets_url:
        if not os.path.isfile('./data_healthy/' + os.path.basename(url)):
            curl -s url -o os.path.realpath('data_healthy/'))

download_cwru_data()

def read_folder(folder):
    """
    Separate data from .mat files and convert into arrays.
    
     'X097_DE_time': array([[ 0.05319692],
        [ 0.08866154],
        [ 0.09971815],
        ...,
        [-0.03463015],
        [ 0.01668923],
        [ 0.04693846]]),
 'X097_FE_time': array([[0.14566727],                 (We aim to convert this)
        [0.09779636],
        [0.05485636],
        ...,
        [0.14053091],
        [0.09553636],
        [0.09019455]]),
 'X097RPM': array([[1796]], dtype=uint16)}
    
    Arguments:
    ----------
    folder: str
        Path of directory where data is stored
    
    """
    data = 'dummy'
    skip = False
    
    for file in os.listdir(folder):
        file_id = file[:-4]
        matlab_file_dict = sio.loadmat(folder+file)
        del data
        
        for key, value in matlab_file_dict.items():
            if 'DE_time' in key or 'FE_time' in key:
                a = np.array(matlab_file_dict[key])
                
                try:
                    data
                except NameError:
                    data = a
                else:
                    if (data.shape[0] != a.shape[0]):
                        print('skipping ' + file_id)
                        skip = True
                        continue
                    data = np.hstack((data,a))
        """
        When data has lots of missing entries, filling in data to maintain quality
        
        """
        if skip:
            skip = False
            continue
        id = np.repeat(file_id, data.shape[0])
        id.shape = (id.shape[0],1)
        data = np.hstack((id,data))
        if data.shape[1] == 2:
            zeroes = np.repeat(float(0),data.shape[0])
            zeroes.shape = (data.shape[0],1)
            data = np.hstack((data,zeroes))
        try:
            result
        except NameError:
            result = data
        else:
            result = np.vstack((result,data))
    return result

result_healthy = read_folder('./data_healthy/')
pdf = pd.DataFrame(result_healthy)
pdf.to_csv('result_healthy_pandas.csv', header=False, index=True)

"""
    Moving on to downloading faulty data from 3 different links;
    - 12k-drive-end-bearing-fault-data
    - 48k-drive-end-bearing-fault-data
    - 12k-fan-end-bearing-fault-data

"""

req1 = requests.get("https://csegroups.case.edu/bearingdatacenter/pages/12k-drive-end-bearing-fault-data")
soup1 = BeautifulSoup(req1.text, "lxml")

pages1 = soup1.findAll('a', href=re.compile('.*Datafiles?.*'))

req2 = requests.get("https://csegroups.case.edu/bearingdatacenter/pages/48k-drive-end-bearing-fault-data")
soup2 = BeautifulSoup(req2.text, "lxml")

pages2 = soup2.findAll('a', href=re.compile('.*Datafiles?.*'))

req3 = requests.get("https://csegroups.case.edu/bearingdatacenter/pages/12k-fan-end-bearing-fault-data")
soup3 = BeautifulSoup(req3.text, "lxml")

pages3 = soup3.findAll('a', href=re.compile('.*Datafiles?.*'))

def scrape_file(url):
    """
    Scraping CRWU website for faulty bearing dataset
    
    Arguments
    ---------
    url: str
        String from <pages> list where the faulty data is online
    
    """
    """path = url['href']
    r = requests.get(url, stream=True)
    if r.status_code == 200:
        with open(path, 'wb') as f:
            for chunk in r:
                f.write(chunk)
    """
    path=[]
    for link in url:
        if link.has_attr('href'):
            path.append(link['href'])
    return path

faulty_url1 = scrape_file(pages1)

faulty_url2 = scrape_file(pages2)

faulty_url3 = scrape_file(pages3)

faulty_url = faulty_url1 + faulty_url2 + faulty_url3

def download_faulty(dir='.'):
    """
    Download faulty bearing dataset
    
    Arguments
    ---------
    dir: str
        Directory where the dataset should be stored
    
    """
    
    for url in faulty_url:
        if not os.path.isfile('./data_faulty/' + os.path.basename(url)):
            curl -s url -o os.path.realpath('data_faulty/'))


download_faulty()

result_faulty = read_folder('./data_faulty/')
fpdf = pd.DataFrame(result_faulty)
fpdf.to_csv('result_faulty_pandas.csv', header=False, index=True)


"""
    Additionally cleaning up artifacts for memory limited machines
    and cleaning up directories for files

"""

del result_healthy
del result_faulty
del pdf
del fpdf

