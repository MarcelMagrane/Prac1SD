'''
Created on 21 abr. 2019

@author: Marcel
'''
from cos_backend import COS_Backend
from ibm_cf_connector import CloudFunctions
import yaml

if __name__ == '__main__':
    with open('ibm_cloud_config.txt', 'r') as config_file:
        res = yaml.safe_load(config_file)

    ibm_cos = res['ibm_cos']
    ibm_functions = res['ibm_cf']
    
    cos = COS_Backend(ibm_cos)
    #f = open('test.txt', 'r')
    #cos.put_object('sdprac1', 'test.txt', f.read())
    
    a=cos.get_object('sdprac1', 'test.txt', extra_get_args={'Range': 'bytes=0-100'})
    string = a.decode('utf-8')
    print(string)
    b=cos.head_object('sdprac1', 'test.txt')
    print(b['content-length'])