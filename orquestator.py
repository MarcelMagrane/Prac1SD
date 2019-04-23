import sys

from cos_backend import COS_Backend
from ibm_cf_connector import CloudFunctions
import yaml
import time

if __name__ == '__main__':
    with open('ibm_cloud_config.txt', 'r') as config_file:
        res = yaml.safe_load(config_file)

    ibm_cos = res['ibm_cos']
    ibm_cf = res['ibm_cf']
    cos = COS_Backend(ibm_cos)
    cloud = CloudFunctions(ibm_cf)
    
    fileread=sys.argv[1]
    partitions=int(sys.argv[2])
    
    #Add required files to the cloud
    f = open(fileread, 'r', encoding='utf-8', errors='ignore')
    cos.put_object('sdprac1', fileread, f.read())
    
    f = open('wordcount.zip', 'rb')
    cloud.create_action('wordcount', f.read())
    
    f = open('reduce.zip', 'rb')
    cloud.create_action('reduce', f.read())
    
    f = open('countwords.zip', 'rb')
    cloud.create_action('countwords', f.read())
    
    #Create partitions of the data file to feed the different workers
    filesize=int(cos.head_object('sdprac1', fileread)['content-length'])
    print(filesize)
    
    partitionsize = int(round(filesize / partitions))
    minimum = 0
    maximum = partitionsize
    x = 1
    
    start = time.time()
    
    #Initialize action wordcount
    while x <= partitions:
        #a=cos.get_object('sdprac1', fileread, extra_get_args={'Range': 'bytes={0}-{1}'.format(minimum, maximum)})
        #text = a.decode('utf-8', 'replace')
        #print(text)
        #print('-------------------------------------------------------------------------------------')
        cloud.invoke('wordcount', {"credentials":res['ibm_cos'], "text":fileread, "order":x, "filemin": minimum, "filemax": maximum})
        minimum = maximum + 1
        maximum = maximum + partitionsize
        x += 1
        if x == partitions or maximum > filesize:
            maximum = filesize
    
    #Wait for wordcount
    print("Waiting for the {0} wordcounts to finish:".format(partitions))
    end = len(cos.list_objects('sdprac1', 'wc'))
    while end != partitions:
        end = len(cos.list_objects('sdprac1', 'wc'))
        #print("Not yet...")
        
    end = time.time()
    
    print("Time spent for wordcount with {0}: {1}".format(partitions, end - start))
    
    start = time.time()
    #Initialize action reduce
    cloud.invoke('reduce', {"credentials":res['ibm_cos'], "elements":partitions})
    
    #Wait for reduce
    print("Waiting for reduce to finish")
    end = len(cos.list_objects('sdprac1', 'reduce'))
    while end != 1:
        end = len(cos.list_objects('sdprac1', 'reduce'))
        #print("Not yet...")
    
    end = time.time()
    
    print("Time spent for reduce: {}".format(end - start))
        
    x = 1
    cos.delete_object('sdprac1', 'wc{}.txt'.format(x))
    while x != partitions: 
        x += 1
        cos.delete_object('sdprac1', 'wc{}.txt'.format(x))
        
    result=cos.get_object('sdprac1', 'reduce.txt')
    result = result.decode('utf-8', 'replace')
    showwords = input("Do you want to see the words? (Y or N)")
    if "Y" in showwords:
        print(result)
    
    cos.delete_object('sdprac1', 'reduce.txt')
    
    start = time.time()
    
    #Initialize action countwords
    cloud.invoke('countwords', {"credentials":res['ibm_cos'], "text":fileread})
    
    #Wait for countwords
    print("Waiting for countwords to finish")
    end = len(cos.list_objects('sdprac1', 'count'))
    while end != 1:
        end = len(cos.list_objects('sdprac1', 'count'))
        #print("Not yet...")
    
    end = time.time()
    
    print("Time spent for countwords: {}".format(end - start))
    
    result=cos.get_object('sdprac1', 'countw.txt')
    result = result.decode('utf-8', 'replace')
    print("The total number of words is: {}".format(result))
    
    cos.delete_object('sdprac1', 'countw.txt')
    