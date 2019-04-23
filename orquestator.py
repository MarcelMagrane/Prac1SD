import sys

from cos_backend import COS_Backend
from ibm_cf_connector import CloudFunctions
import yaml

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
    f = open(fileread, 'r')
    cos.put_object('sdprac1', fileread, f.read())
    
    f = open('wordcount.zip', 'rb')
    cloud.create_action('wordcount', f.read())
    
    #f = open('reducer.zip', 'rb')
    #cloud.create_action('reduce', f.read)
    
    #f = open('countwords.zip', 'rb')
    #cloud.create_action('countwords', f.read)
    
    #Create partitions of the data file to feed the different workers
    filesize=int(cos.head_object('sdprac1', fileread)['content-length'])
    print(filesize)
    
    partitionsize = int(round(filesize / partitions))
    minimum = 0
    maximum = partitionsize
    x = 1
    
    #Initialize action wordcount
    while x <= partitions:
        a=cos.get_object('sdprac1', fileread, extra_get_args={'Range': 'bytes={0}-{1}'.format(minimum, maximum)})
        text = a.decode('utf-8')
        #print(text)
        print('-------------------------------------------------------------------------------------')
        cloud.invoke('wordcount', {"credentials":res['ibm_cos'], "text":text, "order":x})
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
        print("Not yet...")
        
    
    
    