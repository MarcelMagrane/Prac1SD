from cos_backend import COS_Backend

def main(conf, fileread, minimum, maximum):
    
    cos = COS_Backend(conf)
    
    a = cos.get_object('sdprac1', fileread, extra_get_args={'Range': 'bytes={0}-{1}'.format(minimum, maximum)})
    text = a.decode('utf-8', 'replace')
    
    text = text.replace(',', '')
    text = text.replace('.', '')
    
    text = text.split()
    
    d = dict()
    
    for word in text:
        if word in d:
            d[word] += 1
        else:
            d[word] = 1
    
    return d
