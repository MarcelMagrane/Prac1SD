from cos_backend import COS_Backend

def main(conf, fileread):
    
    cos = COS_Backend(conf)
    
    text = cos.get_object('sdprac1', fileread)
    text = text.decode('utf-8', 'replace')
    text = text.replace(',', '')
    text = text.replace('.', '')
    
    number = len(text.split())
    
    return number