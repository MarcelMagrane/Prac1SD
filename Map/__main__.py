from cos_backend import COS_Backend
import countwords


def main(args):
    conf = args.get("credentials")
    fileread = args.get("text")
    cos = COS_Backend(conf)
    
    result = countwords.main(conf, fileread)
    
    f = open("countw.txt", "w")
    f.write(str(result))
    f = open("countw.txt", 'r')
    cos.put_object('sdprac1', "countw.txt", f.read())
        
    return {}