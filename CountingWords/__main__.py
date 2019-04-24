from cos_backend import COS_Backend
import wordcount


def main(args):
    conf = args.get("credentials")
    fileread = args.get("text")
    order = args.get("order")
    minimum = args.get("filemin")
    maximum = args.get("filemax")
    
    cos = COS_Backend(conf)
    
    result = wordcount.main(conf, fileread, minimum, maximum)
    
    f = open("wc{0}.txt".format(order), "w")
    f.write(str(result))
    f = open("wc{0}.txt".format(order), 'r')
    cos.put_object('sdprac1', "wc{0}.txt".format(order), f.read())
        
    return {}