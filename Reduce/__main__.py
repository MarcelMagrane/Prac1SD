from cos_backend import COS_Backend
import reduce
from ast import literal_eval

def main(args):
    conf = args.get("credentials")
    elements = args.get("elements")
    cos = COS_Backend(conf)
    dictlist = [dict() for x in range(elements)]
    x = 1
    while x <=elements:
        text = cos.get_object('sdprac1', "wc{0}.txt".format(x))
        text = text.decode('utf-8')
        
        dictlist[x-1] = literal_eval(text)
        x += 1
        
    finaldict = reduce.main(dictlist)
        
    f = open("reduce.txt", "w")
    f.write(str(finaldict))
    f = open("reduce.txt", 'r')
    cos.put_object('sdprac1', "reduce.txt", f.read())
    
    return {}