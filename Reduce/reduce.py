def main(dictlist):
    
    d = dictlist[0]
    y=1
    while y < len(dictlist):
        d = {x: d.get(x, 0) + dictlist[y].get(x, 0) for x in set(d).union(dictlist[y])}
        y += 1
    
    return d

