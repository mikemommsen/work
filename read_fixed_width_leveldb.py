import leveldb
import json
import sys
import time
from collections import OrderedDict

GEOGLIST = ['statea','countya','tracta','blkgrpa','blocka']

def creategeoid(inrow, schema, geog):
    """creates id based off of geography"""
    geogindex = GEOGLIST.index(geog) + 1
    keygeogs = GEOGLIST[:geogindex]
    mystr = ''
    for val in keygeogs:
        mystr += readfield(inrow, schema, val)
    return mystr

def readfield(inrow, schema, field):
    """uses schema and a field to return value for a row
    it also strips off the blank space"""
    x = schema[field]
    start_pos = x['start_pos']
    end_pos = x['end_pos']
    a = inrow[start_pos: end_pos]
    a = a.strip(' ')
    if x['datatype'] == int:
        try:
            a = int(a)
        except ValueError as v:
            print v, 'should be int', a
            a = 'bigError'
    else:
        try:
            a = a.decode('latin1')
            a = a.encode('utf8')
        except Exception as e:
            print a
            a = 'something with a tilde'
    return a

def readrow(inrow, schema, geog):
    """takes a row and feeds each value to the readfield function
    also creates the key for that row by sending data to creategeoid"""
    mylist = []
    for field in schema:
        mylist.append(readfield(inrow, schema, field))
    key = creategeoid(inrow, schema, geog)
    return key, mylist

def readfixed(infile, schema, dbpath, geog):
    """loops through rows of infile, sends each row to readrow,
    and sends those processed rows to the output database in
    transactions of 1,000"""
    db = leveldb.LevelDB(dbpath)
    batch = leveldb.WriteBatch()
    f = open(infile)
    t1 = time.time()
    for count, r in enumerate(f):
        if r.strip(' '):
            key, val = readrow(r, schema, geog)
            try:
                invalue = db.Get(key)
                invalue = json.loads(invalue)
            except KeyError:
                invalue = []
            invalue += val
            outvalue = json.dumps(invalue)
            batch.Put(key, outvalue)
            if count % 1000 == 0:
                db.Write(batch, sync = True)
                print time.time() - t1,':', count
                del batch
                batch = leveldb.WriteBatch()
    else:
        db.Write(batch, sync = True)
    f.close()
    return True

def readdofile(infile):
    """reads the top part of a .do file to create a dictionary"""
    f = open(infile)
    mylist = [r.strip().split(' ') for r in f]
    looplist = []
    for row in mylist:
        newlist = []
        for val in row:
            if val:
                newlist.append(val)
        if newlist:
            looplist.append(newlist)
    newlist = OrderedDict()
    for a,b,c,d in looplist:
        start_pos, end_pos = map(int, c.split('-'))
        if a == 'str': 
            a = str
        else: 
            a = int
        newlist.update({b.strip('e'):{'datatype': a, 'fieldname':b, 
        'start_pos':start_pos - 1, 'end_pos': end_pos}})
    return newlist

def main():
    infixed = sys.argv[1]
    inschema = sys.argv[2]
    outdb = sys.argv[3]
    geog = sys.argv[4]
    schema = readdofile(inschema)
    a = readfixed(infixed, schema, outdb, geog)
    # this below is to update the schema 
    schemafile = outdb + '.schema'
    f = open(schemafile, 'r+')
    oldschema = json.load(f)
    outschema = oldschema + schema.keys()
    #outschema = schema.keys()
    print outschema
    f.seek(0)
    json.dump(outschema, f,indent=4)
    f.close()
    print True

if __name__ == '__main__':
    main()
