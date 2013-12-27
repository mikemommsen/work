# mike mommsen
# script for splitting my databases into counties so things can actually work
# this stuff is pretty simple for the most part and probably not usable for anyone else
# but hey, i use it so why not include it

import sys
import leveldb
import os
import time
    

def folderIterator(indir, startcounty= '56025', endcounty=None, startblock=None,PROBLEMS=PROBLEMS):
    """function to iterate over a folder with a database for each county just like a single database"""
    #includes start, excludes endcoutny
    #startcounty=None
    files = os.listdir(indir)
    files = sorted(files) # make sure that we dont need sort function
    db = None
    if startcounty:
        startcountindex = files.index(startcounty)
        files = files[startcountindex:]
    print files
    if endcounty:
        endcountyindex = files.index(startcounty)
        files = files[:endcountyindex]
    files = (os.path.join(indir, d) for d in files)
    for database in files:
        if db: 
            del db
        db = leveldb.LevelDB(database)
        if startblock:
            db_iter = db.RangeIter(startblock)
            startblock = None
        else:
            db_iter = db.RangeIter()
        for key, value in db_iter:
            yield key, value

def run(indatabase, outdir):
    """function to split a full database into a directory with one database for each county"""
    t1 = time.time()
    db = leveldb.LevelDB(indatabase)
    start = None#'36053'
    end = None#'36054'
    db_iter = db.RangeIter(start, end)
    prevcounty = None
    batch = None
    outdb = None
    for count, (key, value) in enumerate(db_iter):
        county = key[:5]
        state = key[:2]
        
        #if county != prevcounty:
            #print county
        #if state not in STATES:
            #prevcounty = county
        #    continue
        #else: pass # print state
        count += 1
        if count % 500 == 0:
            outdb.Write(batch,sync=True)
            batch = leveldb.WriteBatch()
            #print count
        if county != prevcounty:
            if batch and outdb:
                outdb.Write(batch, sync=True)
            del outdb
            outdb = leveldb.LevelDB(os.path.join(outdir, county))
            batch = leveldb.WriteBatch()
            print prevcounty, count, time.time() - t1
            prevcounty = county
        batch.Put(key, value)
    else:
        outdb.Write(batch,sync=True)

def main():
    # if main is called this splits the input database into output by calling run
    indatabase = sys.argv[1]
    outdir = sys.argv[2]
    if not os.path.exists(outdir):
        os.mkdir(outdir)
    run(indatabase, outdir)

if __name__ == '__main__':
    main()

