import leveldb
import sys
from itertools import groupby
from itertools import izip
from collections import OrderedDict
from create_universes import equation
from read_interpolate import PROCESSDICT
import csv
import json
import time
from create_universes import equationlist
from verify_results import fixrow
from better_zero_division import variablestore, totalstore, tablestore, BASESORTKEY
import os


# load in some helper dictionaries

#this one has all the rules
with open('census_data/verify_rules.json') as f:
    verify_rules = json.load(f)
# this one shows which table participates in which rule
with open('census_data/methoddict.json') as f:
    methoddict = json.load(f)
# this one lists out each equation for each variable that we calculate - we dont use this though - rip straight from csv
with open('census_data/allequations.json') as f:
    allequations = json.load(f)


def sortkey(inval):
    """this is no longer needed - used to by used to sort something"""
    return inval[0][:-4]

def loaddata(data, schema, varlist=None):
    """function to take a string which represents a json list, and a python list of field values.
    it spits back a dictionary."""
    if varlist:
        return {k: v for k, v in izip(schema, json.loads(data)) if k in varlist}
    else:
        if len(schema) != len(json.loads(data)): 
            print 'error mismatch in length', len(schema), len(data)
            with open('data_out.json', 'w') as f:
                json.dump(json.loads(data), f, indent=4)
            with open('data_out_schema.json', 'w') as f:
                json.dump(schema, f, indent=4)
            sys.exit(1)
        return {k: v for k, v in izip(schema, json.loads(data))}

def dumpdata(data):
    """this is the opposite of loaddata - takes a dict and dumps out the values into a string"""
    return json.dumps(data.values())

def processrow(inrow):
    """this function has been copied over to better zero division and very possibly changed. i copied it over there because you cant have two scripts that import from each other"""
    column = inrow['istads_id']
    column = '_'.join(column.split('.'))
    inrow[1] = 1
    method, comp_mult = inrow['Method'], inrow['Denominator for Component Multiplier'].strip(' ')
    if method == '2' and comp_mult and comp_mult != 'No multiplier':
        method = '3'
    dictionary = PROCESSDICT[method]
    num = inrow[dictionary['numerator']]
    den =  inrow[dictionary['denominator']]
    mult =  dictionary['multiplier']
    mult = [inrow[x] for x in mult]
    mult = [x for x in mult if x != '']
    if mult:
        mult = ' + '.join(mult)
    else:
        mult = 1
    return '({0}) * ({1}) / ({2})'.format(mult, num, den).replace('\n', '')

def readequations(infile):
    """function to read in a csv of equations - i think this is also copied into better_zero_division.py so check out the versions there.
    although they are probably the same."""
    f = open(infile)
    reader = csv.DictReader(f)
    mylist = [r for r in reader]
    mylist = [r for r in mylist if r['Method']]
    fieldnames = reader.fieldnames
    interpolations = []
    for row in mylist:
        name = row['istads_id']
        name = '_'.join(name.split('.')) 
        if 'av' in name:
            string = processrow(row)
            interpolations.append(equation(name, string))
    f.close()
    f = open('census_data/interpolation_equations.json','w')
    outlist = {x.name: str(x.equation) for x in interpolations}
    json.dump(outlist, f, indent=4)
    f.close()
    return interpolations

from splitter import folderIterator

def iterator(intractdb, inblkgrpdb, inblockdb, blockschema, blkgrpschema, tractschema):
    """function to iterate over my databases (block, blkgrp, tracts)
    it takes the database paths and schemas and yields each block one by one"""
    #tractdb = leveldb.LevelDB(intractdb)
    #blkgrpdb = leveldb.LevelDB(inblkgrpdb)
    #blockdb = leveldb.LevelDB(inblockdb)
    #block_iter = blockdb.RangeIter('06')
    #tract_iter = tractdb.RangeIter()
    #blkgrp_iter = blkgrpdb.RangeIter()
    prevtractkey, prevblkgrpkey = None, None
    block_iter = folderIterator(inblockdb)
    tract_iter = folderIterator(intractdb)
    blkgrp_iter = folderIterator(inblkgrpdb)
    for blockkey, blockvalue in block_iter:
        blockdata = loaddata(blockvalue, blockschema)
        tractkey, blkgrpkey = blockkey[:-5], blockkey[:-4]
        if prevblkgrpkey != blkgrpkey:
            new_blkgrpkey, blkgrpvalue = blkgrp_iter.next()
            if blkgrpkey != new_blkgrpkey:
                print blkgrpkey, new_blkgrpkey
                blkgrp_iter = blkgrpdb.RangeIter(blkgrpkey)
                blkgrpkey, blkgrpvalue = blkgrp_iter.next()
            prevblkgrpkey = blkgrpkey
            blkgrpdata = loaddata(blkgrpvalue, blkgrpschema)
            if prevtractkey != tractkey:
                new_tractkey, tractvalue = tract_iter.next()
                if tractkey != new_tractkey:
                    print tractkey, new_tractkey
                    tract_iter = tractdb.RangeIter(tractkey)
                    tractkey, tractvalue = tract_iter.next()
                prevtractkey = tractkey
                tractdata = loaddata(tractvalue, tractschema)
        tractdata.update(blockdata)
        blkgrpdata.update(blockdata)
        yield (blockkey, blkgrpdata, tractdata)

def createtestoutput(intractdb, inblkgrpdb, inblockdb, blockschema, blkgrpschema, tractschema, interpolations):
    """this function was made to create a csv of the subset of the database for verification purposes"""
    # feed in some strings to narrow down what variables we want
    intable = 'ds184_dt270'
    invars = ['ds172_dt18_av7','ds172_dt19_av18',
	'ds172_dt19_av19','ds172_dt19_av3','ds172_dt19_av4',
	'ds184_dt91_av20','ds184_dt91_av20','ds184_dt91_av21',
	'ds184_dt91_av21','ds184_dt91_av26','ds184_dt91_av26',
	'ds184_dt91_av49','ds184_dt91_av50','ds184_dt91_av55']
    invars = sorted(invars, key=lambda x: (int(x.split('_')[0][2:]), int(x.split('_')[-1][2:])))
    # print out the headers
    print ','.join(invars)
    # fire up an iterator
    for key, valdict in iterator(intractdb, inblkgrpdb, inblockdb, blockschema, blkgrpschema, tractschema):
        # rip out all the data that we want
        blkgrpdata = valdict['blkgrpdata']
        blkgrpdata = {k:v for k,v in blkgrpdata.items() if k in invars}
        blockdata = valdict['blockdata']
        blockdata = {k:v for k, v in blockdata.items() if k in invars}
        blockdata.update(blkgrpdata)
        # not sure why we are printing out one at a time and not using ','.join(invars) or something like that
        for x in invars:
            print str(blockdata[x]) + ',',
        print

def run_iterator(inoutdb, intractdb, inblkgrpdb, inblockdb, blockschema, blkgrpschema, tractschema, interpolations, verify_rules, methoddict, variables):
    """this is the core function for processing.
    it creates a database iterator, sends the data to better_zero_division.py, and puts the data into an output"""
    # like below opens an output leveldb database
    #outdb = leveldb.LevelDB('testdatabase3.leveldb')

    # lines for creating one output csv - this is not how we do it now because of size issues
    outcsv = 'test.csv'
    f = open(outcsv, 'w')
    f.write('{0},{1}\n'.format('blockkey', ','.join(variables)))
    #batch = leveldb.WriteBatch()
    # Create a list of tables - sorted correctly just to make sure
    tables = [v for k, v in sorted(interpolations.iteritems(), key=lambda x: BASESORTKEY(x[0]))]
    # generate a row of zeroes so this does not need to be made each time we hit a zero population block
    zerorow = [0] * len(variables)
    # start our timer
    t1 = time.time()
    prevcounty = None
    # loop through the iterator, which is enumerated so we have a count to keep track of speed
    for counter, (blockkey, blkgrpdata, tractdata) in enumerate(iterator(intractdb, inblkgrpdb, inblockdb, blockschema, blkgrpschema, tractschema)):
        # the county is the first five chars of a blockkey
        county = blockkey[:5]
        # when we need to get new counties it means that the iterator is not running correctly
        # i think that we will never get here without an error in the database - missing blocks, changing codes from different censuses, etc
        if prevcounty != county:
            f.close()
            f = open(os.path.join(inoutdb,county), 'w')
            f.write('{0},{1}\n'.format('blockkey', ','.join(variables)))
            print prevcounty, counter, time.time() - t1
            prevcounty = county
            print len(zerorow)
        # if there is no population we just return the premade zerorow
        if not blkgrpdata['ds172_dt12_av1']:
            rowdict = zerorow
        # if there is a population then we have to do some real work
        else:
            rowdict = []
            # loop through each table that we listed out earlier
            for table in tables:
                #if the geog is tract then we are using tract data
                if table.geog == 'Tract':
                    tabledata = tractdata
                # if the table geog is blkgrp then we use that data
                else: 
                    tabledata = blkgrpdata
                #tabledata = {var: tabledata[var] for var in table.variables}
                # now we check to see if and of the data used for the table in the equations are nonzero
                # if there are some then we evaluate the equations for that table
                if any(variablestore.data_filter(tabledata, table.variables)):
                    result = table.evaluate(tabledata)
                    rowdict += [result[variable]['result'] for variable in table.sorted_subrows]
                # if there are not then we just return the same amount of zeroes as the table has variables
                else:
                    rowdict += [0] * len(table.sorted_subrows)

        # this section below is to fill in equations on the fly and print out portions of them to evaluate where problems are taking place

        #for interpolation in interpolations:
        #    rowdict[interpolation.name] = interpolation.interpolate(mydict)
        #    if interpolation.name in ['ds185_dt31_av' + str(x) for x in xrange(10)]:
        #        print interpolation.name, interpolation.equation, interpolation.interpolate(mydict), 
        #        xdict =  {k:mydict['blockdata'][k] for k in interpolation.variables if 'ds172' in k}
        #        xdict.update({k:mydict['tractdata'][k] for k in interpolation.variables if 'ds172' not in k})
        #        print interpolation.equation.subs(xdict)
         
        # this section below has a different way of handling zero division errors from before and is not longer used

        #rowdict = fixrow(rowdict, blockkey, methoddict, verify_rules, allequations, mydict)
        #rowdict = [rowdict[k]['result'] for k in variables]
        #rowdict = {k: v['result'] for k, v in rowdict.iteritems()}
        #batch.Put(blockkey, json.dumps(rowdict))

        # write the data to the csv
        f.write('{0},{1}\n'.format(blockkey, ','.join(map(str,rowdict))))
        # every 500 blocks we write everything out to the output and print time data to the user so we know it is still working
        if (counter + 1) % 500 == 0:
            #outdb.Write(batch, sync=True)
            #batch = leveldb.WriteBatch()
            t2 = time.time()
            print counter, t2 - t1
            f.close()
            f=open(os.path.join(inoutdb,county), 'a')
    # if we reach the end then we synch / close files 
    else:
        f.close()
        #outdb.Write(batch, sync=True)

def run(intractdb, inblkgrpdb, inblockdb, inoutdb, 
        tractschema, blkgrpschema, blockschema, 
        tractvars, blkgrpvars, blockvars, interpolations):
    """old equation runner before I reworked the database iterator and equation handling scripts
    this is unused"""
    # interpolations = equationlist(interpolations)
    tractdb = leveldb.LevelDB(intractdb)
    blkgrpdb = leveldb.LevelDB(inblkgrpdb)
    blockdb = leveldb.LevelDB(inblockdb)
    outdb = leveldb.LevelDB(inoutdb)
    prevtractkey = None
    counter = 0
    batch = leveldb.WriteBatch()
    t1 = time.time()
    for blkgrpkey, group in groupby(blockdb.RangeIter(), key=sortkey):
        tractkey = blkgrpkey[:-1]
        if prevtractkey != tractkey:
            prevtractkey = tractkey
            tractdata = loaddata(tractdb.Get(tractkey), tractschema, tractvars)
            alldata = tractdata
        blkgrpdata = loaddata(blkgrpdb.Get(blkgrpkey), blkgrpschema, blkgrpvars)
        alldata.update(blkgrpdata)
        for blockkey, blockvalue in group:
            blockdata = loaddata(blockvalue, blockschema, blockvars)
            alldata.update(blockdata)
            # outdata is a list to speed things up a bit
            outdata = []
            for interpolation in interpolations:
                outdata.append(interpolation.evaluate(alldata))
            batch.Put(blockkey, json.dumps(outdata))
            counter += 1
            if counter % 5000 == 0:
                outdb.Write(batch, sync=True)
                batch = leveldb.WriteBatch()
                print counter, time.time() - t1
    else:
        outdb.Write(batch, sync=True)

def loadschema(indb):
    """basic function to open the schema for a database"""
    inschema = indb + '.schema'
    f = open(inschema)
    output = json.load(f)
    f.close()
    return output

def dumpschema(inlist, infilepath):
    """function to write out the schema for a database"""
    f = open(infilepath, 'w')
    json.dump(inlist)
    f.close()
    return True
            
def loadschemas(intractdb, inblkgrpdb, inblockdb):
    """loads the schema for all three geogs that we worry about"""
    outlist = [loadschema(x) for x in ('tracts_redo.leveldb', 'census_data/blkgrp.leveldb', 'census_data/blocks.leveldb')]
    return outlist

def findpertinentvars(invarset, inlists):
    """this is unused and i am pretty sure never worked well"""
    outlist = []
    for inlist in inlists:
        outlist.append(invarset.intersection(inlist))
    allschemas = set(inlists[0])
    for l in inlists[1:]:
        allschemas.update(l)
    x = invarset.difference(allschemas)
    x = set(['_'.join(y.split('_')[:]) for y in x])
    return outlist

def filterdict(invarset, indict):
    # creates a dictionary using a list to grab keys. same as dict.fromkeys(invarset, lambda x: indict.get(x))
    return {k: indict[k] for k in invarset}

def readjsonequations(injson):
    """function to read in a json equation into my old style of equation object. this is currently not being used, but does work"""
    outlist = []
    with open(injson) as f:
        mydict = json.load(f)
    for name, dicty in mydict.items():
        outlist.append(equation(name, dicty['equation'], dicty['geog']))
    return outlist

def main():
    intractdb = sys.argv[1]
    inblkgrpdb = sys.argv[2]
    inblockdb = sys.argv[3]
    outdb = sys.argv[4]
    incsv = sys.argv[5]
    # bootleg way to use my new json equations
    with open(incsv) as f:
        rawequations = json.load(f)
    equations = {}
    variables = set()
    for table, tabledata in rawequations.iteritems():
        equations[table] = tablestore.loadfromdict(table, tabledata)
        for total, totaldata in tabledata.iteritems():
            variables.add(total)
            for variable in totaldata['subvariables']:
                variables.add(variable)
    #equations = readjsonequations(incsv)
    #equations = readequations(incsv)
    #equationvars = set([y for x in equations for y in x.variables])
    variables = sorted(variables, key=BASESORTKEY)
    with open(outdb + '.schema', 'w') as f:
        json.dump(variables, f, indent=4)
    tractschema, blkgrpschema, blockschema = loadschemas(intractdb, inblkgrpdb, inblockdb)
    #tractvars, blkgrpvars, blockvars = findpertinentvars(
                 #equationvars,[tractschema, blkgrpschema, blockschema])
    #run(intractdb, inblkgrpdb, inblockdb, outdb, tractschema,       
    #    blkgrpschema, blockschema, tractvars, blkgrpvars, 
    #    blockvars, equations)
    run_iterator(outdb, intractdb, inblkgrpdb, inblockdb, blockschema, blkgrpschema, tractschema, equations, verify_rules, methoddict, variables)
    #createtestoutput(intractdb, inblkgrpdb, inblockdb, blockschema, blkgrpschema, tractschema, equations)

if __name__ == '__main__':
    main()
