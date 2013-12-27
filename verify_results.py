# mike mommsen
# script to verify that cross table totals are accurate
# if two tables both have total population they should equal each other, so thats what we test here
# along with other easy tests across tables

# enjoy
# imports
import sys
import json
import leveldb
from collections import OrderedDict
from operator import attrgetter
import re
import csv
from collections import defaultdict
from itertools import izip, chain
from collections import Counter
from create_universes import zerodiv

def loadschema(indb):
    """"""
    inschema = indb + '.schema'
    f = open(inschema)
    output = json.load(f)
    f.close()
    return output

def loaddata(data, schema):
    """return data fron json list into python dict using schema keys"""
    return {k: v for k, v in izip(schema, json.loads(data))}

def testcrosstablerule(indata, ruledict):
    """function to test a sungle rule for a single row. called by testcrosstablerow"""
    errors = []
    if type(ruledict['total']) == list:
        totals = [indata[x] for x in ruledict['total']]
        x = '{0:.6f}'.format(totals[0])
        minicmplist = [x for y in totals if '{0:.6f}'.format(y) != x]
        if not any(minicmplist):
            total = totals[0]
        else:
            errors.append('totals do not match')
            print json.dumps(dict(zip(ruledict['total'], totals)), indent=4)
            x = {'white alone': indata['ds185_dt307_av1'], 
		'white not hispanic': indata['ds185_dt314_av1'],
		'hispanic': indata['ds185_dt315_av1']}
            print 'races', json.dumps(x, indent=4)
            total = None
    else:
        total = indata.get(ruledict['total'])
    # if i have a race break which is all rules but one
    if len(ruledict) > 1 and total != None:
        # blank dictionary
        mydict = {}
        # pull data to create a new dictionary for each race break
        mydict['hispanic'] = indata[ruledict['hispanic or latino']]
        mydict['twoOrMore'] = indata[ruledict['two or more races']]
        mydict['black'] = indata[ruledict['black']]
        mydict['asian'] = indata[ruledict['asian']]
        mydict['whiteNonLatino'] = indata[ruledict['white non latino']]
        mydict['aian'] = indata[ruledict['aian']]
        mydict['other'] = indata[ruledict['other race alone']]
        mydict['white'] = indata[ruledict['white alone']]
        mydict['pacific'] = indata[ruledict['pacific islander']]
        # sun the vars that are used for race
        mydict['racesum'] = sum([mydict[x] for x in ['twoOrMore', 'black', 'asian', 'white', 'aian', 'other', 'pacific']])
        # test for each part of the dictionary to make sure that the number is smaller than the total
        for key, value in mydict.iteritems():
            if int(value) > int(total):
                errors.append('{0} is greater than total'.format(key))
        # test to make sure that thje sum is equal to the total
        if '{0:.6f}'.format(mydict['racesum']) != '{0:.6f}'.format(total):
            errors.append('races do not add up')
        # white non latino is a subset of white so must be smaller
        if mydict['whiteNonLatino'] > mydict['white']:
             errors.append('more white latinos than whites')
    return errors, total

def testcrosstablerow(indata, crosstablerules):
    """function to test a single row of data (a block) and spit each rule to testcrosstablerule"""
    # this is the part to test each rule on its own
    errorcounter = defaultdict(Counter)
    totaldict = {}
    for key, ruledict in crosstablerules.iteritems():
        errors, total = testcrosstablerule(indata, ruledict)
        totaldict[key] = total
        for error in errors:
            errorcounter[key][error] += 1
    totaldict['occupied housing'] = totaldict['Owner occupied housing units'] + totaldict['Renter occupied housing units']
    if None not in totaldict.values():
        if totaldict['Population 25 years and over'] > totaldict['population 16 years and over']:
            errorcounter['Population 25 years and over']['greater than population 16 years and over'] += 1
        if totaldict['population 16 years and over'] > totaldict['Population 2 years and over']:
            errorcounter['population 16 years and over']['greater than population 2 years and over'] += 1
        if totaldict['Population 2 years and over'] > totaldict['Population 1 year and over in the United States']:
            errorcounter['Population 2 years and over']['greater than population 1 year and over'] += 1
        if totaldict['Civilian employed population 16 years and over'] > totaldict['population 16 years and over']:
            errorcounter['Civilian employed population 16 years and over']['greater than pop 16 years and over'] += 1
        if totaldict['Population 1 year and over in the United States'] > totaldict['Total population']:
            errorcounter['Population 1 year and over in the United States']['greater than Total population'] += 1
        if totaldict['Families'] > totaldict['Households']:
            errorcounter['Families']['greater than Households'] += 1
        if totaldict['Households'] > totaldict['Total population']:
            errorcounter['Households']['greater than Total population'] += 1
        if totaldict['Renter occupied housing units'] > totaldict['Households']:
            errorcounter['Renter occupied housing units']['greater than Households'] += 1
        if totaldict['Owner occupied housing units'] > totaldict['Households']:
            errorcounter['Owner occupied housing units']['greater than Households'] += 1
        if totaldict['occupied housing'] != totaldict['Households']:
            errorcounter['Households']['not same amount of households as occupied housing units'] += 1
    return errorcounter

def testcrosstable(indatabase, schema, crosstablerules):
    """function to iterate over database and feed rows to testcrosstablerow"""
    db = leveldb.LevelDB(indatabase)
    count = 0
    outdict = None
    for key, value in db.RangeIter():
        count += 1
        data = loaddata(value, schema)
        rowresults = testcrosstablerow(data, crosstablerules)
        if not outdict:
            outdict = rowresults
        else:
            for x in rowresults:
                outdict[x].update(rowresults[x])
        if count == 1000:
            print json.dumps(outdict, indent=4)
            break

def testrule(indata, total, components, table):
    """function to test a single rule"""
    # first we try to grab the total
    try:
        valtotal = indata[total]
    # if we cant then print out why and exit 
    except KeyError:
        print indata.keys(), total
        sys.exit(1)
    # grab the components from the rowdictionary
    valcomps = {c: indata[c] for c in components}
    # find out how many components there are
    lengthcomps = len(components)
    # turn the valtotal into a string so we dont need to worry about rounding errors
    strvaltotal = '{0:.10f}'.format(valtotal)
    # take the sum of the components
    valsumcomps = sum(valcomps.values())
    # and turn that into a string for easy comparison
    strvalsumcomps = '{0:.10f}'.format(valsumcomps)
    # valtotalmult is a test to see if each comp == valtotal, which seems like a specialized error to me
    valtotalmult = valtotal * lengthcomps
    # and then we turn that into a string
    strvaltotalmult = '{0:.10f}'.format(valtotalmult)
    # give ourself a 5% margin of error. 
    # results from this are no different then the 10 decimal point rounding
    if (valtotal * 1.06) >= valsumcomps >= (valtotal * .95):
        # valtotal == 0 is a special case because it doesnt prove that we did the math right, just that we had zeroes
        if strvaltotal == '{0:.10f}'.format(0):
            result = 'zero success'
        # but if it is not zero we can count it as a real success
        else:
            result = 'success'
    # but if we have an error
    else:
        #if strvaltotalmult == strvalsumcomps:
            #result = 'mult'
        #else:
        result = 'error'
    #print total, valtotal, valsumcomps, result
    # we used to print out here, but we print out at the higher function now so we can aggregate results into a nice readable format
    if False:#result == 'error':
        zerocomps = [k for k, v in valcomps.iteritems() if v==0]
        print table, total, result, strvaltotal, valcomps, strvalsumcomps
    return result, valtotal, valcomps

def testrow(data, methoddict, verify_rules):
    """"""
    outdict = defaultdict(Counter)
    for table, method in methoddict.items():

        testdict = verify_rules[str(method)]
        tabledata = {k.split('_')[-1][2:]: v for k, v in data.iteritems() if table in k}
        for total, components in testdict.items():
            result, valtotal, valcomps = testrule(tabledata, total, components, table)
            outdict[table, total][result] = 1
    return outdict

def fixrow(data, key, methoddict, verify_rules, equationdict, originaldata):
    """function that i wrote to fix rows that were causing problems after results were created.
    after i did this it was clearly a problem because i had no way of knowing what caused the error.
    because of this i reworked ther interpolation script to allow for fixes in there rather than after the fact"""
    originaldata = {k: v for k, v in chain(originaldata['blockdata'].items(), originaldata['tractdata'].items())}
    def subber(matchobject):
        return '{:d}'.format(originaldata[matchobject.group(0)])

    for table, method in methoddict.items():
        testdict = verify_rules[str(method)]
        tabledata = {k.split('_')[-1][2:]: v for k, v in data.items() if table in k}
        for total, components in sorted(testdict.items(), key = lambda x: int(x[0])):
            result, valtotal, valcomps = testrule(tabledata, total, components)
            if result == 'mult':
                totalname = '{0}_av{1}'.format(table, total)
                if table == 'ds185_dt31':
                    print key, totalname, 'total', re.subn(FINDVARIABLES, subber, equationdict[totalname]['equation'])[0], valtotal
                valcomps = {k: v for k, v in valcomps.iteritems() if v == valtotal}
                lengthcomps = len(valcomps)
                for val in valcomps:
                    tabledata[val] = float(valtotal) / lengthcomps
                    fullname = '{0}_av{1}'.format(table, val)
                    if table == 'ds185_dt31':
                        print key, fullname, 'value', re.subn(FINDVARIABLES, subber, equationdict[fullname]['equation'])[0], tabledata[val]
                    data[fullname] = float(valtotal) / lengthcomps
    return data

def testdata(indatabase, schema, methoddict, verify_rules, equationdict):
    """function for testing within table errors.
    these are errors like male + female != total for example.
    it only verifies that these totals work, not that they are accurate results, just that the hierarchy for the table is being followed"""
    # set outdict to None
    outdict = None
    # blank list
    mylist = []
    # open up the input database ( which is the output from interpolation ( why verify anything else))
    db = leveldb.LevelDB(indatabase)
    # start our counter at zero
    count = 0
    # loop through the database
    for key, value in db.RangeIter():
        # add to the counter
        count += 1
        # load the data using the loaddata function
        data = loaddata(value, schema)
        # if its not the first time through then we add the new data to the old data
        # i wonder if collections.Counter would make this a lot easier
        if outdict:
            newdata = testrow(data, methoddict, verify_rules)
            outdict = {i: outdict[i] + newdata[i] for i in outdict}
        # if it is the first time through then we just make the dictionary
        else:
            outdict = testrow(data, methoddict, verify_rules)
        # when we get to a count we like (10k seemed appropro) we print out results
        if count == 10000:
            #print sorted(set([k[0] for k in outdict]))
            # create a dictionary that only has errors in it
            outdict = OrderedDict([('{0}_av{1}'.format(k[0], k[1]), v) for k,v in sorted(outdict.items()) if 'error' in v or 'mult' in v])
            # loop through the dictionary and format it 
            for k, v in outdict.items():
                comp = verify_rules[str(methoddict['_'.join(k.split('_')[:2])])][k.split('_')[-1][2:]]
                outdict[k]['components'] = ['_'.join(k.split('_')[:2]) + '_av' + c for c in comp]
            # print the output in json because that is nicer to look at
            print json.dumps(outdict, indent=4)
            break
    return mylist

class cell:
    def __init__(self, inkey, invalue):
        self.key = inkey
        self.value = invalue
        self.ds, self.dt, self.av = [x[2:] for x in inkey.split('_')]
        self.table = 'ds{0}_dt{1}'.format(self.ds, self.dt)

class row:
    def __init__(self, indict):
        basicdict = {}
        sturctureddict = defaultdict(dict)
        for k, v in indict.iteritems():
            basicdict[k] = v
            x = cell(k, v)
        self.dictionary = basicdict

def readcsv(infile):
    """this is the thing to read in csv and write out a methoddict and a row dict"""
    f = open(infile)
    reader = csv.DictReader(f)
    labelfield = 'label'
    varfield = 'istads_id'
    mylist = [(r[varfield], tuple(r[labelfield].split(':'))) for r in reader]
    newlist = {}
    xlist = []
    prevcode = mylist[1][0]
    print prevcode
    for code, vals in mylist:
        if code.count('.') == 2:
            ds, dt, av = [x[2:] for x in code.split('.')]
            xlist.append((av, vals))
        else:
            ds, dt, av = [x[2:] for x in prevcode.split('.')]
            newlist[ds, dt] = tuple(xlist)
            xlist = []
        prevcode = code
    newdict = defaultdict(list)
    for k, v in newlist.items():
        newdict[v].append(k)
    methoddict, rowdict = OrderedDict(), {}
    for count, (k, v) in enumerate(sorted(newdict.items(),key=lambda x: x[1])):
        methoddict[count] = OrderedDict([(x,': '.join(y)) for x,y in k])
        for ds, dt in v:
            rowdict['ds{0}_dt{1}'.format(ds, dt)] = count
    print json.dumps(methoddict, indent=4)
    print json.dumps(rowdict, indent=4)

def printproblemequations(verifyrules, equationdict, problemvars):
    for table, method, total, valtotal, valcomps in problemvars:
        ds, dt = [x[2:] for x in table.split('_')]
        comps = verifyrules[str(method)][str(total)]
        print method,total,':',equationdict['{0}_av{1}'.format(table, total)], '=',
        print '+'.join([equationdict['{0}_av{1}'.format(table, comp)] for comp in comps])
        print valtotal, valcomps

FINDVARIABLES = 'ds\w+'
import sympy
from sympy import var

def solveequations(verifyrules, equationdict, methoddict):
    """this was a function to try and solve equations symolically to see where problems were occuring
    as you can imagine this did not work
    this function does not belong in this script, but hey we arent perfect"""
    eqs = ' '.join(equationdict.values())
    variables = re.findall(FINDVARIABLES, eqs)
    print verifyrules
    var(variables)
    for table, method in sorted(methoddict.items()):
        baseequations = []
        ds, dt = [x[2:] for x in table.split('_')]
        for total, comps in verifyrules[str(method)].items():
            tabletotal = '{0}_av{1}'.format(table, total)
            totalvalue = equationdict[tabletotal] 
            complist = []
            for comp in comps:
                tablecomp ='{0}_av{1}'.format(table, comp)
                compval = equationdict[tablecomp]
                complist.append(compval)
            compsjoined = ' + '.join(complist)
            print total, comps
            totaleq = sympy.simplify(totaleq)
            compsjoined = sympy.simplify(compsjoined)
            equalstatement = sympy.Eq(totaleq, compsjoined)
            baseequations.append(equalstatement)
        print sympy.solve(baseequations, dict=True)
    print 'that was stage one'
    for table, method in sorted(methoddict.items()):
        ds, dt = [x[2:] for x in table.split('_')]
        for total, comps in verifyrules[str(method)].items():
            totaleq = equationdict['{0}_av{1}'.format(table, total)]
            complist = []
            for comp in comps:
                compval = equationdict['{0}_av{1}'.format(table, comp)]
                complist.append(compval)
            compsjoined = ' + '.join(complist)
            print total, comps
            totaleq = sympy.simplify(totaleq)
            compsjoined = sympy.simplify(compsjoined)
            equalstatement = sympy.Eq(totaleq, compsjoined)
            print sympy.solve(equalstatement, dict=True)

def main():
    indatabase = sys.argv[1]
    schema = loadschema('contycsv2')
    inmethoddict = sys.argv[2]
    inverifyrules = sys.argv[3]
    inequations = sys.argv[4]
    with open(inmethoddict) as f:
        methoddict = json.load(f)
    with open(inverifyrules) as f:
        verifyrules = json.load(f)
    global equationdict
    with open(inequations) as f:
        equationdict = json.load(f)
    with open('python scripts/cross_table_verify_rules.json') as f:
        crosstablerules = json.load(f)
    problemvars = testdata(indatabase, schema, methoddict, verifyrules, equationdict)
    #testcrosstable(indatabase, schema, crosstablerules)
    #printproblemequations(verifyrules, equationdict, problemvars)
    #solveequations(verifyrules, equationdict, methoddict)

if __name__ == '__main__':
    main()
