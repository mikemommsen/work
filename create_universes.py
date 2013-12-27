# this is a lot of random shit to import huh?
from __future__ import division
import leveldb
from sympy import var, lambdify, sympify, Function
from sympy.utilities.lambdify import implemented_function
import csv
import sys
import re
import time
import json
from read_fixed_width_leveldb import readdofile
from collections import OrderedDict
from operator import attrgetter
from itertools import groupby, chain

# below are some regex dealios for grabbing parts of equations
FINDVARIABLES = 'ds\w+'
FINDDIVISION = r'[/]\s*\(*\s*[0-9a-zA-Z_\s+-]+'

def zerodiv(num, den):
    """if 0 then return 1, else return den"""
    if den == 0:
        return 1
    else:
        return float(num) / float(den)

def findtextinparens(intext, starter, ender):
    counter = 0
    outtext = ''
    #if intext.index(starter) > intext.index(ender):
     #   match = re.match('\s*\w*', intext)
      #  if not match:
       #     print intext, 'BIGERROR'
        #return match.end()
    for char in intext:
        outtext += char
        if char == starter:
            counter += 1
        if char == ender:
            counter -= 1
        if counter <= 0 and char == ender:
            if starter not in outtext:
                outtext = outtext[:-1]
            break
    return len(outtext)

def divreplace(instring):
    if '/' not in instring:
        return instring
    else:
        parts = instring.split('/')
        for count, part in enumerate(parts[:-1]):
            num = findtextinparens(''.join([x for x in reversed(part)]), ')', '(')
            parts[count] = '{0} zerodiv({1}'.format(part[:-num], part[-num:])
        for count, part in enumerate(parts[1:]):
            x = count + 1
            den = findtextinparens(part, '(',')')
            parts[x] = '{0}) {1}'.format(part[:den + 1], part[den + 1:])
        return ','.join(parts)

def demreplace(matchobj):
    """used in the re.subn to replace denominators with zerodiv function"""
    alldata = matchobj.group(0)
    count = alldata.count('(')
    den = matchobj.group(1)
    leftparens = '(' * count
    if den == '1': 
        text = '/ (1'
    else:
        text =  '/ {0}zerodiv({1})'.format(leftparens, den)
    return text

zerodiv = implemented_function(Function('zerodiv'), zerodiv)

class equationlist:
    def __init__(self, inlist):
        self.inlist = sorted(inlist, key=attrgetter('variables'))

    def groupiter(self):
        for k, v in groupby(self.inlist, key=attrgetter('variables')):
            yield k, v

    def addtractdata(self, indict):
        for e in self.inlist:
            if e.dataissue == 0:
                e.tractdata = []
                for v in e.tractvariables:
                    e.tractdata.append(indict.get(v))

    def addblkgrpdata(self, indict):
        for e in self.inlist:
            if e.dataissue == 0:
                e.blkgrpdata = []
                for v in e.blkgrpvariables:
                    e.blkgrpdata.append(indict.get(v))

    def addblockdata(self, indict):
        for e in self.inlist:
            if e.dataissue == 0:
                e.blockdata = []
                for v in e.blockvariables:
                    e.blockdata.append(indict.get(v))

    def evaluate(self):
        outlist = []
        for e in self.inlist:
            if e.dataissue == 0:
                alldata = e.tractdata + e.blkgrpdata + e.blockdata
                outlist.append(e.run_lambda(*alldata))
            else:
                outlist.append(None)
        return outlist

    def evaluatelist(self, indict):
        outlist = []
        for varlist, elist in self.groupiter():
            valueslist = []
            length =  len(list(elist))
            for v in varlist:
                try:
                    val = indict[v]
                    valueslist.append(val)
                except KeyError:
                    outlist += [None] * length
                    break
            else:
                for e in elist:
                    outlist.append(e.run_lambda(*valueslist))
        return outlist

class equation:
    def __init__(self, name, instr, geoglevel=None):
        self.name = name
        self.instring = instr
        self.geoglevel = geoglevel
        instr = instr.replace('.','_')
        instr = divreplace(instr)
        variables = re.findall(FINDVARIABLES, instr)
        self.variables = []
        for variable in variables:
            if variable not in self.variables:
                self.variables.append(variable)
        #var(self.variables)
        self.variables = sorted(self.variables)
        self.equation = sympify(instr)
        self.run_lambda = lambdify(self.variables, self.equation)

    def __str__(self):
        return '{0}: {1}'.format(self.name, str(self.equation))

    def evaluate(self, indict):
        variables = []
        x = None
        for var in self.variables:
            try:
                variables.append(indict[var])
            except KeyError as e:
                break
        else:
            x = self.run_lambda(*variables)
        return x

    def interpolate(self, indict):
        variables = {}
        blockdata = indict['blockdata']
        if self.geoglevel == 'Tract':
            otherdata = indict['tractdata']
        else:
            otherdata = indict['blkgrpdata']
        for var in self.variables:
            if var[:5] == 'ds172':
                variables[var] = blockdata[var]
            else:
                variables[var] = otherdata[var]
        x = self.run_lambda(**variables)
        return x
	
def run(incsv, indatabase, field, table):
    f = open(incsv)
    reader = csv.DictReader(f)
    mylist = []
    id_field = '_av'
    for r in reader:
        if r[field]:# and r['Smallest ACS Geo Level'] == 'Block Group':
            # make sure to change each time for each table
            oid = table.format(r[id_field])
            mylist.append(equation(oid, r[field]))
    f.close()
    return mylist

def updatedatabase(indatabase, equations, schema):
    t1 = time.time()
    db = leveldb.LevelDB(indatabase)
    batch = leveldb.WriteBatch()
    count = 0
    for row in db.RangeIter():
        count += 1
        val = json.loads(row[1])
        val = createdict(val, schema)
        key = row[0]
        for equation in equations:
            name = equation.name
            val[name] = equation.evaluate(val)
        val = json.dumps([val[v] for v in chain(schema, [x.name for x in equations])])
        batch.Put(key, val)
        if count % 1000 == 0:
            db.Write(batch)
            batch = leveldb.WriteBatch()
            t2 = time.time()
            print '{0}: {1}'.format(count, t2 - t1)
            t1 = t2
    else:
        db.Write(batch)
    return True

def createdict(inlist, inschema):
    return OrderedDict(zip(inschema, inlist))

def main():
    inpop = sys.argv[1]
    inhouse = sys.argv[2]
    infamily = sys.argv[3]
    indatabase = sys.argv[4]
    inschema = sys.argv[5]
    with open(inschema) as f:
        schema = json.load(f)
    equations = run(inpop, indatabase, 'ACS dsX_dt1 (istads)', 'dsX_dt1_av{0}')
    equations += run(inhouse, indatabase, 'ACS dsX_dt2 (istads)', 'dsX_dt2_av{0}')
    equations += run(infamily, indatabase, 'ACS dsX_dt3 (istads)', 'dsX_dt3_av{0}')
    with open(indatabase + '.schema', 'w') as f:
        json.dump(schema + [x.name for x in equations], f)
    with open('census_data/universe_equations.json','w') as f:
        json.dump({x.name: str(x.equation) for x in equations}, f, indent=4)
    updatedatabase(indatabase, equations, schema)

if __name__ == '__main__':
    main()

