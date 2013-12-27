# mike mommsen
# this script processes input csvs of equations into objects. 
# these objects process each block which are fed to it from interpolation_leveldb.py
import json
import re
import csv
from collections import deque
from collections import OrderedDict
from create_universes import equation
import sympy
from read_interpolate import PROCESSDICT

from create_universes import zerodiv
from operator import attrgetter
from copy import deepcopy
from collections import Counter
from itertools import chain
import sys

# function to allow for logical sorting
def BASESORTKEY(initem):
    return map(int, re.findall('[0-9]+', initem))

FINDVARIABLES = 'ds\w+'

class datastore:
    """"""
    def __init__(self):
        pass
        # this could be a great place to store the data flattened, not flattened, and with different sort options to help speed up processing and simplify some of the other steps. also this class clearly is not that needed so it is up in the air if this will ever be made


class tablestore:
    """stores information about each table, and allows for processing and storage of a table
	a table in this case refers to a table like ds184.dt12"""
    def __init__(self, tablename=None, totals=None, geog=None):
        """"""
        self.geog = geog
        self.tablename = tablename
        self.totals = totals
        self.sorted_subrows = [subrow.name for total in totals for subrow in total.subrows]
        self.sorted_subrows += [total.totalrow.name for total in totals]
        self.sorted_subrows = sorted(set(self.sorted_subrows), key=BASESORTKEY)
        self.variables = [variable for total in totals for variable in total.allvariables]
        self.blockvariables = [variable for variable in self.variables if 'ds172' in variable or 'dsY' in variable]
        self.acsvariables = [variable for variable in self.variables if 'ds184' in variable or 'ds185' in  variable or 'dsX' in variable]
        self.sorted_totals = sorted(self.totals, key=lambda x: BASESORTKEY(x.totalrow.name))
        if self.geog == 'Tract':
            self.geog_key ='tractdata'
        else:
            self.geog_key = 'blkgrpdata'

    def evaluate(self, indata):
        """function create output data from indata using the equations of the table"""
        outdata = {}
        totaldata = {}
        # the commented out code below was an old way of creating the tablewide dictionary which was slower
        #xdata = indata#[self.geog_key]	
        #flatdata = indata[self.geog_key]
        #blockdata = indata['blockdata']
        #for variable in self.acsvariables:
        #    xdata[variable] = flatdata[variable] 
        #for variable in self.blockvariables:
        #    xdata[variable] = blockdata[variable]
        #for variable in self.variables:
        #    xdata[variable] = indata[variable]

        # loop through each "total" for the table
        for total in self.sorted_totals:
            # if there are any nonzero data elements for that group then evaluate data
            if any(variablestore.data_filter(indata, total.allvariables)):
                rowdata = total.evaluate(indata, totaldata.get(total.totalrow.name))
            # otherwise there is no data to evaluate, so we just return all zeroes
            else:
                rowdata = {var.name: {'result': 0} for var in total.subrows}
                rowdata.update({total.totalrow.name: {'result': 0}})
            totaldata.update(rowdata)
        return totaldata # outdata

    def dump(self):
        """"""
        outdict = {}
        for total in self.totals:
            if total not in outdict:
                outdict[total.totalrow.name] = total.dump()
        return outdict

    def __str__(self):
        """"""
        return self.totalname

    @classmethod
    def loadfromdict(cls, tablename, tabledata):
        """function to create the table, total, variable hierarchy from a dictionary
        the dictionary is created and stored as json at the bottom of this script from a csv of equations"""
        totals = []
        for totalname, totaldata in tabledata.iteritems():
            repeats = len(totaldata['subvariables'])
            geog = totaldata['geog']
            eq = totaldata['equation']
            totalrow = variablestore(totalname, eq['mult'], eq['num'], eq['den'], 1, geog)
            subrows = []
            for subname, subdata in totaldata['subvariables'].iteritems():
                if type(subdata['equation']['mult']) in (str, unicode):
                    rowrepeats = len([x for x in totaldata['subvariables'].itervalues() if x['equation']['mult'] == subdata['equation']['mult']])
                elif type(subdata['equation']['mult']) in (dict, OrderedDict):
                    # this is type 2 with no comp mult
                    if subdata['equation']['den'] == subdata['equation']['num']:
                        print subname
                        rowrepeats = repeats # not sure what to do with these
                    else:
                        rowrepeats = len([x for x in totaldata['subvariables'].itervalues() if x['equation']['mult'] == subdata['equation']['mult']])
                else:
                    print 'error here, unknown type'
                # this is not needed in my opinion, but not worth taking out
                if rowrepeats == 0: 
                    rowrepeats = 1
                subeq = subdata['equation']
                subrows.append(variablestore(subname, subeq['mult'], subeq['num'], subeq['den'], rowrepeats, geog))
            totals.append(totalstore(totalrow, subrows, repeats))
        return cls(tablename, totals, geog)
        
class totalstore:
    """stores information about a "total"
    a total is a variable that is supposed to equal the sum of other variables
    so population would be a total for male and female for example"""
    def __init__(self, totalrow=None, subrows=None, repeats=None):
        """"""
        # clearly dont need all of this garbage down here
        self.totalrow = totalrow
        self.repeats = repeats
        #self.totalname = totalrow.name
        self.subrows = subrows
        #self.geog = totalrow.geog
        #self.den = totalrow.den
        #self.num = totalrow.num
        #self.mult = totalrow.mult
        self.totalvariables = totalrow.variables
        self.subrowvariables = sorted(set([variable for subrow in subrows for variable in subrow.variables]))
        self.allvariables = set(list(self.totalvariables) + list(self.subrowvariables))

    def __str__(self):
        """"""
        return self.name

    def extractdata(self):
        """this is unused"""
        denlist,numlist, multlist = Counter(), Counter(), Counter()
        for subvariable in chain([total], self.subvariables):
            denlist.update(subvariable.denvariables)
            numlist.update(equation.numvariables)
            multlist.update(equation.multvariables)

    def evaluate(self, indata, totalvalue=None):
        """function to evaluate data"""
        # totalvalue is the response from the row dictionary when asked for this variable
        # totalvalue is None (null) when it has not be calcuated already (which should only happen on the first or root)
        if not totalvalue:
            #totaldata = {variable: indata[variable] for variable in self.totalvariables}
            outdata = self.totalrow.evaluate(indata)
            totaldata = outdata.values()[0]
        else:
            outdata = {}
            totaldata = totalvalue
        totalresult = totaldata['result']
        # if the "total" is 0 for this row, then no point in running its subvariables
        if totalresult == 0:
            outdata.update({x.name: {'result': 0} for x in self.subrows})
        # but if the result is not zero, then we do have to run it
        else:
            # loop through each of the variables that should equal total
            for subrow in self.subrows:
                # commented out line below makes a smaller dictionary - but this is slower than using big dict
                #xdict = {var: indata[var] for var in subrow.variables}
                # if any of the variables used in equations to calcalute the subrow then process
                if any(variablestore.data_filter(indata, subrow.variables)):
                    outdata.update(subrow.evaluate(indata, totaldata))
                # otherwise the answer must be zero
                else: 
                    outdata.update({subrow.name: {'result': 0}})

        # this section down here is an area where i do on the fly validation
        
        totalresult = '{:.10f}'.format(totaldata['result'])
        compresult = {k: v['result'] for k, v in outdata.iteritems() if k != self.totalrow.name}
        compresultvalue = '{:.10f}'.format(sum(compresult.values()))
        # note the "and False" at the end of the line which prevents execuation
        if totalresult != compresultvalue and False:
            print 'error here', totaldata['denZero'], self.totalrow.name, totalresult, compresultvalue, {k:v for k,v in outdata.items() if v.get('compdata') for comp in v['compdata']}# if k != self.totalrow.name}
        return outdata

    def dump(self):
        """function to dump data - made to be called from the tablestore"""
        outdict = self.totalrow.dump()
        outdict['subvariables'] = {}
        for subvariable in self.subrows:
            if subvariable not in outdict:
                outdict['subvariables'][subvariable.name] = subvariable.dump()
        return outdict

class variablestore:
    """stores an individual variable and the equations for it
	all equations are stored as multipliers, numerators and denominators"""
    def __init__(self, name=None, mult=None, num=None, den=None, repeats=None, geog=None):
        """"""
        # this is for the special case when num == den, or in other words the only thing that matter is the multiplier
        if num == den:
            self.multonly = True
            self.repeats = 1
        else:
            self.multonly = False
            self.repeats = repeats
        self.geog = geog
        self.name = name
        self.num = num
        self.den = den
        self.denvariables = sorted(set(re.findall(FINDVARIABLES, den)))
        self.numvariables = sorted(set(re.findall(FINDVARIABLES, num)))
        # if the multiplier is a dictionary then the equation is component based so we have to deal with a lot of stuff
        # each component is treated as its own variable store - which is probably not the best way to do it - it should be its own class
        if type(mult) in (dict, OrderedDict):
            # blank list which gets filled with variablestore instances for each component
            self.mult_list = []
            #  blank set which will contain every variable that will be needed for the multiplier 
            # so this can be called like non component based euqations
            self.multvariables = set()
            # loop through each component
            for compkey, compdata in mult['components'].iteritems():
                # this is for handling my two different formats, one that has equations referenced, one without
                if 'equation' in compdata:
                    compdata = compdata['equation']
                # add the variablestore instance for the component to mult_list
                self.mult_list.append(variablestore(compkey, compdata['mult'], compdata['num'], compdata['den'], repeats, geog))
                self.multvariables.update([y for x in compdata.values() for y in re.findall(FINDVARIABLES, x)])
            self.mult = mult['components']
            self.multvariables = sorted(self.multvariables)
        # if multiplier is not a list of components we can just treat it like denominators and multipliers
        else:
            self.mult = mult
            self.mult_list = None
            self.multvariables = sorted(set(re.findall(FINDVARIABLES, mult)))
            self.evaluate_mult = sympy.lambdify(self.multvariables, sympy.sympify(mult))
        # sympy.lambdify allows an equation to be turned into a function
        self.evaluate_num = sympy.lambdify(self.numvariables, sympy.sympify(num))
        self.evaluate_den = sympy.lambdify(self.denvariables, sympy.sympify(den))
        # create a list of all variables that are needed to run this
        self.variables = set(self.denvariables + self.numvariables + self.multvariables)

    def __str__(self):
        """"""
        return self.name

    def dump(self):
        """function to dump data for equations to json - made to be called from the top (tablestore)"""
        if self.mult_list:
            outdict = {'equation': {}, 'geog': self.geog}
            xdict = outdict['equation']
            xdict['mult'] = {'components': {m.name: m.dump() for m in self.mult_list}}
            xdict['num'] = self.num
            xdict['den'] =  self.den
        else:
            outdict = {'equation': {'mult': self.mult, 'num': self.num, 
		'den': self.den}, 'geog': self.geog}
        return outdict

    @staticmethod
    def data_filter(indata, inlist):
        """basic function to return the items in a list from a dict"""
	# this is the fastest way that i could find to return items from a dictionary sorted
	# i generally use this to test if there are any non zero values in a dictionary
	# which saves a lot of time
        for variable in inlist:
            try:
                yield indata[variable]
            except Exception as e:
                print e
                sys.exit(1)

    def evaluate(self, indata=None, totaldata=None):
        """function to evaluate variablestore instance with specific data"""
        # multonly means that den = num, so in cases where this is not true we need to evaluate num and den
        if not self.multonly:
            dendata = self.data_filter(indata, self.denvariables)
            den = self.evaluate_den(*dendata)
            numdata = self.data_filter(indata, self.numvariables)
            num = self.evaluate_num(*numdata)
        # if multonly is True then num and den do not matter and can be set to 1
        else: 
            num = 1
            den = 1
        repeats = None

        # if there is totaldata and the denominator is zero we have some work to do
        # we need to use data from totaldata to ensure that data nests properly
        if totaldata and not den:
            # if the parent was components but the variable does not
            # we need to figure out which component is similar to the kid
            if totaldata.get('compdata') and not self.mult_list:
                xdict = totaldata['compdata']
                total = 0
                # loop through each component of totaldata
                for i in xdict.values():
                    # find component that matches self 
                    if i.get('mult') == self.mult and i.get('denZero'):
                        # when we find the match then totaldata becomes the component from totaldata that matches
                        totaldata = i
                        repeats = i['repeats']
                        break
            # this is a clause to check and see if the parent does not have components but the kids do
            # this would be impossible to know how to handle
            elif not totaldata.get('compdata') and self.mult_list:
                if totaldata.get('denZero'):
                    print 'here', self.name
        # if there are any non zeroes in the indata for the multvariables
        if any(self.data_filter(indata, self.multvariables)):# and num:
            # if we are using the component method
            if self.mult_list:
                # blank dictionary to store data for each component 
                compdata = {}
                # checking to see if the parent also has component data
                if totaldata and totaldata.get('compdata'):
                    # loop through each component
                    for comp in self.mult_list:
                        compdata.update(comp.evaluate(indata, totaldata))
                else:
                    for comp in self.mult_list:
                        compdata.update(comp.evaluate(indata))
                mult = sum(comp['result'] for comp in compdata.itervalues())
            else:
                multdata = self.data_filter(indata, self.multvariables)
                compdata = None
                mult = self.evaluate_mult(*multdata)
            # if we have zeroDivision we are going to have to look at how many things we need to split by
            if den == 0:
                # if the parent also had zero division we are going to have to use that for the splitting as well
                if totaldata and totaldata.get('denZero'):
                    # repeats existing means that we have already calculated repeats up above so we can move on 
                    if repeats: 
                        pass
                    # repeats not existing means we calcluate it here - simple multiplication
                    else: 
                        repeats = totaldata['repeats'] * self.repeats
                # if the parent did not have zeroDivision we can use self.repeats
                else:
                    repeats = self.repeats
                # set denZero to True so when this is run for the children of this variable the kids know what happened
                denZero = True
                # because the denom is zero, numerator must be zero so our ration is one over number of repeats
                ratio = 1.0 / repeats
            else:
                # repeats has to get set so we can return it for the children
                repeats = self.repeats
                # we did not have zeroDivision so this is False
                denZero = False
                # and our ratio is very simply numerator divided by denominator (float is to ensure appropriate division)
                ratio = num / float(den)
            result = ratio * mult
            # looks like we should have a result object huh?
            result = {self.name: {'result': result, 'denZero': denZero, 'repeats': repeats, 'compdata': compdata, 'totalObject': self, 'mult': self.mult}}
        # if the multiplier is zero, we are going to return zero
        else:
            result = {self.name: {'result': 0}}
        return result

class basediv:
    """this function was my first attempt to do what is being done above
	i saved it for parsing in the equations from the spreadsheet, but outside of that it is unused"""
    def __init__(self, name=None, mult=None, num=None, den=None, repeats=None):
        if mult == 1:
            mult = '1'
        if den == 1: 
            den = '1'
        if num == 1:
            num = '1'
        self.den = den
        self.mult = mult
        self.num = num
        self.name = name
        self.denvariables = sorted(set(re.findall(FINDVARIABLES, den)))
        self.numvariables = sorted(set(re.findall(FINDVARIABLES, num)))
        self.denominator_eq = sympy.sympify(den)
        self.numerator_eq = sympy.sympify(num)
        self.denominator = sympy.lambdify(self.denvariables, self.denominator_eq)
        self.numerator = sympy.lambdify(self.numvariables, self.numerator_eq)
        self.repeats = None
        if type(mult) == list:
            self.multvariables = sorted(set([variable for x in self.mult for variable in x.variables]))
            self.multiplier_eq = [str(x) for x in self.mult]
            self.multiplier = mult
        else:
            self.multvariables = sorted(set(re.findall(FINDVARIABLES, mult)))
            self.multiplier_eq = sympy.sympify(mult)
            self.multiplier = sympy.lambdify(self.multvariables, self.multiplier_eq)
        self.variables = self.denvariables + self.numvariables + self.multvariables
 
    def __str__(self):
        return 'division object: ({multiplier_eq}) * ({numerator_eq} / {denominator_eq})'.format(**self.__dict__)

    def evaluate(self, indata):
        """this is unused - variablestore and the other "stores" do this
	this has never even been attempted"""
        dendata = {x: indata[x] for x in self.denvariables}
        numdata = {x: indata[x] for x in self.numvariables}
        denvalue = self.denominator(dendata)
        numvalue = self.numerator(numdata)
        if self.denominator == 0:
            ratio = (self.numerator / float(self.repeats))
        else:
            ratio = (self.numerator / self.denominator)
        # this is where we do that hot swap
        # but what we need to do is think about making the order so comps come at the end
        if type(self.multiplier) == list:
            multiplier = sum([x.evaluate(indata) for x in self.multiplier])
        else:
            multdata = {x: indata[x] for x in self.multvariables}
            multiplier = self.multiplier(multdata)
        return {self.name: self.multiplier * ratio}

def processrow(inrow, name):
    """function to process an individual row from the csv created by jim"""
    # change periods to underscores, remove newlines 
    inrow = {k.replace('.','_'): v.replace('.','_').replace('\n','').replace(' ','') for k, v in inrow.iteritems()}
    column = inrow['istads_id']
    # if we ever ask the row for 1 we get 1 back - not the right way to do it but thats what it is
    inrow[1] = 1
    method, comp_mult = inrow['Method'], inrow['Denominator for Component Multiplier'].strip(' ')
    if method == '2' and comp_mult and comp_mult != 'Nomultiplier':
        method = '3'
    dictionary = PROCESSDICT[method]
    num = inrow[dictionary['numerator']]
    den =  inrow[dictionary['denominator']]
    mult =  dictionary['multiplier']
    mult = {x: inrow[x] for x in mult}
    mult = {x: mult[x] for x in mult if mult[x] != ''}
    if len(mult) > 1:
        mult = [divsplit(x, mult[x]) for x in mult]
    elif mult:
        mult = divreplace(mult.values()[0])
    else:
        mult = 1
    return basediv(name, mult, num, den)

def readequations(infile):
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
            interpolations.append(processrow(row, name))
    f.close()
    #f = open('census_data/interpolation_equations.json','w')
    #outlist = {x.name: str(x.equation) for x in interpolations}
    #json.dump(outlist, f, indent=4)
    #f.close()
    return interpolations

def findtextinparens(intext, starter, ender):
    counter = 0
    outtext = ''
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

def divsplit(name, instring):
    if '/' not in instring:
        num, den = 1., 1.
        mult = instring
    else:
        parts = instring.split('/')
        if len(parts) == 2:
            for count, part in enumerate(parts[:-1]):
                numcount = findtextinparens(''.join([x for x in reversed(part)]), ')', '(')
                num = part[-numcount:]
                # below should be mult
                num = part[:-numcount -1].strip('*')
            for count, part in enumerate(parts[1:]):
                x = count + 1
                dencount = findtextinparens(part, '(',')')
                den = part[:dencount]
                mult = part[dencount + 1:].strip('*')
        else:
            mult = divreplace(instring)
            num, den = 1, 1 
    return basediv(name, mult, num, den)

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

###############################################################################
# this bottom part of the script is not good
# i wrote this out quickly to process the spreadsheets into the format that i liked
# and never changed it

# create a blank ordereddict
newdict = OrderedDict()

# loop through each row of speadsheet
for i in readequations('acs_data/SABINS_INTERPOLATION_11_22_js.csv'):
    x = newdict[i.name] = {}
    x['den'] = unicode(i.denominator_eq)
    x['num'] = unicode(i.numerator_eq)
    # if the equation is made up of components do the things needed for components
    if type(i.multiplier) == list:
        x['mult'] = {'components':OrderedDict()}
        for comp in (x for x in sorted(i.multiplier, key=attrgetter('name'))):
            x['mult']['components'][comp.name] = {
		'mult': unicode(comp.multiplier_eq),
		'den': unicode(comp.denominator_eq),
		'num': unicode(comp.numerator_eq)}
    # if it is just a normal equation then we dont need to do anything
    else:
        x['mult'] = unicode(i.multiplier_eq)
# these are the paths to the rules that i have stored - i should make sure that i include them
allequationspath = 'census_data/allequations.json'
allrulespath = 'census_data/allrules.json'

# load the above files using json
with open(allequationspath) as f:
    allequations = json.load(f)
with open(allrulespath) as f:
    allrules = json.load(f)
# create a list of all of the tables
tables = set(['_'.join(k.split('_')[:2]) for k in allrules])
# i should replace this with BASESORTKEY
sortkey = lambda x: [int(y) for y in re.findall('[0-9]+', x[0])]
# created a sorted list from the set of tables
tables = sorted(tables, key=sortkey)
# create a dictionary which has each table as a key and blank dictionaries which will be filled later as values
tableruledict = OrderedDict.fromkeys(tables, {})
# same as above but unordered 
betterdict= dict.fromkeys(tables, {})
# loop through the tables
for table in tables:
    tableruledict[table] = OrderedDict(sorted([(k, v) for k, v in allrules.iteritems() if table == '_'.join(k.split('_')[:2])], key = sortkey))
    betterdict[table] = OrderedDict(sorted([(k, v) for k, v in allrules.iteritems() if table == '_'.join(k.split('_')[:2])], key = sortkey))
    for key, values in betterdict[table].iteritems():
        tableruledict[table][key] = {'subvariables':OrderedDict()}
        for value in values:
            tableruledict[table][key]['subvariables'][value] = {'equation':newdict[value], 'geog':allequations[value]['geog']}
        tableruledict[table][key]['equation'] = newdict[key]
        tableruledict[table][key]['geog'] = allequations[key]['geog']
del betterdict

def orderedlooper(indict):
    for table, tabledata in indict.iteritems():
        for total, totaldata in tabledata.iteritems():
            yield table, total, totaldata['subvariables']

BASE = {'num': [],'mult': [],'den': []}
c = ['Component 1 (istads)', 'Component 2 (istads)', 'Component 3 (istads)', 'Component 4 (istads)', 'Component 5 (istads)', 'Component 6 (istads)', 'Component 7 (istads)', 'Component 8 (istads)']
BASE = OrderedDict([(x, deepcopy(BASE)) for x in c])

outdict = [(table, k) for table, total in tableruledict.iteritems() for k in total]
tempdict = OrderedDict()
for table, k in sorted(outdict):
    if table not in tempdict:
        tempdict[table] = OrderedDict({k:{}})
    else:
        tempdict[table].update({k:{}})
outdict = tempdict
del tempdict
for table, total, subvariables in orderedlooper(tableruledict):
    comp_eqs = [subvariables[x]['equation'] for x in subvariables]
    numerators = []
    denoms = []
    mults = []
    for comp_eq in comp_eqs:
        numerators.append(comp_eq['num'])
        denoms.append(comp_eq['den'])
        mults.append(comp_eq['mult'])
    # this is the part to deal with the components, might not be needed
    if True == False: # this is to prevent this loop from running # type(mults[0]) == dict:
        table2, total2 = table, total
        tempdict = outdict[table][total]['components'] = deepcopy(BASE)
        comptestdict = deepcopy(BASE)
        for mult in mults:
            compdict = mult['components']
            for key, value in compdict.iteritems():
                xdict = comptestdict[key]
                xdict['den'].append(value['den'])
                xdict['mult'].append(value['mult'])
                xdict['num'].append(value['num'])
        for comp in comptestdict:
            if len(set(comptestdict[comp]['den'])) == 1:
                tempdict[comp]['den'] = comptestdict[comp]['den'][0]
            if len(set(comptestdict[comp]['num'])) == 1:
                tempdict[comp]['num'] = comptestdict[comp]['num'][0]
            if len(set(comptestdict[comp]['mult'])) == 1:
                tempdict[comp]['mult'] = comptestdict[comp]['mult'][0]
    else:pass
    #elif type(mults[0]) != dict: #se:
     #   if len(set(mults)) == 1:
      #      outdict[table][total]['mult'] = mults[0]
            
    if len(set(denoms)) ==1:
        outdict[table][total]['den'] = denoms[0]
    if len(set(numerators)) == 1:
        outdict[table][total]['num'] = numerators[0]
    outdict[table][total]['count'] = len(mults)
print 'here'
#print json.dumps(outdict, indent=4)
#print json.dumps(tableruledict, indent=4)
tabledict = {}
# this is where we load in everything into the object database
for tablename, tabledata in tableruledict.iteritems():
    tabledict[tablename] = tablestore.loadfromdict(tablename, tabledata)
print 'woot'

outdict = {}
for key, value in tabledict.iteritems(): 
    outdict[key] = value.dump()
with open('woot','w') as f:
    json.dump(outdict, f, indent=4)
print 'rewoot'
