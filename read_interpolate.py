import csv
import sys
import pickle
import re
from create_do_from_jims_csv import makesql
from create_do_from_jims_csv import alterstr, updatestr

PROCESSDICT = {
    '1':{'numerator':'istads_id', 'denominator':'Denominator (istads)', 'multiplier': ['Multiplier (istads)']},
    '2': {'numerator': 1,'denominator': 1,
    'multiplier': 
    ['Component 1 (istads)', 'Component 2 (istads)', 'Component 3 (istads)', 'Component 4 (istads)', 
    'Component 5 (istads)', 'Component 6 (istads)', 'Component 7 (istads)', 'Component 8 (istads)', 
    'Component 9 (istads)', 'Component 10 (istads)', 'Component 11 (istads)', 'Component 12 (istads)', 
    'Component 13 (istads)', 'Component 14 (istads)', 'Component 15 (istads)', 'Component 16 (istads)', 
    'Component 17 (istads)', 'Component 18 (istads)', 'Component 19 (istads)', 'Component 20 (istads)']},
    '3':{'numerator':'istads_id', 'denominator':'Denominator for Component Multiplier (istads)',
    'multiplier':
    ['Component 1 (istads)', 'Component 2 (istads)', 'Component 3 (istads)', 'Component 4 (istads)', 
    'Component 5 (istads)', 'Component 6 (istads)', 'Component 7 (istads)', 'Component 8 (istads)', 
    'Component 9 (istads)', 'Component 10 (istads)', 'Component 11 (istads)', 'Component 12 (istads)', 
    'Component 13 (istads)', 'Component 14 (istads)', 'Component 15 (istads)', 'Component 16 (istads)', 
    'Component 17 (istads)', 'Component 18 (istads)', 'Component 19 (istads)', 'Component 20 (istads)']}
     }

ADD_COL = 'alter table {0} add column {1};'
UPDATE_COL = 'update {0} set {1} = {2};'
TABLE = 'tester'

def processrow(inrow):
    column = inrow['istads_id']
    column = '_'.join(column.split('.'))
    inrow[1] = 1
    dictionary = PROCESSDICT[inrow['Method']]
    num = inrow[dictionary['numerator']]
    den =  inrow[dictionary['denominator']]
    mult =  dictionary['multiplier']
    mult = [inrow[x] for x in mult]
    mult = [x for x in mult if x != '']
    mult = ' + '.join(mult)
    matches = re.findall('\((\S*? / \S*?)\)', mult)
    if matches:
        for match in matches:
            x = '({0})'.format(match)
            mult1, den1 = match.split(' / ')
            mult = mult.replace(x, ' case when cast ({0} as int) <> 0 then ({1}) else 1 end '.format(den1, x))
    if den and den != 1:
        statement = ' case when cast ({1} as int) <> 0 then ({0} / {1}) else 1 end * ({2}) '.format(num, den, mult)
    elif den:
        statement = '{0}'.format(mult)
    else:
        pass # this is the table headers
    if 'statement' in vars().keys():
        try:
            statement = makesql('gen {0} = {1}'.format(column, statement.replace('.', '_')))
            column = '_'.join(statement[4:].split(' = ')[0].split('_'))
            table = '_'.join(statement.split('_')[:2])
            print alterstr.format(column, table)
            print updatestr.format(statement, table)            
        except:
            pass

def run(infile):
    f = open(infile)
    reader = csv.DictReader(f)
    mylist = [r for r in reader]
    mylist = [r for r in mylist if r['Method']]
    fieldnames = reader.fieldnames
    for row in mylist:
        processrow(row)

def main():
    infile = sys.argv[1]
    run(infile)
    print 'done'

if __name__ == '__main__':
    main()
