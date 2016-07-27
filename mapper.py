import csv
import math
import sys

csv.register_dialect(
    'mydialect',
    delimiter = ',',
    quotechar = '"',
    doublequote = True,
    skipinitialspace = True,
    lineterminator = '\r\n',
    quoting = csv.QUOTE_MINIMAL)

print('\n Output from an iterable object created from the csv file')
for line in sys.stdin:
    line = line.strip()
    thedata = csv.reader([line])
    for row in thedata:
        # currentKey = '%s\t%s' % (row[0], row[3])
        # print "%s\t%s\t%s\t%s" % (row[0],row[1],row[3],row[4])
        try:
            print "%d\t%s\t%d\t%d" % (int(row[0]),row[3],((int(row[4])/10)*10),int(row[1]))
        except ValueError:
            continue