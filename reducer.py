
#!/usr/bin/env python

from operator import itemgetter
import sys
current_time = None

#
current_liv=0
current_dead=0
current_gender=None
current_class=0
current_age=0
count=0
# input comes from STDIN
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    # times,count=line.split("\t",1)
    line=line.split("\t")


    if len(line)==4:
        pclass=line[0]
        gender = line[1]
        age=line[2]
        surv=line[3]

        try:
            # print "in try catch"

            pclass=int(pclass)
            surv=int(surv)
            age=int(age)

        except ValueError:
            continue

        # key formation based on class and gender
        # this IF-switch only works because Hadoop sorts map output
        # by key class and gender  before it is passed to the reducer
        if current_class == pclass and current_gender==gender and current_age==age:
            if surv == 1:
                current_liv = current_liv + 1
            else:
                current_dead = current_dead + 1

        else:
            if current_class and current_gender and current_age:
                print '%d\t%s\t%d\t%d\t%d' % (current_class,current_gender,current_age,current_liv, current_dead)

            current_liv = 0
            current_dead = 0
            current_class=pclass
            current_gender=gender
            current_age=age

if current_class and current_gender and current_age==age:
    print '%d\t%s\t%d\t%d\t%d' % (current_class, current_gender,current_age, current_liv, current_dead)


