import sys
import time
import json
from datetime import datetime

connlog_path = sys.argv[1] 
result_path = str(connlog_path) + ".labeled" 
logfile = "benign_label_adder.log"

print("Bro Column adder logs at: " + str(logfile))

logfile_logical = open(logfile,"w+")

datetime = datetime.now().strftime("%d/%m/%y %H:%M:%S")
logfile_logical.write("Bro Column adder started at:" + str(datetime) + "\n")
label = "benign"

with open(connlog_path, 'r') as istr:
    with open(result_path, 'w') as ostr:
        cnt = 1
        for line in istr:
            if cnt == 7:
                line = line.rstrip('\n') + '   label' + '   detailed-label'
                print(line, file=ostr)
            if cnt == 8:
                line = line.rstrip('\n') + '   string' + '   string'
                print(line, file=ostr)
            elif(cnt > 8):
                line = line.rstrip('\n')
                try:
                    line = line.rstrip('\n') + '   ' + str(label) + '   -'
                    print(line, file=ostr)
                except Exception as e:
                    msg = "Exception {} at line {} ".format(str(e))
                    msg = msg.rstrip('\n')
                    print(str(msg))
                    logfile_logical.write(str(msg))
                    pass
            elif(cnt < 7):
                line = line.rstrip('\n')
                print(line, file=ostr)
            cnt += 1
            if (cnt % 100000) == 0:
                print("Analyzing {} flows".format(str(cnt)))