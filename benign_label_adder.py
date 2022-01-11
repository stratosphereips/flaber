import sys
import time
import json
from datetime import datetime

usage="Usage: python3 benign_label_adder.py conn.log"

# Label to be added to the conn.log file
label = "benign"

# Receive as input the conn.log to label
connlog_file = sys.argv[1] 
if (len(sys.argv) < 2):
    print(usage)
    sys.exit(1)

# Write the labeled conn.log as a new file conn.log.labeled
labeled_connlog_file = str(connlog_file) + ".labeled" 


try: 
    with open(connlog_file, 'r') as inputconnlog:
        with open(labeled_connlog_file, 'w') as outputconnlog:
            # The line_number counter helps knowing where to add headers and where to add labels, and which lines to ignore
            line_number = 1

            # We process the conn.log file line by line to add labels
            for line in inputconnlog:

                # Ignore the first 6 lines of Zeek header
                if(line_number < 7):
                    line = line.rstrip('\n')
                    print(line, file=outputconnlog)

                # Header line number 7 contains the column names
                elif line_number == 7:
                    line = line.rstrip('\n') + '   label' + '   detailed-label'
                    print(line, file=outputconnlog)

                # Header line number 8 contains the column data type
                elif line_number == 8:
                    line = line.rstrip('\n') + '   string' + '   string'
                    print(line, file=outputconnlog)

                # All the rest are network flows
                elif(line_number > 8):
                    line = line.rstrip('\n')
                    try:
                        line = line.rstrip('\n') + '   ' + str(label) + '   -'
                        print(line, file=outputconnlog)
                    except Exception as e:
                        print(e)
                        pass
                # Increase line counter to move to the next line in the input log
                line_number += 1
                if (line_number % 100000) == 0:
                    print("Analyzing {} flows".format(str(line_number)))
except Exception as e:
    print(e)
    sys.exit(1)
