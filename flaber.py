import csv
import json
import sys
import time
import os
from parsezeeklogs import ParseZeekLogs
from datetime import datetime

helptext="Usage: python3 flaber.py conn.log labels.csv"
logfile = "flaber.log"

if (len(sys.argv) < 2):
    print(helptext)

print("Flaber logs at: " + logfile)

def labeler(log_record, labelsjson):
    # Parsing flows to json
    log_record = json.loads(log_record)
    labelsjson = json.loads(labelsjson)
    
    if len(labelsjson) == 0:
        return False
    
    if len(log_record) == 0:
        return False
    
    i=1
    ok_labels=[]
    # For every label in label file
    for row in labelsjson:        
        try: 
            if len(row.keys()) != 8:
                print("ERROR: labels.csv fie has a wrong format: {}".format(str(row)))
                logic_logfile.write("ERROR: labels.csv fie has a wrong format: {}".format(str(row)))
                pass

            labeled_row = {}
            i+=1
            flowdata = log_record[str(row["Field"])]
            labeled_row["id"] = row["Id"]
            labeled_row["ok"] = False
            # Checking multiple connected label criteria
            if len(row["connector"]) > 1:
                labeled_row["connect"] = row["connector"]
            else:
                labeled_row["connect"] = False
            
            # Checking comparators
            if row["Comparator"] == "eq":

                # Comparing data
                if str(flowdata) == str(row["Data"]):
                    # Adding labels
                    labeled_row["label"] = row["Label"]
                    labeled_row["ok"] = True
                    
            if row["Comparator"] == "gt":
                if int(flowdata) > int(row["Data"]):
                    labeled_row["label"] = row["Label"]
                    labeled_row["ok"] = True
                    
            if row["Comparator"] == "lt":
                if int(flowdata) < int(row["Data"]):
                    labeled_row["label"] = row["Label"]
                    labeled_row["ok"] = True

            # Accounting ok labels
            ok_labels.append(labeled_row)

        except Exception as e:
            print("Exception in labeler function: {}".format(str(e)))
            logic_logfile.write("Exception in labeler function: {} \n ".format(str(e)))
            logic_logfile.write("Exception at row: {} \n ".format(str(row)))
            logic_logfile.write("Exception at flow {} \n ".format(str(log_record)))
           
    # Labeling the ok ones
    try:
        for label in ok_labels:
            haslabel = False    
            if label["ok"]:
                haslabel = True
                # Formating label string
                
                
                if not "label" in log_record:
                    log_record["label"] = ""
                elif(len(log_record["label"]) > 1 and not log_record["label"][-1] == "-"):
                    log_record["label"] += "-"
                
                # Cheking multiple connected label criterias that were ok
                if label["connect"] and label["connect"].split(" ")[1] and label["connect"].split(" ")[1] != " ":
                    conn_id = label["connect"].split(" ")[1]
                    # Combining multiple ok labeled criterias

                    if ok_labels[int(conn_id) - 1]["ok"] and label["ok"]:
                        # Labeling
                    
                        if label["label"] not in log_record["label"]:
                            log_record["label"] += label["label"]
                            
                else:
                    # Labeling
                    if label["label"] not in log_record["label"]:
                        log_record["label"] += label["label"]

    except Exception as e:
        raise(e)

    try:
        if not "label" in log_record or len(log_record["label"]) < 1:
            log_record["label"] = "-"
        else:
            if str(log_record["label"])[-1] == "-":
                log_record["label"] = str(log_record["label"])[0:-1]

    except Exception as e:
       raise(e)

    return log_record

connlog_path = sys.argv[1]
labelsfile_path = sys.argv[2]


# Writing log file data
logic_logfile = open(logfile,"w")
datetimelog = datetime.now().strftime("%d/%m/%y %H:%M:%S")
logic_logfile.write("Flaber started at:" + str(datetime) + "\n")

try:
    # Open the CSV  
    f = open(labelsfile_path, 'rU' )  
    # Change each fieldname to the appropriate field name. I know, so difficult.  
    reader = csv.DictReader(f)  
    # Parse the CSV into JSON  
except Exception as e:
    print("Exception opening conn.log file: {}".format(str(e)))
    logic_logfile.write("Exception opening conn.log file: {} \n ".format(str(e)))
        

try:
    result_file = "out.json"
    
    labelsjson = json.dumps( [ row for row in reader ] )  
    conn_file_lines = os.popen('wc -l ' + str(connlog_path) + ' | cut -d" " -f1').read()
    conn_file_lines = int(conn_file_lines) - 8
    info_msg = "Labeling conn.log file: " + str(connlog_path) + " with " + str(conn_file_lines) +  " lines \n"
    logic_logfile.write(info_msg)
    print(info_msg)

    count = 1
    # Analyzing, labeling and writing out.csv file
    with open(result_file,"w") as outfile:
        # Reading flows from conn.log
        for log_record in ParseZeekLogs(connlog_path, output_format="json", safe_headers=False):
            if log_record is not None:
                # Analyzing and labelig flows
                log_record = labeler(log_record, labelsjson)
                #print(json.dumps(log_record))
                # Writing resulting labeled flows
                outfile.write(json.dumps(log_record) + "\n")
                count += 1
                if (count % 100000 == 0):
                    print("Analyzing {} flows ".format(count))
    
    print("Labeled: {} of {} flows".format(count, str(conn_file_lines)))
    print("Labeled flow written to {}".format(result_file))

except Exception as e:
    print("Exception opening {} file: {}".format(result_file, str(e)))
    logic_logfile.write("Exception opening {} file: {} \n ".format(result_file, str(e)))
    raise(e)

datetimelog = datetime.now().strftime("%d/%m/%y %H:%M:%S")
logic_logfile.write("Flaber finished at:" + str(datetimelog) + "\n")

try:
    f.close()
    logic_logfile.close()
except Exception as e:
    print("Exception closing files: {}".format(str(e)))
    logic_logfile.write("Exception closing files: {} \n ".format(str(e)))
