########################################################################
# Name:     run_bulk_jobs.py
# Author:   Jeremy Ulfohn
# Date:     3 June 2024
# Purpose:  Takes .txt file as input and runs all jobs in the file synchronously
#           Creates dict to track failures, and provides log to each failure when the script concludes
########################################################################
import os
import sys
import argparse
import asyncio

import cs_util
import cs_environment as env
from cs_artifact import get_srg_runjob_command
from cs_logging import logmsg, logerr, logheader, logwarning, logsuccess, print_console_note

sys.dont_write_bytecode = True
script_arrow = str(os.path.basename(__file__)) + " -> "


def scrub_line(line):
    """
    Purpose: Scrubs input into valid CTL path, and checks if job is SRG
    Returns: scrubbed_line (str), skip_ctl (boolean)
    """
    original_line = line
    # Option 1. Job code only
    if len(line) == 4 and line.isalnum():
        line = f"praa{line}.ctl"
    # Option 2. Bare CTL file, or ./praaxxxx.ctl
    line = line.replace("./", "")
    if line.startswith("praa"):
        line = "/NAS/mis/esp/scripts/" + line + ("" if line.endswith('.ctl') else ".ctl")
        
    # Check job name for SRG (i.e. 2nd letter is 'i'). In this case, the CTL file will not exist, so query DB for runjob cmd
    if ".ctl" in line:
        if line.split('/NAS/mis/esp/scripts/praa')[1][1] == "i":
            logwarning(script_arrow + f"{original_line} is an SRG; {line} will not exist")
            # If runjob command is found, replace CTL with the valid runjob. Else, skip it
            line = get_srg_runjob_command(line)
            if not line:
                return original_line, True
    
    if original_line != line:
        logmsg(script_arrow + f"Scrubbed {original_line} to {line}")
    return line, False


def has_logfile(command):
    """
    Purpose: Checks if a command is of type "runjob" or "publish", in which case it has a logfile
    Returns: has_logfile (bool)
    """
    if any(script in command for script in ["runjob", "publish"]):
        return True
    return False
    

# Acceptable inputs: /NAS/mis/esp/scripts/praa1234.ctl == praa1234.ctl == 1234
if __name__ == '__main__':
    missing_jira_id = env.current_user_is_production() and len(sys.argv) < 3
    if (len(sys.argv) not in [2,3]) or missing_jira_id:
        logwarning("Usage: python run_bulk_jobs.py <text_file_of_commands> [jira_request]", skip_format=True)
        if missing_jira_id:
            logerr("[jira_request] parameter is required as a service account")
        sys.exit(1)

    input_filename = sys.argv[1]
    if not os.path.exists(os.path.dirname(input_filename)):
        logerr(script_arrow + "The text_file_of_commands parameter must be passed with full, valid path")
        print_console_note("Example: /NAS/mis/tmp/jobs_to_run.txt")
        sys.exit(1)
        
    # Set $WORKING_JIRA_ID, which (as a service account) will be used to bypass runjob's prompt for Jira ID
    if len(sys.argv) == 3:
        os.environ['WORKING_JIRA_ID'] = sys.argv[2]
        
    logheader(script_arrow + "Execution begins")
    print("-" * 96)
    results_dict = {}
    failure_count = skipped_count = 0
    with open(input_filename) as f:
        # Filter list to exclude commented or empty lines
        lines = list(filter(cs_util.check_valid_line, f.readlines()))
        
    total_lines = len(lines)
    
    # Scrub line for proper formatting, modifying lines[] accordingly
    for ind in range(total_lines):
        lines[ind], skip_ctl = scrub_line(lines[ind].strip())
        if skip_ctl:
            results_dict[lines[ind]] = "SKIPPED"
            continue
    
    # Execute each valid command in lines[] asynchronously
    result_tuples = asyncio.run(cs_util.run_commands_async(lines, results_dict=results_dict))
    
    for ind, (command, failed) in enumerate(result_tuples):
        if failed is None:
            logerr(f"(#{ind+1}/{total_lines}) {command} ran long and did not reach endstate", skip_format=True)
            results_dict[command] = "LONGRUN"
            failure_count += 1
        elif failed == 'SKIPPED':
            logwarning(f"(#{ind+1}/{total_lines}) {command} skipped", skip_format=True)
            skipped_count += 1
        elif failed:
            logerr(f"(#{ind+1}/{total_lines}) {command} failed", skip_format=True)
            results_dict[command] = "FAILURE"
            failure_count += 1
        else:
            logsuccess(f"(#{ind+1}/{total_lines}) {command} completed!", skip_format=True)
            results_dict[command] = "SUCCESS"
                   
        # Print separator line after each result
        print("#" * 96, end='\n' if (ind+1 < total_lines) else '\n\n')

    ###############################
    #    RESULTS OVERVIEW CODE    #
    ###############################
    print("#" * 38 + "  RESULTS OVERVIEW  " + "#" * 38, end='\n\n' + "#" * 96 + '\n')
    
    # 1. Skipped, non-empty commands
    if skipped_count > 0:
        logwarning(f"Total amount of jobs skipped: {skipped_count}\n", skip_format=True)
    
    # 2. Failures/long runs. Display results to user, along with logfiles where applicable
    if failure_count > 0:
        logerr(script_arrow + f"Experienced {failure_count} failures. Listed below:", skip_format=True)
        for ind, k in enumerate(results_dict):
            v = results_dict[k]
            
            if v == "LONGRUN":
                logmsg(f"(#{ind+1}) Job {k} ran long or did not reach end state")
                if has_logfile(k):
                    print_console_note("Logfile available at: " + cs_util.get_runjob_logfile(k))             
            elif v == "FAILURE":
                logmsg(f"(#{ind+1}) Job {k} failed")
                if has_logfile(k):
                    print_console_note("Logfile available at: " + cs_util.get_runjob_logfile(k))           
            elif v == "SKIPPED":
                logmsg(f"(#{ind+1}) Job {k} skipped. Check that associated SRG exists and retry")
                
    # 3. Full success
    else:
        logsuccess(script_arrow + "No failures detected!")
    
    logmsg(script_arrow + "Execution ends")
run_bulk_jobs.txt
Displaying run_bulk_jobs.txt.
