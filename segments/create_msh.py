# Author: Paul Olphert 2023

# This file contains the code to create the MSH segment of the HL7 message
import logging
import traceback
from datetime import date
from pathlib import Path
from hl7apy import core


def create_msh(messageType, control_id, hl7, current_date):
# Initialize msh to None
    msh = None

    # Add MSH Segment
    try:
        # convert the message type to a string replacing the underscore with ^
        messageTypeSegment = str(messageType)
        messageTypeSegment = messageTypeSegment.replace("_", "^")

        hl7.msh.msh_3 = "ULTRA"  # Sending Application
        hl7.msh.msh_4 = "TEST"  # Sending Facility
        hl7.msh.msh_5 = "ULTRA"  # Receiving Application
        hl7.msh.msh_6 = "NUFFIELD"  # Receiving Facility
        hl7.msh.msh_7 = current_date.strftime("%Y%m%d%H%M")  # Date/Time of Message
        hl7.msh.msh_9 = messageTypeSegment  # Message Type
        hl7.msh.msh_10 = control_id  # Message Control ID
        hl7.msh.msh_11 = "T"  # Processing ID
        hl7.msh.msh_12 = "2.4"  # Version ID
        hl7.msh.msh_15 = "AL"  # Accept Acknowledgment Type
        hl7.msh.msh_16 = "NE"  # Application Acknowledgment Type
    except Exception as ae:
        print("An AssertionError occurred:", ae)
        print(f"Could not create MSH Segment: {ae}")
        logging.error(
            f"An error of type {type(ae).__name__} occurred. Arguments:\n{ae.args}"
        )
        logging.error(traceback.format_exc())
        
        return None
    else:
        return hl7