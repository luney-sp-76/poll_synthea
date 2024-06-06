# Author: Paul Olphert 2023

# This file contains the code to create the OBR segment of the HL7 message

import logging
import traceback
from generators.utilities import create_obr_time


# Creates a OBR segment for the HL7 message requires a patient_info object and the hl7 message
def create_obr(patient_info, placer_order_num, filler_order_id, hl7):
    try:
        request_date = create_obr_time()
        observation_date = create_obr_time()
        quantity_timing = create_obr_time()

        hl7.obr.obr_1 = "1"  # Set ID
        hl7.obr.obr_2 = placer_order_num  # Some dummy placer order ID up to 75 characters
        hl7.obr.obr_3 = filler_order_id  # Some dummy filler order ID up to 75 characters
        hl7.obr.obr_4 = patient_info.id  # some test code hl7.Orders.ext_code 
        #Requested Date/Time lab.Request.date_refer 
        hl7.obr.obr_6 = request_date
        # Observation Date/Time lab.Resultp_extra.doc_date
        hl7.obr.obr_7 = observation_date  
        #Ordering Provider 2 component 1 component 1 lab.Resultp_extra.doc_ordering  Default 'WACON'2 component 1 lab.Request.doctor Default 'TEST'
        hl7.obr.obr_16 = "WACON^TEST"
        #Diagnostic Service ID  2 components 1^2 component 2 lab.Request.lab 
        hl7.obr.obr_24 = "BI^UHC"
        #Quantity/Timing 6 component s 6 components1 component 4 lab.Request.date_service,lab.Request.time_service2 component 4 lab.Request.date_coln,lab.Request.time_coln 3 component 6 lab.Request.priority_coln 
        hl7.obr.obr_27 = f"^^^{quantity_timing}^^E"
    except Exception as ae:
        print("An AssertionError occurred:", ae)
        print(f"Could not create MSH Segment: {ae}")
        logging.error(f"An error of type {type(ae).__name__} occurred. Arguments:\n{ae.args}")
        logging.error(traceback.format_exc())

    return hl7