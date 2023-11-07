import logging
import traceback
from generators.utilities import create_obr_time


# Creates a OBR segment for the HL7 message requires a patient_info object and the hl7 message
def create_evn(hl7):
    try:
        request_date = create_obr_time()

        hl7.evn.evn_1 = "A03"
        hl7.evn.evn_2 = request_date
        
    except Exception as ae:
        print("An AssertionError occurred:", ae)
        print(f"Could not create EVN Segment: {ae}")
        logging.error(f"An error of type {type(ae).__name__} occurred. Arguments:\n{ae.args}")
        logging.error(traceback.format_exc())

    return hl7