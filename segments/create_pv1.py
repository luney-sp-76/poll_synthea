# Author: Paul Olphert 2023

# This file contains the code to create the PV1 segment of the HL7 message
import logging
import traceback
from generators.utilities import create_visit_instiution



# Creates a PV1 segment for the HL7 message requires a patient_info object and the hl7 message
def create_pv1(patient_info, hl7):
    try:
        hl7.pv1.pv1_1 = "1"  # Set Patient Class to Inpatient
        hl7.pv1.pv1_2 = "O"  # Set Visit Number
        hl7.pv1.pv1_3 = create_visit_instiution()  # Set Visit Institution
        hl7.pv1.pv1_7 = "^ACON"  # Set Patient Class to Inpatient
        hl7.pv1.pv1_8 = "^ANAESTHETICS CONS^^^^^^L"  # Set Patient Type to Ambulatory
        hl7.pv1.pv1_9 = "^ANAESTHETICS CONS^^^^^^^AUSHICPR"
    except Exception as ae:
        print("An AssertionError occurred:", ae)
        print(f"Could not create PV1 Segment: {ae}")
        logging.error(f"An error of type {type(ae).__name__} occurred. Arguments:\n{ae.args}")
        logging.error(traceback.format_exc())

    return hl7