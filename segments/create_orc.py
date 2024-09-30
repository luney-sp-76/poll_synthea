# Author: Paul Olphert 2023

# This file contains the code to create the ORC segment of the HL7 message
import logging
import traceback


# creates a ORC segment for the HL7 message requires a patient_info object and the hl7 message
def create_orc(hl7, placer_order_num, filler_order_id):
    try:
        hl7.orc.orc_1 = "O"  # New Order
        hl7.orc.orc_2 = placer_order_num  # Some dummy placer order ID up to 75 characters
        hl7.orc.orc_3 = filler_order_id  # Some dummy filler order ID up to 75 characters
    except Exception as ae:
        print("An AssertionError occurred:", ae)
        print(f"Could not create MSH Segment: {ae}")
        logging.error(f"An error of type {type(ae).__name__} occurred. Arguments:\n{ae.args}")
        logging.error(traceback.format_exc())
        return None
    else:
        return hl7