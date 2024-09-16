# This file contains the code to create an OBX segment of the HL7 message
import logging
import traceback

def create_obx(hl7):
    try:
        hl7.obx.obx_1 = "1"  # New Order
        hl7.obx.obx_2 = "TX"
        hl7.obx.obx_3 = "R-ANKLE^Ankle X-ray^L"
        hl7.obx.obx_5 = "Normal findings, no fracture detected"
        hl7.obx.obx_11 = "F"
    except Exception as ae:
        print("An AssertionError occurred:", ae)
        print(f"Could not create MSH Segment: {ae}")
        logging.error(f"An error of type {type(ae).__name__} occurred. Arguments:\n{ae.args}")
        logging.error(traceback.format_exc())

    return hl7