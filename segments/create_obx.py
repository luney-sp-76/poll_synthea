# This file contains the code to create an OBX segment of the HL7 message
import logging
import traceback

def create_obx(hl7, result_type:str="TX", panel_code_desc:str="R-ANKLE^Ankle X-ray^L", 
               result="Normal findings, no fracture detected", units:str=None):
    try:
        # New Order
        hl7.obx.obx_1 = "1"  
        
        # Type of result (text, numeric, etc.)
        hl7.obx.obx_2 = result_type
        
        # Panel code, description, ?
        hl7.obx.obx_3 = panel_code_desc
        
        # Result 
        hl7.obx.obx_5 = result
        
        if result_type != "TX" and units:
            hl7.obx.obx_6 = units
        
        # Status of result 
        hl7.obx.obx_11 = "F" 
        
    except Exception as ae:
        print("An AssertionError occurred:", ae)
        print(f"Could not create MSH Segment: {ae}")
        logging.error(f"An error of type {type(ae).__name__} occurred. Arguments:\n{ae.args}")
        logging.error(traceback.format_exc())
        return None
    else:
        return hl7