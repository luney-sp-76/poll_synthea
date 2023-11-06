import logging
import traceback
import utilities.create_obr_time as create_obr_time


# Creates a OBR segment for the HL7 message requires a patient_info object and the hl7 message
def create_evn(patient_info, placer_order_num, filler_order_id, hl7):
    try:
        request_date = create_obr_time.create_obr_time()
        observation_date = create_obr_time.create_obr_time()
        quantity_timing = create_obr_time.create_obr_time()

        hl7.evn.evn_1 = "A03"
        hl7.evn.evn_2 = request_date
        
    except Exception as ae:
        print("An AssertionError occurred:", ae)
        print(f"Could not create EVN Segment: {ae}")
        logging.error(f"An error of type {type(ae).__name__} occurred. Arguments:\n{ae.args}")
        logging.error(traceback.format_exc())

    return hl7