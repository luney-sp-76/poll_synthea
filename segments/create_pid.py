# Author: Paul Olphert 2023

# This file contains the code to create the PID segment of the HL7 message
import datetime
import logging
import traceback
from ..generators.utilities import create_visit_number, create_visit_instiution, PatientInfo

# Creates a PID segment for the HL7 message requires a patient_info object and the hl7 message
def create_pid(patient_info:PatientInfo, hl7):
    try:
       hl7.pid.pid_1 = "1"
       hl7.pid.pid_3 = patient_info.hl7v2_id[0]
       # PID 3 defaults to P
       #hl7.pid.pid_3 = patient_info.id
       hl7.pid.pid_5 = f"{patient_info.last_name}^{patient_info.first_name}^{patient_info.middle_name}"
       if type(patient_info.birth_date) == datetime.date:
           patient_info.birth_date = patient_info.birth_date.isoformat()
       hl7.pid.pid_7 = patient_info.birth_date
       hl7.pid.pid_8 = patient_info.gender[0].upper()
       hl7.pid.pid_11 = f"{patient_info.address}^{patient_info.address_2}^{patient_info.city}^^{patient_info.post_code}^{patient_info.country_code}"
       visitNo = create_visit_number()
       visitInstitution = create_visit_instiution()
       #pid 18 - 1 component 1 COMMON.Visit.num  2 component 1 lab.Request.bill_number 3 component 4 COMMON.Visit.institution 
       hl7.pid.pid_18 = visitNo + "^" + visitInstitution
       #hl7.pid.pid_19 = patient_info.ssn
    except Exception as ae:
        print("An AssertionError occurred:", ae)
        print(f"Could not create MSH Segment: {ae}")
        logging.error(f"An error of type {type(ae).__name__} occurred. Arguments:\n{ae.args}")
        logging.error(traceback.format_exc())

    return hl7