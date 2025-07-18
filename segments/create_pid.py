# Author: Paul Olphert 2023

# This file contains the code to create the PID segment of the HL7 message
import logging
import traceback
from generators.utilities import (
    create_visit_number,
    create_visit_instiution,
    PatientInfo
)

# Creates a PID segment for the HL7 message
# requires a patient_info object and the hl7 message


def create_pid(patient_info: PatientInfo, hl7):
    try:
        hl7.pid.pid_1 = "1"

        # Handle HL7v2 ID - ensure it's properly formatted
        if hasattr(patient_info, 'hl7v2_id') and patient_info.hl7v2_id:
            if isinstance(patient_info.hl7v2_id, list):
                # If it's a list, take the first element
                pid_3_value = (
                    patient_info.hl7v2_id[0] if patient_info.hl7v2_id else None
                )
            else:
                # If it's a string, use it directly
                pid_3_value = patient_info.hl7v2_id
        else:
            # Fallback to regular id if hl7v2_id is not available
            pid_3_value = getattr(patient_info, "id", None)

        if not isinstance(pid_3_value, str) or not pid_3_value.strip():
            raise ValueError("PID_3 value must be a non-empty string")

        hl7.pid.pid_3 = pid_3_value
        hl7.pid.pid_5 = (
            f"{patient_info.last_name}^"
            f"{patient_info.first_name}^"
            f"{patient_info.middle_name}"
        )
        hl7.pid.pid_7 = patient_info.birth_date.strftime("%Y%m%d")
        hl7.pid.pid_8 = patient_info.gender[0].upper()

        # Now state should be available
        hl7.pid.pid_11 = (
            f"^^^{patient_info.city}^{patient_info.state}"
            f"^{patient_info.post_code}^{patient_info.country}"
        )

        visitNo = create_visit_number()
        visitInstitution = create_visit_instiution()
        # pid 18 - Visit code
        hl7.pid.pid_18 = visitNo + "^" + visitInstitution

    except Exception as ae:
        print("An AssertionError occurred:", ae)
        print(f"Could not create PID Segment: {ae}")  # Fixed error message
        logging.error(
            (
                f"An error of type {type(ae).__name__} occurred. "
                f"Arguments:\n{ae.args}"
            )
        )
        logging.error(traceback.format_exc())

    return hl7
