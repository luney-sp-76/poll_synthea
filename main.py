import logging
import string
from typing import Optional
import traceback
import glob
import firebase_admin
from firebase_admin import credentials, firestore
from fhir.resources.R4B.bundle import Bundle
from fhir.resources.R4B.patient import Patient
from datetime import date
from pathlib import Path
from hl7apy import core

BASE_DIR = Path.cwd()
work_folder_path = BASE_DIR / "Work"
hl7_folder_path = BASE_DIR / "HL7_v2"
MESSAGE_CONTROL_ID = 1000


def create_orm_message(patient_info, messageType):
    global MESSAGE_CONTROL_ID
    # Create empty HL7 message
    hl7 = core.Message("ORM_O01", version="2.2")    

     # Initialize msh to None
    msh = None
    
    # Add MSH Segment
    try:
        hl7.msh.msh_3 = "HL7Server"  # Sending Application
        hl7.msh.msh_4 = "HL7Server"  # Sending Facility
        hl7.msh.msh_5 = "HL7Client"  # Receiving Application
        hl7.msh.msh_6 = "HL7Client"  # Receiving Facility
        hl7.msh.msh_7 = date.today().strftime("%Y%m%d%H%M%S")  # Date/Time of Message
        hl7.msh.msh_9 = "ORM^O01"  # Message Type
        hl7.msh.msh_10 = str(MESSAGE_CONTROL_ID)  # Message Control ID
        MESSAGE_CONTROL_ID += 1
        hl7.msh.msh_11 = "P"  # Processing ID
        hl7.msh.msh_12 = "2.5"  # Version ID
    except Exception as ae:
        print("An AssertionError occurred:", ae)
        print(f"Could not create MSH Segment: {ae}")
        logging.error(f"An error of type {type(ae).__name__} occurred. Arguments:\n{ae.args}", date.today().strftime("%Y%m%d%H%M%S"))
        logging.error(traceback.format_exc())

   # Add PID Segment
    try:
       hl7.pid.pid_3 = patient_info.id
       hl7.pid.pid_5 = f"{patient_info.last_name}^{patient_info.first_name}"
    except Exception as ae:
        print("An AssertionError occurred:", ae)
        print(f"Could not create MSH Segment: {ae}")
        logging.error(f"An error of type {type(ae).__name__} occurred. Arguments:\n{ae.args}")
        logging.error(traceback.format_exc())   
        
    
    # Add ORC Segment for the Order (dummy data for example)
    try:
        hl7.orc.orc_1 = "NW"  # New Order
        hl7.orc.orc_2 = "1234"  # Some dummy order ID
    except Exception as ae:
        print("An AssertionError occurred:", ae)
        print(f"Could not create MSH Segment: {ae}")
        logging.error(f"An error of type {type(ae).__name__} occurred. Arguments:\n{ae.args}")
        logging.error(traceback.format_exc())
   
  
    
    # Add OBR Segment for the Order details (dummy data for example)
    try:
        hl7.obr.obr_4 = "TestCode"  # some test code
    except:
        print("An AssertionError occurred:", ae)
        print(f"Could not create MSH Segment: {ae}")
        logging.error(f"An error of type {type(ae).__name__} occurred. Arguments:\n{ae.args}")
        logging.error(traceback.format_exc())
    
    return hl7


class PatientInfo:
    def __init__(self, id, birth_date, gender, ssn, first_name, last_name, city, state, country, postal_code, age):
        self.id = id
        self.birth_date = birth_date
        self.gender = gender
        self.ssn = ssn
        self.first_name = first_name
        self.last_name = last_name
        self.city = city
        self.state = state
        self.country = country
        self.postal_code = postal_code
        self.age = age


def calculate_age(birth_date):
    today = date.today()
    age = today.year - birth_date.year - ((today.month, today.day) < (birth_date.month, birth_date.day))
    return age


def parse_fhir_message(fhir_message):
    # Parse the FHIR JSON message into a Bundle
    bundle = Bundle.parse_raw(fhir_message)

    # Extract information from the Bundle
    patient_info = None

    # Extract information from the Bundle
    print("Bundle Type:", bundle.type)
    print("Entry Count:", len(bundle.entry))

    for entry in bundle.entry:
        resource = entry.resource
        if isinstance(resource, Patient):
            birth_date = resource.birthDate
            age = calculate_age(birth_date)
            ssn = None
            for identifier in resource.identifier:
                if identifier.system == "http://hl7.org/fhir/sid/us-ssn":
                    ssn = identifier.value
                    break
            patient_info = PatientInfo(
                id=resource.id,
                birth_date=birth_date,
                gender=resource.gender,
                ssn=ssn,
                first_name=resource.name[0].given[0],
                last_name=resource.name[0].family,
                city=resource.address[0].city,
                state=resource.address[0].state,
                country=resource.address[0].country,
                postal_code=resource.address[0].postalCode,
                age=age
            )
            break  # Assuming there's only one patient resource per FHIR message
    return patient_info

def initialize_firestore() -> firestore.client:
    """Initialize Firestore client and return it."""
    json_file = glob.glob("firebase/*.json")[0]
    if not json_file:
        logging.error("No Firebase credentials file found. Exiting.")
        exit(1)
    cred = credentials.Certificate(json_file)
    firebase_admin.initialize_app(cred)
    return firestore.client()

def save_to_firestore(db: firestore.client, patient_info: PatientInfo) -> None:
    """Save patient info to Firestore."""
    patient_id = patient_info.id
    patient_ref = db.collection("full_fhir").document(patient_id)
    if patient_ref.get().exists:
        logging.info(f"Patient with ID {patient_id} already exists in Firestore. Skipping.")
    else:
        patient_data = {
            "id": patient_info.id,
            "birth_date": patient_info.birth_date.isoformat(),
            # ... (rest of the fields)
        }
        patient_ref.set(patient_data)
        logging.info(f"Added patient with ID {patient_id} to Firestore.")

def main():
    # Initialize Firebase Admin SDK with your credentials
    db = initialize_firestore()
    try:
        # Iterate through FHIR JSON files in the work folder
        for file in work_folder_path.glob("*.json"):
            with open(file, "r") as f:
                fhir_message = f.read()
                patient_info = parse_fhir_message(fhir_message)
                if patient_info:
                    hl7_message = create_orm_message(patient_info, "ORM^O01")
                    print("Generated HL7 message:", str(hl7_message))
                    save_hl7_message_to_file(hl7_message, patient_info.id)
                    patient_id = patient_info.id
                    patient_ref = db.collection("full_fhir").document(patient_id)
                    # Check if patient already exists
                    if patient_ref.get().exists:
                        print(f"Patient with ID {patient_id} already exists in Firestore. Skipping.")
                    else:
                        # Add patient to Firestore
                        patient_data = {
                            "id": patient_info.id,
                            "birth_date": patient_info.birth_date.isoformat(),
                            "gender": patient_info.gender,
                            "ssn": patient_info.ssn,
                            "first_name": patient_info.first_name,
                            "last_name": patient_info.last_name,
                            "city": patient_info.city,
                            "state": patient_info.state,
                            "country": patient_info.country,
                            "postal_code": patient_info.postal_code,
                            "age": patient_info.age
                        }
                        patient_ref.set(patient_data)
                        print(f"Added patient with ID {patient_id} to Firestore.")
    except Exception as e:
        logging.error(f"An error of type {type(e).__name__} occurred. Arguments:\n{e.args}")
        logging.error(traceback.format_exc())


def save_hl7_message_to_file(hl7_message, patient_id):
    hl7_file_path = hl7_folder_path / f"{patient_id}.hl7"
    with open(hl7_file_path, "w") as hl7_file:
        hl7_file.write(str(hl7_message.msh.value) + "\r")
        hl7_file.write(str(hl7_message.pid.value) + "\r")
        hl7_file.write(str(hl7_message.orc.value) + "\r")
        hl7_file.write(str(hl7_message.obr.value) + "\r") 


if __name__ == "__main__":
    logging.basicConfig(filename="main.log", level=logging.INFO)
    import poll_synthea
    poll_synthea.call_for_patients()
    main()


# import glob
# import firebase_admin
# from firebase_admin import credentials, firestore
# from fhir.resources.R4B.bundle import Bundle
# from fhir.resources.R4B.patient import Patient
# from datetime import date
# from pathlib import Path
# import hl7

# BASE_DIR = Path.cwd()
# work_folder_path = BASE_DIR / "Work"

# def create_orm_message(patient_info):
#     # Create empty HL7 message
#     hl7_msg = hl7.Message()
    
#     # Add MSH Segment
#     msh = hl7.Segment("MSH")
#     msh[1] = '|'
#     msh[2] = '^~\&'
#     msh[3] = "HL7Server"  # Sending Application
#     msh[4] = "HL7Server"  # Sending Facility
#     msh[5] = "HL7Client"  # Receiving Application
#     msh[6] = "HL7Client"  # Receiving Facility
#     msh[7] = date.today().strftime("%Y%m%d%H%M%S")  # Date/Time of Message
#     msh[9] = "ORM^O01"  # Message Type
#     msh[10] = str(hl7_msg.next())  # Message Control ID
#     msh[11] = "P"  # Processing ID
#     msh[12] = "2.5"  # Version ID

#     hl7_msg.append(msh)

    
#     # Add PID Segment
#     pid = hl7.Segment("PID")
#     pid[3] = patient_info.id
#     pid[5] = f"{patient_info.last_name}^{patient_info.first_name}"
#     hl7_msg.append(pid)
    
#     # Add ORC Segment for the Order (dummy data for example)
#     orc = hl7.Segment("ORC")
#     orc[1] = "NW"  # New Order
#     orc[2] = "1234"  # Some dummy order ID
#     hl7_msg.append(orc)
    
#     # Add OBR Segment for the Order details (dummy data for example)
#     obr = hl7.Segment("OBR")
#     obr[4] = "TestCode"  # some test code
#     hl7_msg.append(obr)
    
#     return hl7_msg


# class PatientInfo:
#     def __init__(self, id, birth_date, gender, ssn, first_name, last_name, city, state, country, postal_code, age):
#         self.id = id
#         self.birth_date = birth_date
#         self.gender = gender
#         self.ssn = ssn
#         self.first_name = first_name
#         self.last_name = last_name
#         self.city = city
#         self.state = state
#         self.country = country
#         self.postal_code = postal_code
#         self.age = age


# def calculate_age(birth_date):
#     today = date.today()
#     age = today.year - birth_date.year - ((today.month, today.day) < (birth_date.month, birth_date.day))
#     return age


# def parse_fhir_message(fhir_message):
#     # Parse the FHIR JSON message into a Bundle
#     bundle = Bundle.parse_raw(fhir_message)

#     # Extract information from the Bundle
#     patient_info = None

#     # Extract information from the Bundle
#     print("Bundle Type:", bundle.type)
#     print("Entry Count:", len(bundle.entry))

#     for entry in bundle.entry:
#         resource = entry.resource
#         if isinstance(resource, Patient):
#             birth_date = resource.birthDate
#             age = calculate_age(birth_date)
#             ssn = None
#             for identifier in resource.identifier:
#                 if identifier.system == "http://hl7.org/fhir/sid/us-ssn":
#                     ssn = identifier.value
#                     break
#             patient_info = PatientInfo(
#                 id=resource.id,
#                 birth_date=birth_date,
#                 gender=resource.gender,
#                 ssn=ssn,
#                 first_name=resource.name[0].given[0],
#                 last_name=resource.name[0].family,
#                 city=resource.address[0].city,
#                 state=resource.address[0].state,
#                 country=resource.address[0].country,
#                 postal_code=resource.address[0].postalCode,
#                 age=age
#             )
#             break  # Assuming there's only one patient resource per FHIR message
#     return patient_info

# def main():
#     # Initialize Firebase Admin SDK with your credentials
#     json_file = glob.glob("firebase/*.json")[0]
#     if not json_file:
#         print("No Firebase credentials file found. Exiting.")
#         exit(1) # Exit with error code 1
#     else:
#         cred = credentials.Certificate(json_file)
#         firebase_admin.initialize_app(cred)

#     db = firestore.client()

#     # Iterate through FHIR JSON files in the work folder
#     for file in work_folder_path.glob("*.json"):
#         with open(file, "r") as f:
#             fhir_message = f.read()
#             patient_info = parse_fhir_message(fhir_message)
#             if patient_info:
#                 hl7_message = create_orm_message(patient_info)
#                 print("Generated HL7 message:", hl7_message)
#                 save_hl7_message_to_file(hl7_message, patient_info.id)
#                 patient_id = patient_info.id
#                 patient_ref = db.collection("full_fhir").document(patient_id)
#                 # Check if patient already exists
#                 if patient_ref.get().exists:
#                     print(f"Patient with ID {patient_id} already exists in Firestore. Skipping.")
#                 else:
#                     # Add patient to Firestore
#                     patient_data = {
#                         "id": patient_info.id,
#                         "birth_date": patient_info.birth_date.isoformat(),
#                         "gender": patient_info.gender,
#                         "ssn": patient_info.ssn,
#                         "first_name": patient_info.first_name,
#                         "last_name": patient_info.last_name,
#                         "city": patient_info.city,
#                         "state": patient_info.state,
#                         "country": patient_info.country,
#                         "postal_code": patient_info.postal_code,
#                         "age": patient_info.age
#                     }
#                     patient_ref.set(patient_data)
#                     print(f"Added patient with ID {patient_id} to Firestore.")


# def save_hl7_message_to_file(hl7_message, patient_id):
#     hl7_file_path = work_folder_path / f"{patient_id}.hl7"
#     with open(hl7_file_path, "w") as hl7_file:
#         hl7_file.write(str(hl7_message))


# if __name__ == "__main__":
#     import poll_synthea
#     poll_synthea.call_for_patients()
#     main()