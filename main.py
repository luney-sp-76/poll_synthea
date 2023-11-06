import logging
import string
import traceback
import firebase_admin
from firebase_admin import credentials, firestore
from fhir.resources.R4B.bundle import Bundle
from fhir.resources.R4B.patient import Patient
from datetime import date
import datetime
from pathlib import Path
from hl7apy import core
import random
import segments.create_pid as create_pid
import segments.create_obr as create_obr
import segments.create_orc as create_orc
import segments.create_msh as create_msh
import segments.create_evn as create_evn
import segments.create_pv1 as create_pv1
from generators.utilities import create_placer_order_num, create_filler_order_num, create_control_id

BASE_DIR = Path.cwd()
work_folder_path = BASE_DIR / "Work"
hl7_folder_path = BASE_DIR / "HL7_v2"


def create_adt_message(patient_info, messageType):
    hl7 = create_message_header(patient_info, messageType)
    hl7 = create_evn.create_evn(hl7)
    hl7 = create_pid.create_pid(patient_info, hl7)
    hl7 = create_pv1.create_pv1(patient_info, hl7)
    #TODO: Add Pv1 segment
 
    return hl7

def create_orm_message(patient_info, messageType):
    hl7 = create_message_header(patient_info, messageType)
    hl7 = create_pid.create_pid(patient_info, hl7)
    hl7 = create_pv1.create_pv1(patient_info, hl7)
    placer_order_num = create_placer_order_num()
    filler_order_id = create_filler_order_num()
    hl7 = create_orc.create_orc(hl7, placer_order_num, filler_order_id)
    hl7 = create_obr.create_obr(patient_info, placer_order_num, filler_order_id, hl7)

def create_oru_message(patient_info, messageType):
    hl7 = create_message_header(patient_info, messageType)
    hl7 = create_pid.create_pid(patient_info, hl7)
    placer_order_num = create_placer_order_num()
    filler_order_id = create_filler_order_num()
    hl7 = create_orc.create_orc(hl7, placer_order_num, filler_order_id)
    hl7 = create_obr.create_obr(patient_info, placer_order_num, filler_order_id, hl7)

    return hl7


# Creates a HL7 message includes the MSH segment then options based on message type
def create_message_header(patient_info, messageType):
    global BASE_DIR
    current_date = date.today()

    # used for the control id
    control_id = create_control_id()

    # Create empty HL7 message
    try:
        hl7 = core.Message(messageType, version="2.5")
    except Exception as e:
        hl7 = None
        print(f"An error occurred while initializing the HL7 Message: {e}")
        print(f"messageType: {messageType}")

    # Create MSH Segment
    hl7 = create_msh.create_msh(messageType, control_id, hl7, current_date) # MSH Segment

    return hl7


# update the dobs after the sample patients are created - 
# if a request is for 365 patients between the age 10 and 11 then each patient 
# could be given a Day of birth that is incremented one day older than the previous for the whole year
class PatientInfo:
    def __init__(
        self,
        id,
        birth_date,
        gender,
        ssn,
        first_name,
        middle_name,
        last_name,
        city,
        state,
        country,
        postal_code,
        age,
    ):
        self.id = id
        self.birth_date = birth_date
        self.gender = gender
        self.ssn = ssn
        self.first_name = first_name
        self.middle_name = middle_name
        self.last_name = last_name
        self.city = city
        self.state = state
        self.country = country
        self.postal_code = postal_code
        self.age = age


def calculate_age(birth_date):
    today = date.today()
    age = (
        today.year
        - birth_date.year
        - ((today.month, today.day) < (birth_date.month, birth_date.day))
    )
    return age


def parse_fhir_message(fhir_message):
    # Parse the FHIR JSON message into a Bundle
    bundle = Bundle.parse_raw(fhir_message)

    # Extract information from the Bundle
    patient_info = None

    # Extract information from the Bundle
    print("Bundle Type:", bundle.type)
    print("Entry Count:", len(bundle.entry))
    count = 0
    for entry in bundle.entry:
        resource = entry.resource
        if isinstance(resource, Patient):
            count += 1
            #set the birth date to be the first day of the year using the year of the first patient
            if count == 1:
                birth_date = resource.birthDate
                birth_date = birth_date.replace(month=1, day=1)
                age = calculate_age(birth_date)
            else:
                #use the previous patients birthdate to increment the next patients birthdate by one day
                birth_date = birth_date + datetime.timedelta(days=1)
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
                middle_name=resource.name[0].given[1],
                last_name=resource.name[0].family,
                city=resource.address[0].city,
                state=resource.address[0].state,
                country=resource.address[0].country,
                postal_code=resource.address[0].postalCode,
                age=age,
            )
            break  # Assuming there's only one patient resource per FHIR message
    return patient_info


def initialize_firestore() -> firestore.client:
    global BASE_DIR
    """Initialize Firestore client and return it."""
    json_file = Path("firebase/pollsynthea-firebase-adminsdk-j01m1-f9a1592562.json")
    if json_file:
        cred = credentials.Certificate(json_file)
        firebase_admin.initialize_app(cred)
        return firestore.client()
    else:
        print("No Firebase credentials found. Exiting.")
        exit(1)


def save_to_firestore(db: firestore.client, patient_info: PatientInfo) -> None:
    """Save patient info to Firestore."""
    patient_id = patient_info.id
    patient_ref = db.collection("full_fhir").document(patient_id)
    if patient_ref.get().exists:
        logging.info(
            f"Patient with ID {patient_id} already exists in Firestore. Skipping."
        )
    else:
        patient_data = {
            "id": patient_info.id,
            "birth_date": patient_info.birth_date.isoformat(),
            # ... (rest of the fields)
        }
        patient_ref.set(patient_data)
        logging.info(f"Added patient with ID {patient_id} to Firestore.")


def main():
    #TODO: Add a menu to choose the message type with validation for choices
    # Initialize Firebase Admin SDK with your credentials
    db = initialize_firestore()
    print("1. ORU_R01\n")
    print("2. ADT_A03\n")
    print("3. ORM_O01\n")
    messageType = input("Choose a message type: ")
    if messageType == "1":
        messageType = "ORU_R01"
    elif messageType == "2":
        messageType = "ADT_A03"
    elif messageType == "3":
        messageType = "ORM_O01"
    try:
        # Iterate through FHIR JSON files in the work folder
        for file in work_folder_path.glob("*.json"):
            with open(file, "r") as f:
                fhir_message = f.read()
                patient_info = parse_fhir_message(fhir_message)
                if patient_info:
                    if messageType == "ADT_A03":
                        hl7_message = create_adt_message(patient_info, messageType)
                    elif messageType == "ORM_O01":
                        hl7_message = create_orm_message(patient_info, messageType)
                    elif messageType == "ORU_R01":
                        hl7_message = create_oru_message(patient_info, messageType)
                    print("Generated HL7 message:", str(hl7_message))
                    save_hl7_message_to_file(hl7_message, patient_info.id)
                    patient_id = patient_info.id
                    patient_ref = db.collection("full_fhir").document(patient_id)
                    # Check if patient already exists
                    if patient_ref.get().exists:
                        print(
                            f"Patient with ID {patient_id} already exists in Firestore. Skipping."
                        )
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
                            "age": patient_info.age,
                        }
                        patient_ref.set(patient_data)
                        print(f"Added patient with ID {patient_id} to Firestore.")

                else:
                    print("no patient info")
    except Exception as e:
        logging.error(
            f"An error of type {type(e).__name__} occurred. Arguments:\n{e.args}"
        )
        logging.error(traceback.format_exc())


def save_hl7_message_to_file(hl7_message, patient_id):
    hl7_file_path = hl7_folder_path / f"{patient_id}.hl7"
    with open(hl7_file_path, "w") as hl7_file:
        hl7_file.write(str(hl7_message.msh.value) + "\r")
        hl7_file.write(str(hl7_message.evn.value) + "\r")
        hl7_file.write(str(hl7_message.pid.value) + "\r")
        hl7_file.write(str(hl7_message.pv1.value) + "\r")
        hl7_file.write(str(hl7_message.orc.value) + "\r")
        hl7_file.write(str(hl7_message.obr.value) + "\r")


if __name__ == "__main__":
    logging.basicConfig(filename="main.log", level=logging.INFO)
    import poll_synthea

    poll_synthea.call_for_patients()
    main()
