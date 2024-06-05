import logging
import traceback
import firebase_admin
from firebase_admin import credentials, firestore
from datetime import date
from pathlib import Path
from hl7apy import core
import segments.create_pid as create_pid
import segments.create_obr as create_obr
import segments.create_orc as create_orc
import segments.create_msh as create_msh
import segments.create_evn as create_evn
import segments.create_pv1 as create_pv1
from generators.utilities import create_placer_order_num, create_filler_order_num, create_control_id, parse_fhir_message, PatientInfo

BASE_DIR = Path.cwd()
work_folder_path = BASE_DIR / "Work"
hl7_folder_path = BASE_DIR / "HL7_v2"

# Creates an HL7 MSH segment and returns the HL7 message this must be called first to create the HL7 message
def create_message_header(patient_info, messageType):
    global BASE_DIR
    current_date = date.today()

    # used for the control id
    control_id = create_control_id()

    # Create empty HL7 message
    try:
        hl7 = core.Message(messageType, version="2.3")
    except Exception as e:
        hl7 = None
        print(f"An error occurred while initializing the HL7 Message: {e}")
        print(f"messageType: {messageType}")

    # Create MSH Segment
    hl7 = create_msh.create_msh(messageType, control_id, hl7, current_date) # MSH Segment

    return hl7

# Creates an HL7 ADT message includes the MSH segment then options based on message type then returns an HL7 message
def create_adt_message(patient_info, messageType):
    hl7 = create_message_header(patient_info, messageType)
    hl7 = create_evn.create_evn(hl7)
    hl7 = create_pid.create_pid(patient_info, hl7)
    hl7 = create_pv1.create_pv1(patient_info, hl7)
  
    return hl7

# Creates an HL7 ORM message includes the MSH segment then options based on message type then returns an HL7 message
def create_orm_message(patient_info, messageType):
    hl7 = create_message_header(patient_info, messageType)
    hl7 = create_pid.create_pid(patient_info, hl7)
    hl7 = create_pv1.create_pv1(patient_info, hl7)
    placer_order_num = create_placer_order_num()
    filler_order_id = create_filler_order_num()
    hl7 = create_orc.create_orc(hl7, placer_order_num, filler_order_id)
    hl7 = create_obr.create_obr(patient_info, placer_order_num, filler_order_id, hl7)

    return hl7

# Creates an HL7 ORU message includes the MSH segment then options based on message type then returns an HL7 message
def create_oru_message(patient_info, messageType):
    hl7 = create_message_header(patient_info, messageType)
    hl7 = create_pid.create_pid(patient_info, hl7)
    hl7 = create_pv1.create_pv1(patient_info, hl7)
    placer_order_num = create_placer_order_num()
    filler_order_id = create_filler_order_num()
    hl7 = create_orc.create_orc(hl7, placer_order_num, filler_order_id)
    hl7 = create_obr.create_obr(patient_info, placer_order_num, filler_order_id, hl7)

    return hl7


# HL7MessageProcessor class to process FHIR messages and create HL7 messages  
class HL7MessageProcessor:
    def __init__(self, hl7_folder_path):
        self.db = initialize_firestore()
        self.messageType = None
        self.hl7_folder_path = Path(hl7_folder_path)
    

    def main(self):
        #TODO: Add a menu to choose the message type with validation for choices
        # Initialize Firebase Admin SDK with your credentials
        print("1. ORU_R01\n")
        print("2. ADT_A01\n")
        print("3. ORM_O01\n")
        messageType = input("Choose a message type: ")
        if messageType == "1":
            self.messageType = "ORU_R01"
        elif messageType == "2":
            self.messageType = "ADT_A01"
        elif messageType == "3":
            self.messageType = "ORM_O01"
        try:
            # Iterate through FHIR JSON files in the work folder
            for file in work_folder_path.glob("*.json"):
                with open(file, "r") as f:
                    fhir_message = f.read()
                    patient_info = parse_fhir_message(fhir_message)
                    if patient_info:
                        if self.messageType == "ADT_A01":
                            hl7_message = create_adt_message(patient_info, self.messageType)
                        elif self.messageType == "ORM_O01":
                            hl7_message = create_orm_message(patient_info, self.messageType)
                        elif self.messageType == "ORU_R01":
                            hl7_message = create_oru_message(patient_info, self.messageType)
                        print("Generated HL7 message:", str(hl7_message))
                        self.save_hl7_message_to_file(hl7_message, patient_info.id)
                        patient_id = patient_info.id
                        patient_ref = self.db.collection("full_fhir").document(patient_id)
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


    def save_hl7_message_to_file(self, hl7_message, patient_id):
        hl7_file_path = self.hl7_folder_path / f"{patient_id}.hl7"
        with open(hl7_file_path, "w") as hl7_file:
            hl7_file.write(str(hl7_message.msh.value) + "\r")
            if self.messageType in ["ADT_A01"]:
                hl7_file.write(str(hl7_message.evn.value) + "\r")
            hl7_file.write(str(hl7_message.pid.value) + "\r")
            hl7_file.write(str(hl7_message.pv1.value) + "\r")
            if self.messageType in ["ORU_R01", "ORM_O01"]:
                #hl7_file.write(str(hl7_message.obx.value) + "\r")
                hl7_file.write(str(hl7_message.orc.value) + "\r")
                hl7_file.write(str(hl7_message.obr.value) + "\r")


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
                "city": patient_info.city,
                "country": patient_info.country,
                "first_name": patient_info.first_name,
                "gender": patient_info.gender,
                "last_name": patient_info.last_name,
                "postal_code":patient_info.postal_code,
                "ssn":patient_info.ssn,
                "state":patient_info.state
            }
            patient_ref.set(patient_data)
            logging.info(f"Added patient with ID {patient_id} to Firestore.")
   

if __name__ == "__main__":
    logging.basicConfig(filename="main.log", level=logging.INFO)
    import poll_synthea

    poll_synthea.call_for_patients()
    # Create an instance of HL7MessageProcessor and call its 'main' method
    hl7_folder = hl7_folder_path  # Make sure this path is correct
    processor = HL7MessageProcessor(hl7_folder)
    processor.main()
