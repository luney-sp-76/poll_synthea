# Author Paul Olphert 2023

# This file contains the code to Build an HL7 message from FHIR data and
#  create a patient in Firestore
from datetime import date
import logging
import traceback
import firebase_admin
from firebase_admin import credentials, firestore
from pathlib import Path
from generators.utilities import (
    create_control_id,
    create_filler_order_num,
    create_placer_order_num,
    get_firestore_age_range,
    parse_fhir_message,
    PatientInfo,
    assign_age_to_patient
)
from hl7apy import core
from segments.create_pid import create_pid
from segments.create_obr import create_obr
from segments.create_orc import create_orc
from segments.create_msh import create_msh
from segments.create_evn import create_evn
from segments.create_pv1 import create_pv1
# from pathlib import Path

BASE_DIR = Path.cwd()
work_folder_path = BASE_DIR / "Work"
hl7_folder_path = BASE_DIR / "HL7_v2"


# Creates an HL7 MSH segment and returns the
#  HL7 message this must be called first to create the HL7 message
def create_message_header(messageType):
    global BASE_DIR
    current_date = date.today()

    # used for the control id
    control_id = create_control_id()

    # Create empty HL7 message
    try:
        # Move to 2.4 - test!!
        hl7 = core.Message(messageType, version="2.4")
    except Exception as e:
        hl7 = None
        print(f"An error occurred while initializing the HL7 Message: {e}")
        print(f"messageType: {messageType}")

    # Create MSH Segment
    hl7 = create_msh.create_msh(
        messageType, control_id, hl7, current_date)  # MSH Segment

    return hl7


# Creates an HL7 ADT message includes the MSH segment
#  then options based on message type then returns an HL7 message
def create_adt_message(patient_info, messageType):
    hl7 = create_message_header(messageType)
    hl7 = create_evn.create_evn(hl7)
    hl7 = create_pid.create_pid(patient_info, hl7)
    hl7 = create_pv1.create_pv1(patient_info, hl7)
    return hl7


# Creates an HL7 ORM message includes the MSH segment
#  then options based on message type then returns an HL7 message
def create_orm_message(patient_info, messageType):
    hl7 = create_message_header(messageType)
    hl7 = create_pid.create_pid(patient_info, hl7)
    hl7 = create_pv1.create_pv1(patient_info, hl7)
    placer_order_num = create_placer_order_num()
    filler_order_id = create_filler_order_num()
    hl7 = create_orc.create_orc(hl7, placer_order_num, filler_order_id)
    hl7 = create_obr.create_obr(
        patient_info, placer_order_num, filler_order_id, hl7)
    return hl7


# Creates an HL7 ORU message includes the MSH segment
#  then options based on message type then returns an HL7 message
def create_oru_message(patient_info, messageType):
    hl7 = create_message_header(messageType)
    hl7 = create_pid.create_pid(patient_info, hl7)
    hl7 = create_pv1.create_pv1(patient_info, hl7)
    placer_order_num = create_placer_order_num()
    filler_order_id = create_filler_order_num()
    hl7 = create_orc.create_orc(hl7, placer_order_num, filler_order_id)
    hl7 = create_obr.create_obr(
        patient_info, placer_order_num, filler_order_id, hl7)
    return hl7


def create_oml_message(patient_info, messageType):
    hl7 = create_message_header(messageType)
    hl7 = create_pid.create_pid(patient_info, hl7)
    placer_order_num = create_placer_order_num()
    filler_order_id = "24325-3^Liver^Function^Test"
    hl7 = create_orc.create_orc(hl7, placer_order_num, filler_order_id)
    hl7 = create_obr.create_obr(
        patient_info, placer_order_num, filler_order_id, hl7)
    return hl7


# HL7MessageProcessor class to process FHIR messages and create HL7 messages
class HL7MessageProcessor:
    """
    Mandatory args: hl7_folder_path: string
    Optional args: initialised firestore client: firestore.client
    """

    def __init__(self, hl7_folder_path, db=None):
        self.messageType = None
        self.hl7_folder_path = Path(hl7_folder_path)

        # Check to see if firestore client has been passed as argument
        if db:
            self.db = db
        else:
            self.db = initialize_firestore()

    def main(self, predetermined_message_type=None):
        """
        Reads and parses fhir docs in work folder,
        generates HL7 messages, and
        uploads patient info to firestore.

        Optional args: predetermined_message_type: string
        """
        # TODO: Add a menu to choose the
        # message type with validation for choices
        # Initialize Firebase Admin SDK with your credentials
        print("1. ORU_R01\n")
        print("2. ADT_A01\n")
        print("3. ORM_O01\n")

        if predetermined_message_type:
            self.messageType = predetermined_message_type
        else:
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
        except Exception as e:
            logging.error(f"Error reading FHIR JSON files: {e}")
            logging.error(traceback.format_exc())

            # At this point, parse the FHIR message and process it
            try:
                if fhir_message is None:
                    raise ValueError(
                        "FHIR message is None. Check the file path."
                    )
                patient_info = parse_fhir_message(fhir_message)
                hl7_message = None
                if self.messageType == "ADT_A01":
                    hl7_message = create_adt_message(
                        patient_info, self.messageType
                    )
                elif self.messageType == "ORM_O01":
                    hl7_message = create_orm_message(
                        patient_info, self.messageType
                    )
                elif self.messageType == "ORU_R01":
                    hl7_message = create_oru_message(
                        patient_info, self.messageType
                    )
                if hl7_message:
                    self.save_hl7_message_to_file(hl7_message, patient_info.id)
                    # Optionally, upload patient info to Firestore
                    # self.db.collection("patients").document(patient_info.id).set(patient_info.__dict__)
            except Exception as e:
                logging.error(
                    (
                        f"An error of type {type(e).__name__} occurred. "
                        f"Arguments:\n{e.args}"
                    )
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
                # hl7_file.write(str(hl7_message.obx.value) + "\r")
                hl7_file.write(str(hl7_message.orc.value) + "\r")
                hl7_file.write(str(hl7_message.obr.value) + "\r")


def initialize_firestore() -> firestore.client:
    global BASE_DIR
    """Initialize Firestore client and return it."""
    json_file = Path(BASE_DIR, "firebase",
                     "pollsynthea-firebase-adminsdk-j01m1-044b9f312b.json")
    if json_file:
        cred = credentials.Certificate(json_file)
        firebase_admin.initialize_app(cred)
        return firestore.client()
    else:
        print("No Firebase credentials found. Exiting.")
        exit(1)


def produce_ADT_A01_from_firestore(
        db: firestore.client,
        num_of_patients: int,
        lower: int,
        upper: int,
        peter_pan: bool
        ) -> bool:
    """
    Produces an ADT_A01 message
    for each patient record retrieved from firestore.
    The HL7 messages are saved in the
    'hl7_folder_path' using patientID as filename.

    """
    patients: list[PatientInfo] = get_firestore_age_range(
        db, num_of_patients, lower, upper, peter_pan)

    if patients:
        for patient in patients:

            hl7_message = create_adt_message(patient, "ADT_A01")

            # Testing purposes
            print("Generated HL7 message:", str(hl7_message))

            hl7_file_path = hl7_folder_path / f"{patient.id}.hl7"
            with open(hl7_file_path, "w") as hl7_file:
                hl7_file.write(str(hl7_message.msh.value) + "\r")
                hl7_file.write(str(hl7_message.evn.value) + "\r")
                hl7_file.write(str(hl7_message.pid.value) + "\r")
                hl7_file.write(str(hl7_message.pv1.value) + "\r")

        return True
    else:
        return False


def produce_OML_O21_from_firestore(
        db: firestore.client,
        num_of_patients: int,
        age: int,
        assign_age: bool
        ) -> bool:
    """
    Produces an OML_O21 message
    for each patient record retrieved from firestore.
    The HL7 messages are saved in the
    'hl7_folder_path' using patientID as filename.

    """

    if assign_age:
        patients: list[PatientInfo] = get_firestore_age_range(
            db=db,
            num_of_patients=num_of_patients,
            lower=1,
            upper=100,
            peter_pan=True
            )
        for i, patient in enumerate(patients):
            patient = assign_age_to_patient(
                patient_info=patient, desired_age=age, index=i)
    else:
        patients: list[PatientInfo] = get_firestore_age_range(
            db=db,
            num_of_patients=num_of_patients,
            lower=age,
            upper=age,
            peter_pan=True)

    hl7_messages = []

    for patient in patients:
        hl7 = create_oml_message(patient, "OML_O21")
        hl7_messages.append(hl7)

    return hl7_messages


if __name__ == "__main__":
    logging.basicConfig(filename="main.log", level=logging.INFO)
    import poll_synthea

    poll_synthea.call_for_patients()

    # Create an instance of HL7MessageProcessor and call its 'main' method
    hl7_folder = hl7_folder_path  # Make sure this path is correct
    logging.info(f"HL7 folder path: {hl7_folder}")
    if not hl7_folder.exists():
        hl7_folder.mkdir(parents=True, exist_ok=True)
        logging.info(f"Created HL7 folder at: {hl7_folder}")
    processor = HL7MessageProcessor(hl7_folder)
    processor.main()
