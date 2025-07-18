# Author Paul Olphert 2023

# This file contains the code to Build an HL7 message from FHIR data and
#  create a patient in Firestore
from datetime import date
import logging
import traceback
import firebase_admin
from firebase_admin import credentials, firestore
from pathlib import Path
from segments import create_msh
# from create_msh import create_msh
from segments import create_pid
from segments import create_orc
from segments import create_obr
from segments import create_evn
from segments import create_pv1
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
import requests
import urllib3
# from pathlib import Path


# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

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
            else:
                logging.error(f"Invalid message type selected: {messageType}")
                print("Invalid selection. Please choose 1, 2, or 3.")
                return

        processed_count = 0
        error_count = 0

        # Iterate through FHIR JSON files in the work folder
        for file in work_folder_path.glob("*.json"):
            try:
                print(f"Processing file: {file.name}")
                with open(file, "r") as f:
                    fhir_content = f.read()

                # Handle multiple FHIR messages in a single file
                # Check if the content contains multiple JSON objects
                fhir_messages = []

                # Try to parse as single JSON object first
                try:
                    import json
                    # If it's a single JSON object
                    json.loads(fhir_content)
                    fhir_messages.append(fhir_content)
                except json.JSONDecodeError:
                    # If single JSON parsing fails,
                    # try to split by lines/objects
                    # This handles NDJSON format (newline-delimited JSON)
                    lines = fhir_content.strip().split('\n')
                    for line in lines:
                        line = line.strip()
                        if line:
                            try:
                                json.loads(line)  # Validate JSON
                                fhir_messages.append(line)
                            except json.JSONDecodeError:
                                logging.warning(
                                        (
                                            "Skipping invalid JSON line in "
                                            "f{file.name}:"
                                            f" {line[:100]}..."
                                        )
                                )

                # Process each FHIR message
                for i, fhir_message in enumerate(fhir_messages):
                    try:
                        if not fhir_message or fhir_message.isspace():
                            logging.warning(
                                (
                                    f"Empty FHIR message in {file.name}, "
                                    f"message {i+1}"
                                )
                            )

                        # Fix: Pass the required parameters
                        # to parse_fhir_message
                        patient_info = parse_fhir_message(
                            self.db, fhir_message)

                        hl7_message = None

                        if self.messageType == "ADT_A01":
                            hl7_message = create_adt_message(
                                patient_info, self.messageType)
                        elif self.messageType == "ORM_O01":
                            hl7_message = create_orm_message(
                                patient_info, self.messageType)
                        elif self.messageType == "ORU_R01":
                            hl7_message = create_oru_message(
                                patient_info, self.messageType)

                        if hl7_message:
                            # Create unique filename for multiple messages
                            #  from same file
                            if len(fhir_messages) > 1:
                                filename_suffix = f"_{i+1}"
                            else:
                                filename_suffix = ""

                            patient_id_with_suffix = (
                                f"{patient_info.id}{filename_suffix}"
                            )
                            self.save_hl7_message_to_file(
                                hl7_message, patient_id_with_suffix)
                            processed_count += 1
                            print(
                                "Successfully created HL7 message "
                                f"for patient: {patient_id_with_suffix}")

                            # Optionally, upload patient info to Firestore
                            # self.db.collection("patients").document(patient_info.id).set(patient_info.__dict__)
                        else:
                            logging.error(
                                (
                                    "Failed to create HL7 message for patient "
                                    f"in {file.name}, message {i+1}"
                                )
                            )
                            error_count += 1

                    except Exception as e:
                        logging.error(
                            f"Error processing FHIR message {i+1} "
                            f"in {file.name}: {e}")
                        logging.error(traceback.format_exc())
                        error_count += 1

            except Exception as e:
                logging.error(f"Error reading file {file.name}: {e}")
                logging.error(traceback.format_exc())
                error_count += 1

        print("\nProcessing complete:")
        print(f"- Successfully processed: {processed_count} messages")
        print(f"- Errors encountered: {error_count} messages")

        if processed_count == 0:
            print("No HL7 messages were created. Check the logs for errors.")
            print(f"Make sure FHIR JSON files exist in: {work_folder_path}")

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


def test_mockaroo_connection():
    """Test connection to Mockaroo API for
    address data WITHOUT SSL verification"""
    try:
        # Skip SSL certificate verification entirely
        response = requests.get(
            'https://my.api.mockaroo.com/address.json?key=c5668b10',
            verify=False,  # Disable SSL verification
            timeout=30
        )
        response.raise_for_status()
        logging.info(
            "Mockaroo API test successful (no SSL verification): "
            f"{response.status_code}")
        print(f"Mockaroo API test successful: {response.status_code}")
        return response.json()
    except Exception as error:
        logging.error(f"Mockaroo API test failed: {error}")
        print(f"Mockaroo API test failed: {error}")
        return None


# In main.py - handle all patient generation logic here
def ensure_patients_exist(db, num_of_patients, lower, upper, peter_pan):
    """Ensure enough patients exist in Firestore, generating if needed"""
    patients, current_count = get_firestore_age_range(
        db,
        num_of_patients,
        lower,
        upper,
        peter_pan
    )

    if current_count < num_of_patients:
        needed = num_of_patients - current_count
        print(f"Generating {needed} additional patients...")

        info = {
            "number_of_patients": needed,
            "age_from": lower,
            "age_to": upper,
            "sex": "F"
        }

        poll_synthea.call_for_patients(info=info)

        # Process generated files and save to Firestore
        process_generated_fhir_files(db)

        # Get patients again
        patients, _ = get_firestore_age_range(
            db,
            num_of_patients,
            lower,
            upper,
            peter_pan
        )

    return patients


def process_generated_fhir_files(db):
    """Process FHIR files and save to Firestore"""
    for file in work_folder_path.glob("*.json"):
        try:
            with open(file, "r") as f:
                fhir_message = f.read()
                patient_info = parse_fhir_message(db, fhir_message)
                save_to_firestore(db=db, patient_info=patient_info)
        except Exception as e:
            logging.error(f"Error processing {file.name}: {e}")


def save_to_firestore(db, patient_info):
    """Save patient_info to Firestore using full_fhir collection"""
    try:
        # Use consistent field names and collection
        patient_data = {
            "id": patient_info.id,
            "hl7v2_id": (
                [patient_info.hl7v2_id]
                if patient_info.hl7v2_id else []
            ),  # Array format
            "birth_date":
                patient_info.birth_date.isoformat()
                if hasattr(patient_info.birth_date, 'isoformat')
                else str(patient_info.birth_date),
            "gender": patient_info.gender,
            "ssn": patient_info.ssn,
            "first_name": patient_info.first_name,
            "middle_name": patient_info.middle_name,
            "last_name": patient_info.last_name,
            "address": patient_info.address,
            "address_2": patient_info.address_2,
            "city": patient_info.city,
            "state": getattr(patient_info, 'state', 'UK'),
            "country": patient_info.country,
            "post_code": patient_info.post_code,
            "country_code": patient_info.country_code,
            "age": patient_info.age,
            "creation_date":
                patient_info.creation_date.isoformat()
                if hasattr(patient_info.creation_date, 'isoformat')
                else str(patient_info.creation_date),
        }

        # Add conditions and observations arrays if they exist
        if hasattr(patient_info, 'conditions') and patient_info.conditions:
            patient_data["conditions"] = [
                condition.__dict__ for condition in patient_info.conditions
            ]

        if hasattr(patient_info, 'observations') and patient_info.observations:
            patient_data["observations"] = [
                observation.__dict__
                for observation in patient_info.observations
            ]

        # Use full_fhir collection
        db.collection("full_fhir").document(patient_info.id).set(patient_data)
        logging.info(
            f"Saved patient {patient_info.id}"
            " to Firestore (full_fhir collection)."
        )

    except Exception as e:
        logging.error(
            f"Failed to save patient {patient_info.id} to Firestore: {e}")


if __name__ == "__main__":
    # Clear any existing log file and configure logging properly
    log_file = Path("main.log")
    if log_file.exists():
        log_file.unlink()

    logging.basicConfig(
        filename="main.log",
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        filemode='w'
    )

    try:
        import poll_synthea
        poll_synthea.call_for_patients()

        # Create an instance of HL7MessageProcessor and call its 'main' method
        hl7_folder = hl7_folder_path
        logging.info(f"HL7 folder path: {hl7_folder}")
        if not hl7_folder.exists():
            hl7_folder.mkdir(parents=True, exist_ok=True)
            logging.info(f"Created HL7 folder at: {hl7_folder}")

        processor = HL7MessageProcessor(hl7_folder)
        if processor.db is None:
            processor.db = initialize_firestore()
        processor.main()

        # Test Mockaroo connection without SSL verification
        print("Testing Mockaroo API connection (no SSL verification)...")
        logging.info(
            "Testing Mockaroo API connection (no SSL verification)...")
        mockaroo_data = test_mockaroo_connection()
        if mockaroo_data:
            logging.info("Mockaroo API is accessible")
            print("Mockaroo API is accessible")
        else:
            logging.warning(
                "Mockaroo API is not accessible - continuing without it")
            print("Mockaroo API is not accessible - continuing without it")
    except Exception as main_error:
        logging.error(f"Main execution failed: {main_error}")
        logging.error(traceback.format_exc())
        print(f"Application failed: {main_error}")
