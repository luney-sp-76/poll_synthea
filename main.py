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

BASE_DIR = Path.cwd()
work_folder_path = BASE_DIR / "Work"
hl7_folder_path = BASE_DIR / "HL7_v2"

# Creates a random control ID for the HL7 message
def create_control_id():
    current_date_time = datetime.datetime.now()
    formatted_date_minutes_milliseconds = current_date_time.strftime("%Y%m%d%H%M%S.%f")
    control_id = formatted_date_minutes_milliseconds.replace(".", "")
    return control_id

# Creates a random visit number for the HL7 message
def create_visit_number():
    visit_number = ''.join(["{}".format(random.randint(0, 9)) for _ in range(0, 3)])
    return visit_number

# Creates a random visit institution for the HL7 message
def create_visit_instiution():
    visit_institution = ''.join(["{}".format(random.randint(0, 9)) for _ in range(0, 3)]) \
                           + ''.join(["{}".format(random.choice(string.ascii_uppercase)) for _ in range(0, 2)])
    return visit_institution 


# dummy placer order ID up to 75 characters
def create_placer_order_num():
    order_id = ''.join(["{}".format(random.randint(0, 9)) for _ in range(0, 3)]) \
                           + ''.join(["{}".format(random.choice(string.ascii_uppercase)) for _ in range(0, 2)])
    return order_id

 # dummy filler order ID  with the format "1^^23^4"
def create_filler_order_num():
    # allocate a random number between 1 and 999999999
    random_number = random.randint(1, 999999999)
    # format the random number as 1^^23^4
    filler_order_id = f"1^^{random_number //10000}^{random_number % 10000}"

    return filler_order_id

# generate a random time for the OBR segment
def create_obr_time():
    random_days_ago = random.randint(1, 7)
    random_date = date.today() - datetime.timedelta(days=random_days_ago)

    return random_date.strftime("%Y%m%d%H%M")


# Creates a HL7 message
def create_message(patient_info, messageType):
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

        

#TODO: Make a call to each of the functions below to create the segments depending on the message type 
#TODO: Create a seperate function for MSH SEGMENT

     # Initialize msh to None
    msh = None

    # Add MSH Segment
    try:
        #convert the message type to a string replacing the underscore with ^ 
        messageTypeSegment = str(messageType)
        messageTypeSegment = messageTypeSegment.replace("_", "^")

        hl7.msh.msh_3 = "ULTRA"  # Sending Application
        hl7.msh.msh_4 = "MATER"  # Sending Facility
        hl7.msh.msh_5 = "PAMS"  # Receiving Application
        hl7.msh.msh_6 = "PAMS"  # Receiving Facility
        hl7.msh.msh_7 = current_date.strftime("%Y%m%d%H%M")  # Date/Time of Message
        hl7.msh.msh_9 = messageTypeSegment  # Message Type
        hl7.msh.msh_10 = control_id  # Message Control ID
        hl7.msh.msh_11 = "T"  # Processing ID
        hl7.msh.msh_12 = "2.5"  # Version ID
        hl7.msh.msh_15 = "AL"  # Accept Acknowledgment Type
        hl7.msh.msh_16 = "NE"  # Application Acknowledgment Type
    except Exception as ae:
        print("An AssertionError occurred:", ae)
        print(f"Could not create MSH Segment: {ae}")
        logging.error(f"An error of type {type(ae).__name__} occurred. Arguments:\n{ae.args}")
        logging.error(traceback.format_exc())

#TODO: Create a function to generate a random number for the message control ID
#TODO: Create a function to generate a random number for the visit institution
#TODO: Create a seperate function for PID SEGMENT

   # Add PID Segment
    try:
       hl7.pid.pid_1 = "1"
       # PID 3 defaults to P
       #hl7.pid.pid_3 = patient_info.id
       hl7.pid.pid_5 = f"{patient_info.last_name}^{patient_info.first_name}^{patient_info.middle_name}"
       hl7.pid.pid_7 = patient_info.birth_date.strftime("%Y%m%d")
       hl7.pid.pid_8 = patient_info.gender[0].upper()
       hl7.pid.pid_11 = f"^^^{patient_info.city}^{patient_info.state}^{patient_info.postal_code}^{patient_info.country}"
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
        
#TODO: Create a seperate function for ORC SEGMENT
    # Add ORC Segment for the Order (dummy data for example)
    try:
        placer_order_num = create_placer_order_num()
        filler_order_id = create_filler_order_num()
        hl7.orc.orc_1 = "O"  # New Order
        hl7.orc.orc_2 = placer_order_num  # Some dummy placer order ID up to 75 characters
        hl7.orc.orc_3 = filler_order_id  # Some dummy filler order ID up to 75 characters
    except Exception as ae:
        print("An AssertionError occurred:", ae)
        print(f"Could not create MSH Segment: {ae}")
        logging.error(f"An error of type {type(ae).__name__} occurred. Arguments:\n{ae.args}")
        logging.error(traceback.format_exc())
   
  
    
    # Add OBR Segment for the Order details (dummy data for example)
    try:
        request_date = create_obr_time()
        observation_date = create_obr_time()
        quantity_timing = create_obr_time()

        hl7.obr.obr_1 = "1"  # Set ID
        hl7.obr.obr_2 = placer_order_num  # Some dummy placer order ID up to 75 characters
        hl7.obr.obr_3 = filler_order_id  # Some dummy filler order ID up to 75 characters
        hl7.obr.obr_4 = patient_info.id  # some test code hl7.Orders.ext_code 
        #Requested Date/Time lab.Request.date_refer 
        hl7.obr.obr_6 = request_date
        # Observation Date/Time lab.Resultp_extra.doc_date
        hl7.obr.obr_7 = observation_date  
        #Ordering Provider 2 component 1 component 1 lab.Resultp_extra.doc_ordering  Default 'WACON'2 component 1 lab.Request.doctor Default 'TEST'
        hl7.obr.obr_16 = "WACON^TEST"
        #Diagnostic Service ID  2 components 1^2 component 2 lab.Request.lab 
        hl7.obr.obr_24 = "BI^UHC"
        #Quantity/Timing 6 component s 6 components1 component 4 lab.Request.date_service,lab.Request.time_service2 component 4 lab.Request.date_coln,lab.Request.time_coln 3 component 6 lab.Request.priority_coln 
        hl7.obr.obr_27 = f"^^^{quantity_timing}^^E"


    except:
        print("An AssertionError occurred:", ae)
        print(f"Could not create MSH Segment: {ae}")
        logging.error(f"An error of type {type(ae).__name__} occurred. Arguments:\n{ae.args}")
        logging.error(traceback.format_exc())
    
    return hl7


class PatientInfo:
    def __init__(self, id, birth_date, gender, ssn, first_name, middle_name, last_name, city, state, country, postal_code, age):
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
                middle_name=resource.name[0].given[1],
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
                    hl7_message = create_message(patient_info, messageType)
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
                        
                else:
                    print("no patient info")
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