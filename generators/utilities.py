# utilities.py
import logging
from pathlib import Path
import random, string, datetime
from datetime import date, datetime
import datetime
import time
from fhir.resources.R4B.bundle import Bundle
from fhir.resources.R4B.patient import Patient
from fhir.resources.R4B.condition import Condition
from fhir.resources.R4B.observation import Observation
from firebase_admin import firestore
from google.cloud.firestore_v1 import document
from google.cloud.firestore_v1.base_query import FieldFilter, Or
from google.cloud.firestore_v1 import aggregation
from ..poll_synthea import call_for_patients
from hl7apy.parser import parse_message
from hl7apy.consts import VALIDATION_LEVEL
import requests

BASE_DIR = Path.cwd()
work_folder_path = BASE_DIR / "Work"
hl7_folder_path = BASE_DIR / "HL7_v2"



# generate a random time for the OBR segment
def create_obr_time():
    random_days_ago = random.randint(1, 7)
    random_date = date.today() - datetime.timedelta(days=random_days_ago)

    return random_date.strftime("%Y%m%d%H%M")


# generate a random placer order number for the HL7 message
def create_placer_order_num():
    order_id = "".join(
        ["{}".format(random.randint(0, 9)) for _ in range(0, 3)]
    ) + "".join(
        ["{}".format(random.choice(string.ascii_uppercase)) for _ in range(0, 2)]
    )
    return order_id


# generate a random filler order number for the HL7 message
def create_filler_order_num():
    # allocate a random number between 1 and 999999999
    random_number = random.randint(1, 999999999)
    # format the random number as 1^^23^4
    filler_order_id = f"1^^{random_number // 10000}^{random_number % 10000}"
    return filler_order_id


# Creates a random visit institution for the HL7 message
def create_visit_instiution():
    visit_institution = "".join(
        ["{}".format(random.randint(0, 9)) for _ in range(0, 3)]
    ) + "".join(
        ["{}".format(random.choice(string.ascii_uppercase)) for _ in range(0, 2)]
    )
    return visit_institution


# Creates a random visit number for the HL7 message
def create_visit_number():
    visit_number = "".join(["{}".format(random.randint(0, 9)) for _ in range(0, 3)])
    return visit_number


# Creates a random control ID for the HL7 message
def create_control_id():
    current_date_time = datetime.datetime.now()
    formatted_date_minutes_milliseconds = current_date_time.strftime("%Y%m%d%H%M%S.%f")
    control_id = formatted_date_minutes_milliseconds.replace(".", "")
    return control_id


def increment_id(id:str) -> str:
    """Increments a patient id 

    Args: 
    - id: ``str``, the patient id to increment

    Returns: 
    - ``str``, the incremented patient id 
    """

    # Remove dashes from the string
    cleaned_hex_string = id.replace('-', '')

    # Convert the cleaned hex string to an integer
    hex_int = int(cleaned_hex_string, 16)

    # Increment the integer by 1
    incremented_hex_int = hex_int + 1

    # Convert the incremented integer back to a hex string
    incremented_hex_string = hex(incremented_hex_int)[2:]  # Remove the '0x' prefix

    # Ensure the incremented hex string maintains the original length
    incremented_hex_string = incremented_hex_string.zfill(len(cleaned_hex_string))

    # Reinsert the dashes to match the original format
    formatted_incremented_hex_string = '-'.join([
        incremented_hex_string[:8],
        incremented_hex_string[8:12],
        incremented_hex_string[12:16],
        incremented_hex_string[16:20],
        incremented_hex_string[20:]
    ])

    return formatted_incremented_hex_string


# Increments patient hl7v2_id by one  
def increment_hl7v2_id(s):
    def increment_char(c):
        if 'A' <= c < 'Z':
            return chr(ord(c) + 1)
        elif c == 'Z':
            return '0'
        elif '0' <= c < '9':
            return chr(ord(c) + 1)
        elif c == '9':
            return 'A'
        else:
            return c
    
    s = list(s)
    i = len(s) - 1

    while i >= 0:
        s[i] = increment_char(s[i])
        if (s[i] >= 'A' and s[i] <= 'Z') or (s[i] >= '0' and s[i] <= '9' and s[i] != '0'):
            break
        i -= 1
    
    return ''.join(s)


# Creates a random patient ID for the patient 
def create_patient_hl7v2_id(db: firestore.client):
    """Generates an hl7v2_id for a new patient, given the highest id currently 
    in the database. 

    Args: 
    - db: ``firestore.client``, the client for interfacing with the firestore database

    Returns: 
    - patient_id: ``String``, the fully-formed patient hl7v2_id. 
    
    To do: 
    - Catch edge cases such as no ID being returned by query
    """
    synthea_code = "SYN"

    # Pull largest id from firebase
    db_ref = db.collection("full_fhir")
    query = (
        db_ref.order_by("hl7v2_id", direction=firestore.Query.DESCENDING).limit(1)
    )

    results = query.stream()
    for result in results:
        greatest_id = result._data["hl7v2_id"]
        break

    greatest_id = greatest_id[3 : 9]

    # Increment previous patient id to new value
    new_id = increment_hl7v2_id(greatest_id)

    # Generate new hl7v2_id in full
    patient_id = f"{synthea_code + new_id}^^^PAS^MR"

    # Return new hl7v2_id 
    return patient_id


# PatientInfo class to store patient information from a Bundled FHIR message
class PatientInfo:
    """A class which holds all patient information. 

    Attributes: 
    - id
    - hl7v2_id: ``list[str]``
    - birth_date
    - gender
    - ssn
    - first_name: ``str``
    - middle_name: ``str``
    - last_name: ``str``
    - address: ``str``
    - address_2: ``str``
    - city: ``str``
    - country: ``str``
    - post_code: ``str``
    - country_code: ``str``
    - age
    - creation_date
    - conditions: ``list[PatientCondition]``
    - observations: ``list[PatientObservation]``
    """
    def __init__(
        self,
        id,
        birth_date,
        gender,
        ssn,
        first_name,
        middle_name,
        last_name,
        address,
        address_2,
        city, 
        country,
        post_code,
        country_code,
        age,
        creation_date,
        hl7v2_id = None,
    ):
        self.id = id

        # Create id array and assign first id 
        if hl7v2_id: 
            self.hl7v2_id = hl7v2_id
        else:
            self.hl7v2_id: list[str] = []

        self.birth_date = birth_date
        self.gender = gender
        self.ssn = ssn
        self.first_name = first_name
        self.middle_name = middle_name
        self.last_name = last_name
        self.address = address
        self.address_2 = address_2
        self.city = city
        self.country = country
        self.post_code = post_code
        self.country_code = country_code
        self.age = age
        self.creation_date = creation_date
        self.conditions: list[PatientCondition] = []
        self.observations: list[PatientObservation] = []


    def __repr__(self):  
        return ("PatientInfo id:% s hl7v2_id:% s birth_date:% s gender:% s ssn:% s first_name:% s middle_name:% s last_name:% s "
                "address:% s address_2:% s city:% s country:% s post_code:% s country_code:% s age:% s creation_date:% s"
                "conditions:% s observations:% s") % \
                (self.id, self.hl7v2_id, self.birth_date, self.gender, self.ssn, self.first_name, self.middle_name, self.last_name, \
                 self.address, self.address_2, self.city, self.country, self.post_code, self.country_code, self.age, self.creation_date, 
                 self.conditions, self.observations)
    

    def __str__(self):
        return ("From str method of PatientInfo: id is % s, hl7v2_id is % s, birth_date is % s, gender is % s, ssn is % s, "
                "first_name is % s, middle_name is % s, last_name is % s, address is % s, address_2 is % s, city is % s, "
                "country is % s, post_code is % s, country_code is % s, age is % s, creation_date is % s, conditions is % s, observations is % s") % \
                (self.id, self.hl7v2_id, self.birth_date, self.gender, self.ssn, self.first_name, self.middle_name, self.last_name, \
                 self.address, self.address_2, self.city, self.country, self.post_code, self.country_code, self.age, self.creation_date, 
                 self.conditions, self.observations)


class PatientCondition: 
    """A class which holds information about a patient condition. 

    Attributes: 
    - condition: ``String``
    - clinical_status: ``String``
    - verification_status: ``String``
    - onset_date_time: ``Date``
    - recorded_date: ``Date``
    - abatement_time: ``Date | None``
    - encounter_reference: ``String``
    - subject_reference: ``String``
    - snomed_code: ``String``
    """
    def __init__(
        self,
        condition, 
        clinical_status, 
        verification_status, 
        onset_date_time, 
        recorded_date,
        abatement_time,
        encounter_reference,
        subject_reference, 
        snomed_code
    ):
        self.condition = condition 
        self.clinical_status = clinical_status
        self.verification_status = verification_status
        self.onset_date_time = onset_date_time
        self.recorded_date = recorded_date
        self.abatement_time = abatement_time
        self.encounter_reference = encounter_reference
        self.subject_reference = subject_reference
        self.snomed_code = snomed_code

    def __repr__(self):  
        return ("PatientCondition condition:% s clinical_status:% s verification_status:% s onset_date_time:% s "
                "recorded_date:% s abatement_time:% s encounter_reference:% s subject_reference:% s snomed_code:% s") % \
                (self.condition, self.clinical_status, self.verification_status, self.onset_date_time, self.recorded_date, \
                 self.abatement_time, self.encounter_reference, self.subject_reference, self.snomed_code)
    

    def __str__(self):
        return ("From str method of PatientCondition: condition is % s, clinical_status is % s, verification_status is % s, "
                "onset_date_time is % s, recorded_date is % s, abatement_time is % s, encounter_reference is % s, "
                "subject_reference is % s, snomed_code is % s") % \
                (self.condition, self.clinical_status, self.verification_status, self.onset_date_time, self.recorded_date, \
                 self.abatement_time, self.encounter_reference, self.subject_reference, self.snomed_code)


class PatientObservation: 
    """A class which holds information regarding a patient observation. 

    Attributes: 
    - category: ``String``
    - observation: ``String``
    - placer_order_number
    - filler_order_number
    - status: ``String``
    - effective_date_time: ``Date``
    - issued: ``Date``
    - value_quantity: ``String | None``
    - value_codeable_concept: ``String | None``
    - encounter_reference: ``String``
    - subject_reference: ``String``
    - component: ``list[dict] | None`` with two keys: "code_text", and "result"
    
    """
    def __init__(
        self,
        category, 
        observation,
        placer_order_number,
        filler_order_number,
        status,  
        effective_date_time, 
        issued,
        value_quantity,
        value_codeable_concept,
        encounter_reference,
        subject_reference,
        component,
    ):
        self.category = category
        self.observation = observation
        self.placer_order_number = placer_order_number
        self.filler_order_number = filler_order_number
        self.status = status
        self.effective_date_time = effective_date_time
        self.issued = issued
        self.value_quantity = value_quantity
        self.value_codeable_concept = value_codeable_concept
        self.encounter_reference = encounter_reference
        self.subject_reference = subject_reference
        self.component = component

    def __repr__(self):  
        return ("PatientObservation category:% s observation:% s placer_order_number:% s filler_order_number:% s status:% s "
                "effective_date_time:% s issued:% s value_quantity:% s value_codeable_concept:% s encounter_reference:% s "
                "subject_reference:% s component:% s") % \
                (self.category, self.observation, self.placer_order_number, self.filler_order_number, self.status, 
                 self.effective_date_time, self.issued, self.value_quantity, self.value_codeable_concept, 
                 self.encounter_reference, self.subject_reference, self.component)
    

    def __str__(self):
        return ("From str method of PatientObservation: category is % s, observation is % s, placer_order_number is % s, "
                "filler_number_order is % s, status is % s, "
                "effective_date_time is % s, issued is % s, value_quantity is % s, value_codeable_concept is % s, "
                "encounter_reference is % s, subject_reference is % s, component is % s") % \
                (self.category, self.observation, self.placer_order_number, self.filler_order_number, self.status, 
                 self.effective_date_time, self.issued,
                 self.value_quantity, self.value_codeable_concept, self.encounter_reference, self.subject_reference, 
                 self.component)


# Calculate the age of the patient
def calculate_age(birth_date):
    today = date.today()
    age = (
        today.year
        - birth_date.year
        - ((today.month, today.day) < (birth_date.month, birth_date.day))
    )
    return age


# Get random address from mockeroo API 
def request_random_address():
    """Requests a random address from a mockeroo API.

    Will require error checks to ensure address is reachable and the API responds as expected. 
    """
    response = requests.get("https://my.api.mockaroo.com/address.json?key=d995a340")

    return response.json()


# TODO update the dobs after the sample patients are created - 
# if a request is for 365 patients between the age 10 and 11 then each patient 
# could be given a Day of birth that is incremented one day older than the previous for the whole year


# Parses a FHIR JSON message and returns a PatientInfo object
def parse_fhir_message(db: firestore.client, fhir_message, require_address=True):
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

            # Handle missing middle name
            if len(resource.name[0].given) > 1:
                middle_name = resource.name[0].given[1]
            else:
                middle_name = None

            # Create patient id array
            hl7v2_id = []
            hl7v2_id.append(create_patient_hl7v2_id(db=db))


            # If true, reading from synthetic Fhir json generated using Synthea
            if require_address:
                address_json = request_random_address()
                address = address_json["address"]
                address_2 = address_json["address_2"]
                city = address_json["city"]
                country = address_json["country"]
                post_code = address_json["post_code"]
                country_code = address_json["country_code"]
            else: 
                # Replace with appropriate location of info within UK patient in Fhir 
                # For now, remains the same 
                address_json = request_random_address()
                address = address_json["address"]
                address_2 = address_json["address_2"]
                city = address_json["city"]
                country = address_json["country"]
                post_code = address_json["post_code"]
                country_code = address_json["country_code"]

            patient_info = PatientInfo(
                id=resource.id,
                birth_date=birth_date,
                gender=resource.gender,
                ssn=ssn,
                first_name=resource.name[0].given[0],
                middle_name=middle_name,
                last_name=resource.name[0].family,
                address = address,
                address_2 = address_2,
                city = city,
                country = country,
                post_code = post_code,
                country_code = country_code,
                age=age,
                creation_date=date.today(),
                hl7v2_id=hl7v2_id
            )
            # break  # Assuming there's only one patient resource per FHIR message

        if (isinstance(resource, Condition) and patient_info):
            patient_info = parse_fhir_conditions(resource, patient_info)

        if (isinstance(resource, Observation) and patient_info):
            patient_info = parse_fhir_observations(resource, patient_info)

    return patient_info


def parse_fhir_conditions(resource: Condition, patient_info: PatientInfo) -> PatientInfo:
    """Attempts to extract patient conditions from a fhir bundle. 

    Args: 
    - resource: ``Condition``, parsed from raw fhir message
    - patient_info: ``PatientInfo``, containing the parsed patient information thus far

    Returns: 
    - patient_info: ``PatientInfo``, with conditions as a new attribute (conditions) 
    """

    # May not be present in condition record
    abatement_date_time = None

    condition = resource.code.text
    clinical_status = resource.clinicalStatus.coding[0].code
    snomed_code = resource.code.coding[0].code
    verification_status = resource.verificationStatus.coding[0].code
    onset_date_time = resource.onsetDateTime
    recorded_date = resource.recordedDate
    encounter_reference = str(resource.encounter.reference)
    subject_reference = str(resource.subject.reference)

    if resource.clinicalStatus.coding[0].code == "resolved":
        abatement_date_time = resource.abatementDateTime

    patient_condition = PatientCondition(condition=condition, clinical_status=clinical_status,
                                            verification_status=verification_status, onset_date_time=onset_date_time, 
                                            recorded_date=recorded_date, abatement_time=abatement_date_time, 
                                            encounter_reference=encounter_reference, subject_reference=subject_reference, 
                                            snomed_code=snomed_code)
    patient_info.conditions.append(patient_condition)
    return patient_info


def parse_fhir_observations(resource: Observation, patient_info: PatientInfo) -> PatientInfo:
    """Attempts to extract patient observations from a fhir bundle. 

    Args: 
    - resource: ``Observation``, parsed from raw fhir message
    - patient_info: ``PatientInfo``, containing the parsed patient information thus far

    Returns: 
    - patient_info: ``PatientInfo``, with observations as a field 
    """

    value_quantity = None 
    value_codeable_concept = None
    component_list = None

    category = resource.category[0].coding[0].code
    observation = resource.code.text
    status = resource.status
    effective_date_time = resource.effectiveDateTime
    issued = resource.issued
    encounter_reference = str(resource.encounter.reference)
    subject_reference = str(resource.subject.reference)

    if resource.valueQuantity:
        value_quantity = str(resource.valueQuantity.value) + resource.valueQuantity.unit

    if resource.valueCodeableConcept:
        value_codeable_concept = resource.valueCodeableConcept.text

    if resource.component:

        # Component list is an array of dicts, of the form: 
        # - code_text: <survey question, test performed, ...>
        # - result   : <survey answer, test result, ...>
        component_list = []
        for component in resource.component:

            # Create empty dict for component partition
            component_dict = {}

            # Can be a question in survey, or test in suite of tests
            component_text = component.code.text

            # Assign result of component partition - survey answer, test result, ...
            component_result = None
            if component.valueQuantity:
                component_result = str(component.valueQuantity.value) + component.valueQuantity.unit
            if component.valueCodeableConcept:
                component_result = component.valueCodeableConcept.text
            if component.valueString:
                component_result = component.valueString

            # Add to dict 
            component_dict["code_text"] = component_text
            component_dict["result"] = component_result

            # Add dict to component array
            component_list.append(component_dict)

    patient_observation = PatientObservation(category=category, observation=observation, placer_order_number=None, 
                                                filler_order_number=None, status=status, 
                                                effective_date_time=effective_date_time, issued=issued, 
                                                value_quantity=value_quantity, 
                                                value_codeable_concept=value_codeable_concept, 
                                                encounter_reference=encounter_reference, 
                                                subject_reference=subject_reference, 
                                                component=component_list)
    patient_info.observations.append(patient_observation)
    return patient_info


def firestore_doc_to_patient_info(db: firestore.client, doc: document) -> PatientInfo:
    """Transforms a document from Firestore into a ``PatientInfo`` object for 
    further use. 

    Args: 
    - doc: ``document``, a retrieved Firestore document

    Returns:
    - patient_info: ``PatientInfo``, a class which holds all patient information 
    within the Firestore document
    
    """
    # Handle middle name 
    middle_name = None
    if ("middle_name" in doc._data): middle_name = doc._data["middle_name"] 

    # Handle creation date - if patient doesn't have one, then assign today's date
    if ("creation_date" in doc._data): 
        creation_date = doc._data["creation_date"]
    else: 
        creation_date = date.today().isoformat()

    # Handle possible missing hl7v2_id 
    if ("hl7v2_id" in doc._data):
        hl7v2_id = doc._data["hl7v2_id"]
    else:
        hl7v2_id = create_patient_hl7v2_id(db=db)

    # Create patient_info object for further use 
    patient_info = PatientInfo(
        id=doc._data["id"],
        hl7v2_id=hl7v2_id,
        birth_date=doc._data["birth_date"],
        gender=doc._data["gender"],
        ssn=doc._data["ssn"],
        first_name=doc._data["first_name"],
        middle_name=middle_name,
        last_name=doc._data["last_name"],
        address=doc._data["address"],
        address_2=doc._data["address_2"],
        city=doc._data["city"],
        country=doc._data["country"],
        post_code=doc._data["post_code"],
        country_code=doc._data["country_code"],
        age=doc._data["age"],
        creation_date=creation_date,
    )

    if ("conditions" in doc._data):
        for condition in doc._data["conditions"]:
            pat_condition=condition["condition"]
            clinical_status=condition["clinical_status"]
            verification_status=condition["verification_status"]
            onset_date_time=condition["onset_date_time"]
            recorded_date=condition["recorded_date"]
            abatement_time=condition["abatement_time"]
            encounter_reference=condition["encounter_reference"]
            subject_reference=condition["subject_reference"]
            snomed_code=condition["snomed_code"]

            condition_record = PatientCondition(condition=pat_condition, clinical_status=clinical_status, 
                                                verification_status=verification_status, onset_date_time=onset_date_time, 
                                                recorded_date=recorded_date, abatement_time=abatement_time, 
                                                encounter_reference=encounter_reference, subject_reference=subject_reference, 
                                                snomed_code=snomed_code)
            
            patient_info.conditions.append(condition_record)

    if ("observations" in doc._data):
        for observation in doc._data["observations"]:
            new_observation = PatientObservation(
                                    category=observation["category"],
                                    observation=observation["observation"],
                                    placer_order_number=observation["placer_order_number"],
                                    filler_order_number=observation["filler_order_number"],
                                    status=observation["status"],
                                    effective_date_time=observation["effective_date_time"],
                                    issued=observation["issued"],
                                    value_quantity=observation["value_quantity"],
                                    value_codeable_concept=observation["value_codeable_concept"],
                                    encounter_reference=observation["encounter_reference"],
                                    subject_reference=observation["subject_reference"],
                                    component=observation["component"]
                                )
            patient_info.observations.append(new_observation)

    return patient_info


def parse_HL7_message(msg:str, db:firestore.client):
    """
    A rudimentary function, still under development, which looks to return a parsed hl7 object, as well as 
    to retrieve patient info from a HL7 message if a PID / ORM field is present. 

    
    Arguments: 
    - msg: str, the HL7 message from which patient information should be taken. 

    Returns: 
    - hl7: a parsed hl7 object containing the information from msg
    - patient_info: PatientInfo, an object containing patient information retrieved from an HL7 message. 
    """
    
    hl7 = parse_message(msg.replace('\n', '\r'), validation_level=VALIDATION_LEVEL.QUIET, find_groups=True)

    patient_info = None

    if (hl7.msh.msh_9.to_er7() == "ORU^R01"):
        pid = hl7.oru_r01_patient_result.oru_r01_patient.pid
    elif (hl7.msh.msh_9.to_er7() == "ORM^O01"):
        pid = hl7.orm_o01_patient.pid
    elif (hl7.msh.msh_9.to_er7() == "ORU^R01"):
        pid = hl7.oru_r01_patient_result.oru_r01_patient.pid
    else:
        pid = hl7.pid

    # Empty list if no PID
    if (pid):
        try: 

            if pid.pid_2:
                patient_id = pid.pid_2.to_er7()
            else:
                # Pull largest id from firebase
                db_ref = db.collection("full_fhir")
                query = (
                    db_ref.order_by("id", direction=firestore.Query.DESCENDING).limit(1)
                )

                results = query.stream()
                for result in results:
                    greatest_id = result._data["id"]
                    break

                patient_id = increment_id(greatest_id)

            hl7v2_id = []
            for child in pid.pid_3:
                hl7v2_id.append(child.to_er7())
            print("Got patient HL7v2 id")
            print(hl7v2_id)

            birth_date = pid.pid_7.to_er7()
            # Turn into date object
            birth_date=datetime.datetime.strptime(birth_date, "%Y%m%d").date()

            print("Got patient DOB")

            gender = pid.pid_8.to_er7()
            if pid.pid_19:
                ssn = pid.pid_19.to_er7()
            else: 
                ssn = None

            print("Got patient gender & ssn")

            # Need better handling of possible empty fields vvvvv
            first_name = pid.pid_5.pid_5_2.to_er7()
            last_name = pid.pid_5.pid_5_1.to_er7()
            if pid.pid_5.pid_5_3:
                middle_name = pid.pid_5.pid_5_3.to_er7()
            else: 
                middle_name = None

            print("Got patient names")
            
            address = pid.pid_11.pid_11_1.to_er7()
            if pid.pid_11.pid_11_2:
                address_2 = pid.pid_11.pid_11_2.to_er7()
            else: 
                address_2 = None
            city = pid.pid_11.pid_11_3.to_er7()
            post_code = pid.pid_11.pid_11_5.to_er7()
            if pid.pid_11.pid_11_6:
                country_code = pid.pid_11.pid_11_6.to_er7()
            else: 
                country_code = None

            print("Got patient locations")

            age = calculate_age(birth_date=birth_date)
            creation_date = date.today()

            print("Got patient age & creation date")

            patient_info = PatientInfo(id=patient_id, birth_date=birth_date, gender=gender, ssn=ssn, first_name=first_name, 
                                    middle_name=middle_name, last_name=last_name, address=address, address_2=address_2, city=city,
                                    country="United Kingdom", post_code=post_code, country_code=country_code, age=age, 
                                    creation_date=creation_date, hl7v2_id=hl7v2_id)

        except Exception as e: 
            print(f"Error encountered while attempting to retrieve patient info from PID: {repr(e)}")
    else: 
        print("PID not found")

    if (hl7.msh.msh_9.to_er7() == "ORM^O01"):
        PatientObservation
        category = "laboratory"
        observation = hl7.orm_o01_order.orm_o01_order_detail.orm_o01_choice.obr.obr_4.obr_4_2.to_er7()
        placer_order_number = hl7.orm_o01_order.orc.orc_2.to_er7()
        if hl7.orm_o01_order.orc.orc_3:
            filler_order_number = hl7.orm_o01_order.orc.orc_3.to_er7()
        else:
            filler_order_number = None
        status = hl7.orm_o01_order.orc.orc_5.to_er7()
        if hl7.orm_o01_order.orm_o01_order_detail.orm_o01_choice.obr.obr_6:
            issued = hl7.orm_o01_order.orm_o01_order_detail.orm_o01_choice.obr.obr_6.to_er7()
        else:
            issued = datetime.datetime.now().isoformat(timespec="minutes")
        subject_reference = patient_info.hl7v2_id

        observation = PatientObservation(category=category, observation=observation, placer_order_number=placer_order_number, 
                                         filler_order_number=filler_order_number, status=status, effective_date_time=None, 
                                         issued=issued, value_quantity=None, value_codeable_concept=None, encounter_reference=None,
                                         subject_reference=subject_reference, component=None)

        patient_info.observations.append(observation)

    if (hl7.msh.msh_9.to_er7() == "ORU^R01"):

        PatientObservation
        # To determine category correctly, would need to reference common coding systems 
        category = "laboratory"

        observation = hl7.oru_r01_patient_result.oru_r01_order_observation.obr.obr_4.obr_4_2.to_er7()
        placer_order_number = hl7.oru_r01_patient_result.oru_r01_order_observation.orc.orc_2.to_er7()
        if hl7.oru_r01_patient_result.oru_r01_order_observation.orc.orc_3:
            filler_order_number = hl7.oru_r01_patient_result.oru_r01_order_observation.orc.orc_3.to_er7()
        else: 
            filler_order_number = None
        status = hl7.oru_r01_patient_result.oru_r01_order_observation.obr.obr_25.to_er7()
        if hl7.oru_r01_patient_result.oru_r01_order_observation.obr.obr_6:
            issued = hl7.oru_r01_patient_result.oru_r01_order_observation.obr.obr_6.to_er7()
        else:
            issued = None
        effective_date_time = hl7.oru_r01_patient_result.oru_r01_order_observation.obr.obr_7.to_er7()
        encounter_reference = None 
        subject_reference = patient_info.hl7v2_id

        obx_segments = [x.OBX for x in hl7.ORU_R01_PATIENT_RESULT.ORU_R01_ORDER_OBSERVATION.ORU_R01_OBSERVATION]

        if len(obx_segments) > 1:
            component = []
            value_quantity = None 
            value_codeable_concept = None 
            for obx in obx_segments:
                component_dict = {}
                component_dict["code_text"] = obx.obx_3.to_er7() 
                component_dict["result"] = obx.obx_5.to_er7() + " " + obx.obx_6.to_er7()
                component.append(component_dict)
                
        else:
            component = None 
            obx = obx_segments[0]
            if (obx.obx_2.to_er7() == ("CE" or "CWE")):
                value_codeable_concept = obx.obx_5.to_er7()
                value_quantity = None 
            else:
                value_quantity = obx.obx_5.to_er7() + " " + obx.obx_6.to_er7()
                value_codeable_concept = None
                
        observation = PatientObservation(category=category, observation=observation, placer_order_number=placer_order_number, 
                                         filler_order_number=filler_order_number, status=status, 
                                         effective_date_time=effective_date_time, issued=issued, value_quantity=value_quantity, 
                                         value_codeable_concept=value_codeable_concept, encounter_reference=encounter_reference,
                                         subject_reference=subject_reference, component=component)

        patient_info.observations.append(observation)

    return hl7, patient_info


def get_firestore_age_range(db: firestore.client, num_of_patients: int, lower: int, upper: int, peter_pan: bool) -> list[PatientInfo]: 
    """
    Pull patient information from Firestorm, given an age range. If not enough patients exist in the firestore, 
    they will be generated using poll_synthea and the HL7 processor. 

    If peter_pan is set to true, patients will have their DOBs changed to match their age at time of creation.
    If false, their age will be updated using their DOB. 
    
    Returns a list of patients.
    """

    patients = []
    uploaded_patients = []

    while (len(patients) == 0):
        count, query = count_patient_records(db, lower, upper, peter_pan)

        # If there are enough patients...
        if (count >= num_of_patients):

            docs = query.limit(num_of_patients).stream()

            # Stream the patient docs 
            for doc in docs:
                patient_info = firestore_doc_to_patient_info(db=db, doc=doc)

                # Matches age with dob - method for doing so depends on the peter_pan bool
                if peter_pan:
                    patient_info = update_retrieved_patient_dob(patient_info=patient_info)
                else: 
                    patient_info = update_retrieved_patient_age(patient_info=patient_info)

                patients.append(patient_info)

            # Return a list of patients     
            return patients
        
        else: 
            print(f"Database only has {count} matching patient(s) - generating new patients...")

            info = {
                "number_of_patients": int(num_of_patients - count),
                "age_from": lower, 
                "age_to": upper, 
                "sex": "F"
            }

            # Generate patients using poll_synthea
            call_for_patients(info=info)

            # Iterate through FHIR JSON files in the work folder
            for file in work_folder_path.glob("*.json"):
                if file.name not in uploaded_patients:
                    try: 
                        with open(file, "r") as f:
                            fhir_message = f.read()

                            # Parse patient information from file 
                            patient_info = parse_fhir_message(fhir_message)
                            save_to_firestore(db=db, patient_info=patient_info)
                            uploaded_patients.append(file.name)
                            
                    except UnicodeDecodeError as e:
                        print("Problem reading file...")
                        print(e)
                    except Exception as e: 
                        print("Couldn't parse patient information from fhir message...")
                        time.sleep(3)


def update_retrieved_patient_dob(patient_info: PatientInfo, ) -> PatientInfo:
    """Uses the patient's creation date to calculate their new date of birth. 
    
    This function is called if patients are retrieved with the 'peter_pan' bool 
    set to true. 
    """

    current_date = date.today()
    creation_date = patient_info.creation_date
    birth_date = patient_info.birth_date

    # Find days passed since creation date 
    years_passed = (current_date.year - creation_date.year)

    # We only change their birth year, as most patients' DOB will be 01/01/...
    if (years_passed > 0): 

        # Add the number of years passed
        new_birth_date = birth_date.replace(year=(birth_date.year + years_passed))

        patient_info.birth_date = new_birth_date

    # Only becomes relevant for tricky DOB close to current date, i.e., patient has just turned 18 yesterday
    elif (patient_info.age != calculate_age(patient_info.birth_date)):

        # Add a single year as patient must have recent birthday
        new_birth_date = birth_date.replace(year=(birth_date.year + 1))

        patient_info.birth_date = new_birth_date

    return patient_info


def update_retrieved_patient_age(patient_info: PatientInfo) -> PatientInfo:
    """Changes the patient's age to match their date of birth.
    
    This function is called if patients are retrieved with the 'peter_pan' bool 
    set to false. 
    """

    patient_info.age = calculate_age(birth_date=patient_info.birth_date)

    return patient_info


def assign_age_to_patient(patient_info: PatientInfo, desired_age: int, index: int | None) -> PatientInfo:
    """Changes the patient's date of birth and age to the desired age
    
    Optional arg - index: int, which indicates the position of the patient in the array looped through
    """

    # Sets year of birth to appropriate year; day and month are both '01' to simplify references

    new_birth_date = date.today().replace(year=(date.today().year - desired_age), month=1, day=1)

    # Increment the new_birth_date for each patient iteration in the list  
    if index: new_birth_date = new_birth_date + datetime.timedelta(days=index)

    patient_info.birth_date = new_birth_date
    patient_info.age = desired_age

    return patient_info


def count_patient_records(db: firestore.client, lower: int, upper: int, peter_pan: bool) -> tuple[int | float, any]:
    """Counts the number of patient records that match the age requirements specified. 

    Returns both the count of the patients in the db, and the query used in the check. 
    """

    # Form the query based on peter_pan bool 
    if peter_pan:

        # We can simply collect patients using 'age', as will be changing their dob to match
        query = db.collection("full_fhir").where(filter=FieldFilter("age", "<=", upper))\
                                            .where(filter=FieldFilter("age", ">=", lower))
    else:

        # We need to calculate the appropriate dob ranges; we can't search by age as we will change this
        current_date = date.today()

        # If they are X years old today, their DOB will fall between these ranges
        lower_year = current_date.year - lower
        upper_dob = current_date.replace(year=lower_year)

        upper_year = current_date.year - upper 
        lower_dob = current_date.replace(year=upper_year)

        # Find all records between the two valid DOBs
        query = db.collection("full_fhir").where(filter=FieldFilter("birth_date", "<=", upper_dob.isoformat()))\
                                            .where(filter=FieldFilter("birth_date", ">=", lower_dob.isoformat()))
    
    aggregate_query = aggregation.AggregationQuery(query)

    # `alias` to provides a key for accessing the aggregate query results
    aggregate_query.count(alias="all")

    # Get the number of patient records which fit the criteria
    results = aggregate_query.get()
    count = results[0][0].value

    return count, query


def save_to_firestore(db: firestore.client, patient_info: PatientInfo, update_record:bool=False) -> None:
        """Saves patient info to Firestore. 

        If no patient exists with the id specified in the ``PatientInfo`` object, then a new 
        document will be created in Firestore containing their information. 

        If a document containing the patient's information already exists in Firestore, then this
        document will be overwritten by the new patient information given as an argument, provided 
        that the ``update_record`` argument is set to ``True``
        
        Args: 
        - db: ``firestore.client``, an initialised firestore client
        - patient_info: ``PatientInfo``, a PatientInfo object
        - update_record: ``bool`` = False, used to determine whether or not to overwrite existing docs

        Returns: 
        - ``None``
        """

        try: 
            patient_id = patient_info.id
            patient_ref = db.collection("full_fhir").document(patient_id)
            if ((patient_ref.get().exists) and (not update_record)):
                print(
                    f"Patient with ID {patient_id} already exists in Firestore. Skipping."
                )
            else:

                # Preparations before uploading 
                if patient_info.hl7v2_id:
                    hl7v2_id = patient_info.hl7v2_id
                else:
                    hl7v2_id = [create_patient_hl7v2_id(db=db)]

                if type(patient_info.birth_date) != str:
                    patient_info.birth_date = patient_info.birth_date.isoformat()
                if type(patient_info.creation_date) != str:
                    patient_info.creation_date = patient_info.creation_date.isoformat()

                patient_data = {
                    "id": patient_info.id,
                    "hl7v2_id": hl7v2_id,
                    "birth_date": patient_info.birth_date,
                    "gender": patient_info.gender,
                    "ssn":patient_info.ssn,
                    "first_name": patient_info.first_name,
                    "middle_name": patient_info.middle_name,
                    "last_name": patient_info.last_name,
                    "address": patient_info.address,
                    "address_2": patient_info.address_2,
                    "city": patient_info.city,
                    "country": patient_info.country,
                    "post_code": patient_info.post_code,
                    "country_code": patient_info.country_code,
                    "age":patient_info.age,
                    "creation_date":patient_info.creation_date
                }

                if hasattr(patient_info, 'conditions'):
                    conditions = []
                    for condition in patient_info.conditions:
                        conditions.append(condition.__dict__)
                    patient_data["conditions"] = conditions

                if hasattr(patient_info, 'observations'):
                    observations = []
                    for observation in patient_info.observations:
                        observations.append(observation.__dict__)
                    patient_data["observations"] = observations

                patient_ref.set(patient_data)
                if update_record:
                    print(f"Patient with ID {patient_id} has been updated.")
                else:
                    print(f"Added patient with ID {patient_id} to Firestore.")

        except Exception as e:
            print('Failed to upload to Firestore: %s', repr(e)) 


def update_following_ORM_O01(db: firestore.client, patient_info: PatientInfo) -> None: 
    """Looks to update a patient record in Firestore following the reception of an ORM_O01 message. 

    If a patient with a matching HL7v2 id can be found in the database, the new observation request 
    will be added to their record. 

    If no patient can be found with a matching HL7v2 id, then a new document will be created in Firestore 
    containing the patient information, along with the observation request. 

    Args: 
    - db: ``firestore.client``, the client used to connect to Firestore 
    - patient_info: ``PatientInfo``, the patient information parsed from the HL7v2 ORM_O01 message

    Returns: 
    - ``None``
    """
    retrieved_patient_info = None
    db_ref = db.collection("full_fhir")

    for hl7v2_id in patient_info.hl7v2_id:
        if not retrieved_patient_info:
            filter1 = FieldFilter("hl7v2_id", "==", hl7v2_id)
            filter2 = FieldFilter("hl7v2_id", "array_contains", hl7v2_id)
            or_filter = Or(filters=[filter1, filter2])

            query = (
                db_ref.where(filter=or_filter).limit(1)
            )

            print(f"Looking for patient with hl7_v2 id {hl7v2_id}...")
            results = query.stream()

            for doc in results:
                retrieved_patient_info = firestore_doc_to_patient_info(db=db, doc=doc)
                print("Found patient record.")

    if retrieved_patient_info: 

        retrieved_patient_info.observations.extend(patient_info.observations)
        save_to_firestore(db=db, patient_info=retrieved_patient_info, update_record=True)
    else: 
        print("No matching patient found - creating new record.")
        save_to_firestore(db=db, patient_info=patient_info)


def update_following_ORU_R01(db: firestore.client, patient_info: PatientInfo) -> None: 
    """Looks to update a patient record in Firestore following the reception of an ORU_R01 message. 

    If a patient with a matching HL7v2 id can be found in the database, the new observation result 
    will be added to their record. 

    If no patient can be found with a matching HL7v2 id, then a new document will be created in Firestore 
    containing the patient information, along with the observation result. 

    Args: 
    - db: ``firestore.client``, the client used to connect to Firestore 
    - patient_info: ``PatientInfo``, the patient information parsed from the HL7v2 ORU_R01 message

    Returns: 
    - ``None``
    """
    retrieved_patient_info = None
    db_ref = db.collection("full_fhir")

    for hl7v2_id in patient_info.hl7v2_id:
        if not retrieved_patient_info:
            filter1 = FieldFilter("hl7v2_id", "==", hl7v2_id)
            filter2 = FieldFilter("hl7v2_id", "array_contains", hl7v2_id)
            or_filter = Or(filters=[filter1, filter2])

            query = (
                db_ref.where(filter=or_filter).limit(1)
            )

            print(f"Looking for patient with hl7_v2 id {hl7v2_id}...")
            results = query.stream()

            for doc in results:
                retrieved_patient_info = firestore_doc_to_patient_info(db=db, doc=doc)
                print("Found patient record.")

    if retrieved_patient_info: 

        for i, observation in enumerate(retrieved_patient_info.observations):

            # Should use placer and filler order numbers to match in future
            if observation.observation == patient_info.observations[0].observation \
                and observation.placer_order_number == patient_info.observations[0].placer_order_number \
                and observation.filler_order_number == patient_info.observations[0].filler_order_number:

                print("Observation identified.")
                # Update the retrieved info with the new observation results
                retrieved_patient_info.observations[i] = patient_info.observations[0]

                print(retrieved_patient_info)

                print("Updating record...")
                save_to_firestore(db=db, patient_info=retrieved_patient_info, update_record=True)
                break
    else: 
        print("No matching patient found - creating new record.")
        save_to_firestore(db=db, patient_info=patient_info)
