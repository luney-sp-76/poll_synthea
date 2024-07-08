# utilities.py
import logging
from pathlib import Path
import random, string, datetime
from datetime import date, datetime
import datetime
import time
from fhir.resources.R4B.bundle import Bundle
from fhir.resources.R4B.patient import Patient
from firebase_admin import firestore
from google.cloud.firestore_v1.base_query import FieldFilter
from google.cloud.firestore_v1 import aggregation
from ..poll_synthea import call_for_patients
from hl7apy.parser import parse_message


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


# Creates a random patient ID for the patient - need more clarity before changing 
def create_patient_id():
    alphanumeric = "".join(
        ["{}".format(random.choice(string.ascii_uppercase + string.digits)) for _ in range(0, 8)]
    )
    patient_id = f"{alphanumeric}^^^PAS^MR"
    return patient_id


# PatientInfo class to store patient information from a Bundled FHIR message
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
        creation_date,
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
        self.creation_date = creation_date


    def __repr__(self):  
        return ("PatientInfo id:% s birth_date:% s gender:% s ssn:% s first_name:% s middle_name:% s last_name:% s "
                "city:% s state:% s country:% s postal_code:% s age:% s creation_date:% s") % \
                (self.id, self.birth_date, self.gender, self.ssn, self.first_name, self.middle_name, self.last_name, \
                 self.city, self.state, self.country, self.postal_code, self.age, self.creation_date)
    

    def __str__(self):
        return ("From str method of PatientInfo: id is % s, birth_date is % s, gender is % s, ssn is % s, "
                "first_name is % s, middle_name is % s, last_name is % s, city is % s, state is % s, country is % s, "
                "postal_code is % s, age is % s, creation_date is % s") % \
                (self.id, self.birth_date, self.gender, self.ssn, self.first_name, self.middle_name, self.last_name, \
                 self.city, self.state, self.country, self.postal_code, self.age, self.creation_date)


# Calculate the age of the patient
def calculate_age(birth_date):
    today = date.today()
    age = (
        today.year
        - birth_date.year
        - ((today.month, today.day) < (birth_date.month, birth_date.day))
    )
    return age


# TODO update the dobs after the sample patients are created - 
# if a request is for 365 patients between the age 10 and 11 then each patient 
# could be given a Day of birth that is incremented one day older than the previous for the whole year


# Parses a FHIR JSON message and returns a PatientInfo object
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

            # Handle missing middle name
            if len(resource.name[0].given) > 1:
                middle_name = resource.name[0].given[1]
            else:
                middle_name = None

            patient_info = PatientInfo(
                id=resource.id,
                birth_date=birth_date,
                gender=resource.gender,
                ssn=ssn,
                first_name=resource.name[0].given[0],
                middle_name=middle_name,
                last_name=resource.name[0].family,
                city=resource.address[0].city,
                state=resource.address[0].state,
                country=resource.address[0].country,
                postal_code=resource.address[0].postalCode,
                age=age,
                creation_date=date.today(),
            )
            break  # Assuming there's only one patient resource per FHIR message
    return patient_info


def parse_HL7_message(msg):
    """
    A rudimentary function, still under development, which looks to return a parsed hl7 object, as well as 
    to retrieve patient info from a HL7 message if a PID field is present. 
    
    Arguments: 
    - msg: str, the HL7 message from which patient information should be taken. 

    Returns: 
    - hl7: a parsed hl7 object containing the information from msg
    - patient_info: PatientInfo, an object containing patient information retrieved from an HL7 message. 
    """
    
    hl7 = parse_message(msg.replace('\n', '\r'), find_groups=True)
    patient_info = None

    if (hl7.msh.msh_9.to_er7() == "ORU^R01"):
        hl7 = hl7.oru_r01_patient_result.oru_r01_patient

    # Empty list if no PID
    if (hl7.pid):
        try: 
            patient_id = hl7.pid.pid_3.to_er7()
            print("Got patient id")

            birth_date = hl7.pid.pid_7.to_er7()
            # Turn into date object
            birth_date=datetime.datetime.strptime(birth_date, "%Y%m%d").date()

            print("Got patient DOB")

            gender = hl7.pid.pid_8.to_er7()
            ssn = hl7.pid.pid_19.to_er7()

            print("Got patient gender & ssn")

            # Need better handling of possible empty fields vvvvv
            first_name = hl7.pid.pid_5.pid_5_2.to_er7()
            last_name = hl7.pid.pid_5.pid_5_1.to_er7()
            middle_name = hl7.pid.pid_5.pid_5_3.to_er7()

            print("Got patient names")
            
            city = hl7.pid.pid_11.pid_11_3.to_er7()
            state = hl7.pid.pid_11.pid_11_4.to_er7()
            postal_code = hl7.pid.pid_11.pid_11_5.to_er7()
            country = hl7.pid.pid_11.pid_11_6.to_er7()

            print("Got patient locations")

            age = calculate_age(birth_date=birth_date)
            creation_date = date.today()

            print("Got patient age & creation date")

            patient_info = PatientInfo(id=patient_id, birth_date=birth_date, gender=gender, ssn=ssn, first_name=first_name, 
                                    middle_name=middle_name, last_name=last_name, city=city, state=state, country=country, 
                                    postal_code=postal_code, age=age, creation_date=creation_date)

        except Exception as e: 
            print("Error encountered while attempting to retrieve patient info from PID.")
            print(str(e))

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

                # Handle middle name 
                middle_name = None
                if ("middle_name" in doc._data): middle_name = doc._data["middle_name"] 

                # Handle creation date - if patient doesn't have one, then assign today's date
                if ("creation_date" in doc._data): 
                    creation_date = datetime.datetime.strptime(doc._data["creation_date"], "%Y-%m-%d").date()
                else: 
                    creation_date = date.today()

                # Create patient_info object for further use 
                patient_info = PatientInfo(
                    id=doc._data["id"],

                    # Consider simply converting birth date at this point instead of throughout
                    birth_date=datetime.datetime.strptime(doc._data["birth_date"], "%Y-%m-%d").date(),
                    gender=doc._data["gender"],
                    ssn=doc._data["ssn"],
                    first_name=doc._data["first_name"],
                    middle_name=middle_name,
                    last_name=doc._data["last_name"],
                    city=doc._data["city"],
                    state=doc._data["state"],
                    country=doc._data["country"],
                    postal_code=doc._data["postal_code"],
                    age=doc._data["age"],
                    creation_date=creation_date,
                )

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


def save_to_firestore(db: firestore.client, patient_info: PatientInfo) -> None:
        """Save patient info to Firestore."""
        patient_id = patient_info.id
        patient_ref = db.collection("full_fhir").document(patient_id)
        if patient_ref.get().exists:
            print(
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
                "state":patient_info.state,
                "age":patient_info.age,
                "creation_date":patient_info.creation_date.isoformat(),
            }
            patient_ref.set(patient_data)
            print(f"Added patient with ID {patient_id} to Firestore.")
