# utilities.py
import random, string, datetime
from datetime import date, datetime
import datetime
from fhir.resources.R4B.bundle import Bundle
from fhir.resources.R4B.patient import Patient
from firebase_admin import firestore
from google.cloud.firestore_v1.base_query import FieldFilter
from google.cloud.firestore_v1 import aggregation

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


# Creates a random patient ID for the patient 
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


def get_firestore_age_range(db: firestore.client, num_of_patients: int, lower: int, upper: int, peter_pan: bool) -> list[PatientInfo]: 
    """Pull patient information from Firestorm, given an age range.

    If peter_pan is set to true, patients will have their DOBs changed to match their age at time of creation.
    If false, their age will be updated using their DOB. 
    
    Returns a list of patients, or None if not enough patients in the database match the parameters provided.
    
    """

    patients = []
    count, query = count_patient_records(db, lower, upper, peter_pan)

    # If there are enough patients...
    if (count >= num_of_patients):

        docs = query.limit(num_of_patients).stream()

        # Stream the patient docs 
        for doc in docs:

            # Handle middle name 
            middle_name = None
            if ("middle_name" in doc._data): middle_name = doc._data["middle_name"] 

            # Handle creation_date 
            creation_date = None
            if ("creation_date" in doc._data): creation_date = doc._data["creation_date"]

            # Create patient_info object for further use 
            patient_info = PatientInfo(
                id=doc._data["id"],

                # Consider simply converting birth date at this point instead of throughout
                birth_date=doc._data["birth_date"],
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
        print(f"Patient retrieval failed - database only has {count} matching patient(s).")
        return None


def update_retrieved_patient_dob(patient_info: PatientInfo) -> PatientInfo:
    """Uses the patient's age to calculate their new date of birth"""

    current_date = date.today()
    creation_date = datetime.datetime.strptime(patient_info.creation_date, "%Y-%m-%d").date()
    birth_date = datetime.datetime.strptime(patient_info.birth_date, "%Y-%m-%d").date()

    # Find days passed since creation date 
    days_passed = (current_date - creation_date).days

    # Add the number of days passed to the original birth date, arriving at updated birth date 
    new_birth_date = birth_date + datetime.timedelta(days=days_passed)

    patient_info.birth_date = new_birth_date
    return patient_info


def update_retrieved_patient_age(patient_info: PatientInfo) -> PatientInfo:
    """Changes the patient's age to match their date of birth"""

    birth_date = datetime.datetime.strptime(patient_info.birth_date, "%Y-%m-%d").date()
    patient_info.age = calculate_age(birth_date=birth_date)

    return patient_info


def assign_age_to_patient(patient_info: PatientInfo, desired_age: int) -> PatientInfo:
    """Changes the patient's date of birth and age to the desired age"""

    # Randomises date of birth within age parameters
    new_birth_date = date.today().replace(year=(date.today().year - desired_age)) \
                        - datetime.timedelta(days=random.randint(1, 364))

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
                                            .where(filter=FieldFilter("age", ">=", lower))\
                                            .order_by("creation_date")
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