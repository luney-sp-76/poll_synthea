import firebase_admin
from firebase_admin import credentials, firestore
from fhir.resources.R4B.bundle import Bundle
from fhir.resources.R4B.patient import Patient
from datetime import datetime, date
from pathlib import Path
import poll_synthea

BASE_DIR = Path.cwd()
work_folder_path = BASE_DIR / "Work"


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


poll_synthea.call_for_patients()


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


# Initialize Firebase Admin SDK with your credentials
cred = credentials.Certificate("firebase/pollsynthea-firebase-adminsdk-j01m1-bb3600d47d.json")
firebase_admin.initialize_app(cred)

db = firestore.client()

# Iterate through FHIR JSON files in the work folder
for file in work_folder_path.glob("*.json"):
    with open(file, "r") as f:
        fhir_message = f.read()
        patient_info = parse_fhir_message(fhir_message)
        if patient_info:
            patient_json = {
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
            print(patient_json)  # Print the JSON FHIR message

# Add patient information to Firestore
            doc_ref = db.collection("full_fhir").document(patient_info.id)
            doc_ref.set(patient_json)
            print("Patient information added to Firestore:", patient_info.id)
