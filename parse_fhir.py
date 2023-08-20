from fhir.resources.R4B.bundle import Bundle
from fhir.resources.R4B.patient import Patient
from pathlib import Path
import poll_synthea

BASE_DIR = Path.cwd()
work_folder_path = BASE_DIR / "Work"


class PatientInfo:
    def __init__(self, id, birth_date, gender, first_name, last_name, city, state, country):
        self.id = id
        self.birth_date = birth_date
        self.gender = gender
        self.first_name = first_name
        self.last_name = last_name
        self.city = city
        self.state = state
        self.country = country


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
            patient_info = PatientInfo(
                id=resource.id,
                birth_date=resource.birthDate,
                gender=resource.gender,
                first_name=resource.name[0].given[0],
                last_name=resource.name[0].family,
                city=resource.address[0].city,
                state=resource.address[0].state,
                country=resource.address[0].country
            )
            break  # Assuming there's only one patient resource per FHIR message
    return patient_info


# Iterate through FHIR JSON files in the work folder
for file in work_folder_path.glob("*.json"):
    with open(file, "r") as f:
        fhir_message = f.read()
        patient_info = parse_fhir_message(fhir_message)
        if patient_info:
            patient_json = {
                "id": patient_info.id,
                "birth_date": patient_info.birth_date,
                "gender": patient_info.gender,
                "first_name": patient_info.first_name,
                "last_name": patient_info.last_name,
                "city": patient_info.city,
                "state": patient_info.state,
                "country": patient_info.country
            }
            print(patient_json)  # Print the JSON FHIR message