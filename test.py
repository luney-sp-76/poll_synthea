from pathlib import Path
from main import initialize_firestore, get_firestore_age_range, hl7_folder_path, produce_ADT_A01_from_firestore, \
    produce_OML_O21_from_firestore
from generators.utilities import PatientCondition, PatientInfo, PatientObservation, \
    assign_age_to_patient, calculate_age, count_patient_records, parse_fhir_message, save_to_firestore, \
        firestore_doc_to_patient_info, create_patient_id
import unittest, datetime, numbers, os, os.path
from poll_synthea import call_for_patients
from google.cloud.firestore_v1.base_query import FieldFilter
from google.cloud.firestore_v1 import aggregation
import requests

BASE_DIR = Path.cwd()
work_folder_path = BASE_DIR / "Work"


def number_to_alphanumeric_upper(n):
    characters = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    base = len(characters)
    
    if n == 0:
        return characters[0] * 5
    
    result = []
    while n > 0:
        result.append(characters[n % base])
        n //= base
    
    result.reverse()
    result_str = ''.join(result)
    
    # Pad with leading zeros to ensure the result is 5 characters long
    return result_str.zfill(5)
  

class Test(unittest.TestCase):
    """
    The main class which holds all unit tests. 

    To run an individual test, simply run the file.

    To run an individual test, from the terminal: 
    - py test.py Test.<function name goes here>

    For example: py .\test.py Test.test_patient_retrieval_in_age_range
    """

    def test_patient_retrieval_in_age_range(self):
        """Test to ensure patients retrieved are within the age range specified

        Keep the number of patients low to limit requests to firebase. 
        
        """

        num_of_patients = 10
        lower_bound = 10
        upper_bound = 20
        peter_pan = True

        patients: list[PatientInfo] = get_firestore_age_range(db=firestore, num_of_patients=num_of_patients, \
                                                              lower=lower_bound, upper=upper_bound, peter_pan=peter_pan)

        for patient in patients: 
            with self.subTest(patient = patient):
                self.assertTrue(lower_bound <= patient.age <= upper_bound, "Should be within given range")

            with self.subTest(patient = patient):
                self.assertEqual(calculate_age(patient.birth_date), patient.age, "Should be equal")
            with self.subTest(patient=patient):
                self.assertTrue(lower_bound <= calculate_age(patient.birth_date) <= upper_bound, "Should be within given range")


    def test_assign_patient_age(self):
        """Test to ensure patients are correctly assigned their new age, along with a valid dob. 
        
        Keep the number of patients low to limit number of requests to firebase. 
        
        """

        num_of_patients = 5
        new_age = 99
        peter_pan = False

        patients: list[PatientInfo] = get_firestore_age_range(db=firestore, num_of_patients=num_of_patients, \
                                                              lower=10, upper=100, peter_pan=peter_pan)

        for i, patient in enumerate(patients): 
            updated_patient = assign_age_to_patient(patient_info=patient, desired_age=new_age, index=i)
            with self.subTest(patient = patient):
                self.assertEqual(calculate_age(updated_patient.birth_date), new_age, "Should be equal")
            with self.subTest(patient = patient):
                self.assertEqual(patient.age, new_age, "Should be equal")


    def test_count_docs_in_firestore(self):
        """Testing producing a count of all docs that fit certain criteria
        
        This will be used prior to streaming patient data, to prevent requests being sent for 
        more patients than those that exist within the database.
        
        """

        lower = 10
        upper = 20
        peter_pan = True

        count, _ = count_patient_records(db=firestore, lower=lower, upper=upper, peter_pan=peter_pan)

        self.assertIsInstance(count, numbers.Number)


    def test_production_of_ADT_A01(self):
        """Testing the production of ADT_A01 messages using patient info from firestore.
        
        Function should take patient information from firestore, and use it to construct an ADT_A01. 
        
        """

        num_of_patients = 10
        lower = 10
        upper = 100
        peter_pan = False

        num_files_before = len([name for name in os.listdir(hl7_folder_path)])

        status = produce_ADT_A01_from_firestore(db=firestore, num_of_patients=num_of_patients, \
                                       lower=lower, upper=upper, peter_pan=peter_pan)

        # Test to see if there are 'num_of_patients' more files in the folder after function runs

        num_files_after = len([name for name in os.listdir(hl7_folder_path)])

        # IMPORTANT: this assertion may fail if files in HL7_v2 folder are not removed before running, 
        # as there is a chance files with the same name will be produced, overwriting existing HL7 files, 
        # and leaving the total number of files within the folder unchanged!
        self.assertEqual(num_files_before + num_of_patients, num_files_after)
        self.assertTrue(status)


    def test_production_of_OML_021(self):

        hl7_messages = produce_OML_O21_from_firestore(db=firestore, num_of_patients=1, age=30, assign_age=True)

        for hl7_message in hl7_messages:

            # Display HL7 message
            print("-------------------")
            print(hl7_message.msh.value)
            print(hl7_message.pid.value)
            print(hl7_message.orc.value)
            print(hl7_message.obr.value)
            print("-----------------\n")


    def test_generation_of_patients_following_low_count(self):
        num_of_patients = 35
        lower = 56
        upper = 57
        peter_pan = True
        patients = None 

        patients = get_firestore_age_range(firestore, num_of_patients, lower, upper, peter_pan)

        for patient in patients: 
            print (patient.first_name)

        self.assertEqual(len(patients), num_of_patients)

        
#     def test_parsing_hl7_message(self):
#         hl7_message = """MSH|^~\&|HIS|RIH|ADT|RIH|20230523102000||ADT^A31|123456|P|2.4
# EVN|A31|20230523102000
# PID|1||12345678^^^RIH^MR||Doe^John^A||19800101|M|||456 Elm St^^Newtown^CA^90211^USA||555-5678||||M|N|123-45-6789
# PV1|1|O|^^^RIH||||1234^Smith^John^A|||||||||||||||12345678"""
    
#         parse_HL7_message(hl7_message)


    # def test_reception_of_ORU_R01_message(self):
    #     """
    #     Testing reception of ORU_R01 messages - utilises much of the same code from the project 
    #     <http_to_flatfile_to_http_hl7>. Ensure tcp_HL7 folder is empty before running. 

    #     Note: attempting to kill the client and server running as subprocesses will result in a warning message, 
    #     which states that the subprocess is still running, and provides the subprocess PID. 

    #     On Windows: 
    #     - You can check for the existence of this subprocess from the terminal using: ps -id <PID> 
    #     - You can kill this subprocess if it exists from the terminal using: taskkill /PID <PID> /F
    #     """
    #     server = subprocess.Popen(["py", "tcp_server.py"], shell=False)
    #     client = subprocess.Popen(["py", "tcp_client.py"], shell=False)

    #     time.sleep(2)
        
    #     ORU_R01_file = Path(HL7_FILE_PATH)
    #     self.assertTrue(ORU_R01_file.is_file())

    #     # Tidying up
    #     server = psutil.Process(server.pid)
    #     for child in server.children(recursive=True): 
    #         child.kill()
    #     server.kill()
        
      
    def test_condition_parsing(self):
        """ Tests the creation of the condition attribute within a ``PatientInfo`` object 
        using the ``PatientCondition`` class. 

        Requires at least one fhir doc in the ``Work`` folder. 
        """
        for file in work_folder_path.glob("*.json"):
            with open(file, "r") as f:
                fhir_message = f.read()

                # Parse patient information from file 
                patient_info = parse_fhir_message(db=firestore, fhir_message=fhir_message)

                if patient_info.conditions:
                    for condition in patient_info.conditions:
                        self.assertTrue(hasattr(condition, "condition"))
                        self.assertTrue(hasattr(condition, "clinical_status"))
                        self.assertTrue(hasattr(condition, "verification_status"))
                        self.assertTrue(hasattr(condition, "onset_date_time"))
                        self.assertTrue(hasattr(condition, "recorded_date"))
                        self.assertTrue(hasattr(condition, "encounter_reference"))
                        self.assertTrue(hasattr(condition, "subject_reference"))


    def test_observation_parsing(self): 
        """ Tests the creation of the observation attribute within a ``PatientInfo`` object 
        using the ``PatientObservation`` class. 

        Requires at least one fhir doc in the ``Work`` folder. 
        """
        for file in work_folder_path.glob("*.json"):
            with open(file, "r") as f:
                fhir_message = f.read()

                # Parse patient information from file 
                patient_info = parse_fhir_message(db=firestore, fhir_message=fhir_message)

                if patient_info.observations:
                    for observation in patient_info.observations:
                        self.assertTrue(hasattr(observation, "category"))
                        self.assertTrue(hasattr(observation, "observation"))
                        self.assertTrue(hasattr(observation, "status"))
                        self.assertTrue(hasattr(observation, "effective_date_time"))
                        self.assertTrue(hasattr(observation, "issued"))
                        self.assertTrue(hasattr(observation, "encounter_reference"))
                        self.assertTrue(hasattr(observation, "subject_reference"))
                        if observation.component:
                            for component in observation.component:
                                self.assertTrue(component["code_text"])
                                self.assertTrue(component["result"])


    def test_fhir_conditions_observations_upload(self):
        """Testing the upload of patient conditions and observations to Firestore. 
        """
        print(f"Generating new patients...")

        info = {
            "number_of_patients": 1,
            "age_from": 10, 
            "age_to": 80, 
            "sex": "M"
        }

        # Generate patients using poll_synthea
        call_for_patients(info=info)

        for file in work_folder_path.glob("*.json"):
            try: 
                with open(file, "r") as f:
                    fhir_message = f.read()

                    # Parse patient information from file 
                    patient_info = parse_fhir_message(db=firestore, fhir_message=fhir_message)

                    save_to_firestore(db=firestore, patient_info=patient_info)
            except Exception as e: 
                 print('Failed to parse Fhir & upload to Firestore: %s', repr(e))


    def test_retrieval_of_conditions_observations(self):
        """Testing the retrieval and parsing of patient information from Firestore 
        docs, including patient conditions and observations. 

        To retrieve only docs with both conditions and observations fields, use the retrieval 
        with creation date 2024-07-17.

        To instead retrieve a number of docs which may or may not contain conditions and/or observations, 
        use the retrieval with a limit, and set the limit to the desired number of docs to test. 
        """
        # Retrieve docs from Firestore
        
        docs = firestore.collection("full_fhir").where(filter=FieldFilter("creation_date", "==", "2024-07-31")).limit(5).stream()
        # docs = firestore.collection("full_fhir").limit(10).stream()

        for doc in docs: 

            # Transform into a PatientInfo object
            patient_info = firestore_doc_to_patient_info(db=firestore, doc=doc)

            # Test basic patient information retrieval and parsing from Firestore doc 
            self.assertTrue(hasattr(patient_info, "id"))
            self.assertTrue(hasattr(patient_info, "hl7v2_id"))
            self.assertTrue(hasattr(patient_info, "birth_date"))
            self.assertTrue(hasattr(patient_info, "gender"))
            self.assertTrue(hasattr(patient_info, "ssn"))
            self.assertTrue(hasattr(patient_info, "first_name"))
            self.assertTrue(hasattr(patient_info, "middle_name"))
            self.assertTrue(hasattr(patient_info, "last_name"))
            self.assertTrue(hasattr(patient_info, "address"))
            self.assertTrue(hasattr(patient_info, "address_2"))
            self.assertTrue(hasattr(patient_info, "city"))
            self.assertTrue(hasattr(patient_info, "country"))
            self.assertTrue(hasattr(patient_info, "post_code"))
            self.assertTrue(hasattr(patient_info, "country_code"))
            self.assertTrue(hasattr(patient_info, "age"))
            self.assertTrue(hasattr(patient_info, "creation_date"))
            self.assertTrue(hasattr(patient_info, "conditions"))
            self.assertTrue(hasattr(patient_info, "observations"))
            
            # Test conditions retrieval and parsing from Firestore doc
            if patient_info.conditions:
                for condition in patient_info.conditions:
                    self.assertTrue(hasattr(condition, "condition"))
                    self.assertTrue(hasattr(condition, "clinical_status"))
                    self.assertTrue(hasattr(condition, "verification_status"))
                    self.assertTrue(hasattr(condition, "onset_date_time"))
                    self.assertTrue(hasattr(condition, "recorded_date"))
                    self.assertTrue(hasattr(condition, "encounter_reference"))
                    self.assertTrue(hasattr(condition, "subject_reference"))

            # Test observations retrieval and parsing from Firestore doc
            if patient_info.observations:
                for observation in patient_info.observations:
                    self.assertTrue(hasattr(observation, "category"))
                    self.assertTrue(hasattr(observation, "observation"))
                    self.assertTrue(hasattr(observation, "status"))
                    self.assertTrue(hasattr(observation, "effective_date_time"))
                    self.assertTrue(hasattr(observation, "issued"))
                    self.assertTrue(hasattr(observation, "encounter_reference"))
                    self.assertTrue(hasattr(observation, "subject_reference"))
                    if observation.component:
                        for component in observation.component:
                            self.assertTrue(component["code_text"])
                            self.assertTrue(component["result"])

            # Print random parts of patient info

            print(patient_info)

            if patient_info.conditions:
                print(patient_info.conditions[0])
            if patient_info.observations:
                print(patient_info.observations[0])


    def test_hl7v2_id_generation(self):
        """Testing the generation of a new patient hl7v2 id 

        Will need to manually check against the database that the printed value is 
        an iteration above the highest stored id in the database. 

        """
        print(create_patient_id(db=firestore))


    def test_check_hl7v2_id_exists(self):
        """Testing the upload of patient hl7v2 ids to Firestore
        """

        # Change as necessary vvv
        id_to_check = "SYN00008^^^PAS^MR"

        query = firestore.collection("full_fhir").where(filter=FieldFilter("hl7v2_id", "==", id_to_check))
        aggregate_query = aggregation.AggregationQuery(query)
        aggregate_query.count(alias="all")

        # Get the number of patient records which fit the criteria
        results = aggregate_query.get()
        count = results[0][0].value
        if count > 0:
            print("ID is in use")
        else: 
            print("ID is not in use")


    def test_assign_multiple_hl7v2_ids(self):
        
        for file in work_folder_path.glob("*.json"):
            with open(file, "r") as f:
                fhir_message = f.read()

                # Parse patient information from file 
                patient_info = parse_fhir_message(db=firestore, fhir_message=fhir_message)

                # Assert that a list has been created 
                self.assertIsInstance(patient_info.hl7v2_id, list)

                # Add another id 
                patient_info.hl7v2_id.append(create_patient_id(db=firestore))

                # Print each id 
                for id in patient_info.hl7v2_id:
                    print(id)

                # Check that there are two ids 
                self.assertEqual(2, len(patient_info.hl7v2_id))


    def test_get_address_from_api(self):
        response = requests.get("https://my.api.mockaroo.com/address.json?key=d995a340")
        print(response.json())


if __name__ == '__main__':
    firestore = initialize_firestore()
    unittest.main()