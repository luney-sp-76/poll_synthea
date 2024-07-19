from pathlib import Path
from main import initialize_firestore, get_firestore_age_range, hl7_folder_path, produce_ADT_A01_from_firestore, \
    produce_OML_O21_from_firestore
from generators.utilities import PatientCondition, PatientInfo, PatientObservation, \
    assign_age_to_patient, calculate_age, count_patient_records, parse_fhir_message, save_to_firestore, \
        firestore_doc_to_patient_info
import unittest, datetime, numbers, os, os.path
from poll_synthea import call_for_patients
from google.cloud.firestore_v1.base_query import FieldFilter

BASE_DIR = Path.cwd()
work_folder_path = BASE_DIR / "Work"

class Test(unittest.TestCase):

    def test_patient_retrieval_in_age_range(self):
        """Test to ensure patients retrieved are within the age range specified

        Keep the number of patients low to limit requests to firebase. 
        
        """

        num_of_patients = 4
        lower_bound = 10
        upper_bound = 20
        peter_pan = True

        patients: list[PatientInfo] = get_firestore_age_range(db=firestore, num_of_patients=num_of_patients, \
                                                              lower=lower_bound, upper=upper_bound, peter_pan=peter_pan)

        # If request is granted...
        if patients:
            for patient in patients: 
                with self.subTest(patient = patient):
                    self.assertTrue(lower_bound <= patient.age <= upper_bound, "Should be within given range")
                
                if peter_pan and patient.creation_date:
                    dob = patient.birth_date
                else: 
                    dob = datetime.datetime.strptime(patient.birth_date, "%Y-%m-%d").date()

                with self.subTest(patient = patient):
                    self.assertEqual(calculate_age(dob), patient.age, "Should be equal")
                with self.subTest(patient=patient):
                    self.assertTrue(lower_bound <= calculate_age(dob) <= upper_bound, "Should be within given range")
        else:
            print("Not enough patients fit the criteria given...")


    def test_assign_patient_age(self):
        """Test to ensure patients are correctly assigned their new age, along with a valid dob. 
        
        Keep the number of patients low to limit number of requests to firebase. 
        
        """

        num_of_patients = 5
        new_age = 20
        peter_pan = True

        patients: list[PatientInfo] = get_firestore_age_range(db=firestore, num_of_patients=num_of_patients, \
                                                              lower=10, upper=100, peter_pan=peter_pan)

        if patients:
            for patient in patients: 
                updated_patient = assign_age_to_patient(patient_info=patient, desired_age=new_age)
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

        num_of_patients = 5
        lower = 10
        upper = 100
        peter_pan = True

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
        num_of_patients = 30
        lower = 23
        upper = 24
        peter_pan = False
        patients = None 

        patients = get_firestore_age_range(firestore, num_of_patients, lower, upper, peter_pan)

        for patient in patients: 
            print (patient.first_name)

        self.assertEqual(len(patients), num_of_patients)


    def test_condition_parsing(self):
        """ Tests the creation of the condition attribute within a ``PatientInfo`` object 
        using the ``PatientCondition`` class. 

        Requires at least one fhir doc in the ``Work`` folder. 
        """
        for file in work_folder_path.glob("*.json"):
            with open(file, "r") as f:
                fhir_message = f.read()

                # Parse patient information from file 
                patient_info = parse_fhir_message(fhir_message)

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
                patient_info = parse_fhir_message(fhir_message)

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
            with open(file, "r") as f:
                fhir_message = f.read()

                # Parse patient information from file 
                patient_info = parse_fhir_message(fhir_message)

                save_to_firestore(db=firestore, patient_info=patient_info)


    def test_retrieval_of_conditions_observations(self):
        """Testing the retrieval and parsing of patient information from Firestore 
        docs, including patient conditions and observations. 

        To retrieve only docs with both conditions and observations fields, use the retrieval 
        with creation date 2024-07-17.

        To instead retrieve a number of docs which may or may not contain conditions and/or observations, 
        use the retrieval with a limit, and set the limit to the desired number of docs to test. 
        """
        # Retrieve docs from Firestore
        
        docs = firestore.collection("full_fhir").where(filter=FieldFilter("creation_date", "==", "2024-07-17")).stream()
        # docs = firestore.collection("full_fhir").limit(10).stream()

        for doc in docs: 

            # Transform into a PatientInfo object
            patient_info = firestore_doc_to_patient_info(doc=doc)

            # Test basic patient information retrieval and parsing from Firestore doc 
            self.assertTrue(hasattr(patient_info, "id"))
            self.assertTrue(hasattr(patient_info, "birth_date"))
            self.assertTrue(hasattr(patient_info, "gender"))
            self.assertTrue(hasattr(patient_info, "ssn"))
            self.assertTrue(hasattr(patient_info, "first_name"))
            self.assertTrue(hasattr(patient_info, "middle_name"))
            self.assertTrue(hasattr(patient_info, "last_name"))
            self.assertTrue(hasattr(patient_info, "city"))
            self.assertTrue(hasattr(patient_info, "state"))
            self.assertTrue(hasattr(patient_info, "country"))
            self.assertTrue(hasattr(patient_info, "postal_code"))
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


if __name__ == '__main__':
    firestore = initialize_firestore()
    unittest.main()