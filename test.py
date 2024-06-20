from main import initialize_firestore, get_firestore_age_range, hl7_folder_path, produce_ADT_A01_from_firestore
from generators.utilities import PatientInfo, assign_age_to_patient, calculate_age, count_patient_records
import unittest, datetime, numbers, os, os.path


class Test(unittest.TestCase):

    def test_patient_retrieval_in_age_range(self):
        """Test to ensure patients retrieved are within the age range specified

        Keep the number of patients low to limit requests to firebase. 
        
        """

        num_of_patients = 4
        lower_bound = 10
        upper_bound = 20
        peter_pan = False

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
        peter_pan = False

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
        peter_pan = False

        count = count_patient_records(db=firestore, lower=lower, upper=upper, peter_pan=peter_pan)

        self.assertIsInstance(count, numbers.Number)


    def test_production_of_ADT_A01(self):
        """Testing the production of ADT_A01 messages using patient info from firestore.
        
        Function should take patient information from firestore, and use it to construct an ADT_A01. 
        
        """

        num_of_patients = 5
        lower = 10
        upper = 100
        peter_pan = False

        num_files_before = len([name for name in os.listdir(hl7_folder_path)])

        produce_ADT_A01_from_firestore(db=firestore, num_of_patients=num_of_patients, \
                                       lower=lower, upper=upper, peter_pan=peter_pan)

        # Test to see if there are 'num_of_patients' more files in the folder after function runs

        num_files_after = len([name for name in os.listdir(hl7_folder_path)])

        # IMPORTANT: this assertion may fail if files in HL7_v2 folder are not removed before running, 
        # as there is a chance files with the same name will be produced, overwriting existing HL7 files, 
        # and leaving the total number of files within the folder unchanged!
        self.assertEqual(num_files_before + num_of_patients, num_files_after)


if __name__ == '__main__':
    firestore = initialize_firestore()
    unittest.main()