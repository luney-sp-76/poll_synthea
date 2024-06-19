from main import initialize_firestore, get_firestore_age_range
from generators.utilities import PatientInfo, assign_age_to_patient, calculate_age, count_patient_records
import unittest
import datetime
from google.cloud.firestore_v1.base_query import FieldFilter
from google.cloud.firestore_v1 import aggregation
import numbers


class Test(unittest.TestCase):

    def test_patient_retrieval_in_age_range(self):
        """Test to ensure patients retrieved are within the age range specified

        Keep the number of patients low to limit requests to firebase. 
        
        """

        num_of_patients = 4
        lower_bound = 200
        upper_bound = 220
        peter_pan = False

        patients: list[PatientInfo] = get_firestore_age_range(firestore, num_of_patients, lower_bound, upper_bound, peter_pan)

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


    def test_assign_patient_age(self):
        """Test to ensure patients are correctly assigned their new age, along with a valid dob. 
        
        Keep the number of patients low to limit number of requests to firebase. 
        
        """

        num_of_patients = 5
        new_age = 20
        peter_pan = False

        patients: list[PatientInfo] = get_firestore_age_range(firestore, num_of_patients, 10, 100, peter_pan)

        if patients:
            for patient in patients: 
                updated_patient = assign_age_to_patient(patient, new_age)
                with self.subTest(patient = patient):
                    self.assertEqual(calculate_age(updated_patient.birth_date), new_age, "Should be equal")
                with self.subTest(patient = patient):
                    self.assertEqual(patient.age, new_age, "Should be equal")


    def test_count_docs_in_firestore(self):
        """Testing producing a count of all docs that fit certain criteria
        
        This will be used prior to streaming patient data, to prevent requests being sent for 
        more patients than those that exist within the database.
        
        """

        upper = 20
        lower = 10
        peter_pan = False

        count = count_patient_records(firestore, upper, lower, peter_pan)

        self.assertIsInstance(count, numbers.Number)


if __name__ == '__main__':
    firestore = initialize_firestore()
    unittest.main()