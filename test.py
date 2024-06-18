from main import initialize_firestore, get_firestore_age_range
from generators.utilities import PatientInfo, assign_age_to_patient, calculate_age
import unittest
import datetime


class Test(unittest.TestCase):

    def test_patient_retrieval_in_age_range(self):
        """Test to ensure patients retrieved are within the age range specified

        Keep the number of patients low to limit requests to firebase. 
        
        """

        num_of_patients = 5
        lower_bound = 50
        upper_bound = 60
        peter_pan = True

        patients: list[PatientInfo] = get_firestore_age_range(firestore, num_of_patients, lower_bound, upper_bound, peter_pan)

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
        peter_pan = True

        patients: list[PatientInfo] = get_firestore_age_range(firestore, num_of_patients, 10, 100, peter_pan)

        for patient in patients: 
            updated_patient = assign_age_to_patient(patient, new_age)
            with self.subTest(patient = patient):
                self.assertEqual(calculate_age(updated_patient.birth_date), new_age, "Should be equal")
            with self.subTest(patient = patient):
                self.assertEqual(patient.age, new_age, "Should be equal")


if __name__ == '__main__':

    firestore = initialize_firestore()
    unittest.main()