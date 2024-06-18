from main import initialize_firestore, get_firestore_age_range
from generators.utilities import PatientInfo, assign_age_to_patient, calculate_age
import unittest


class Test(unittest.TestCase):

    def test_patient_retrieval_in_age_range(self):

        num_of_patients = 10
        lower_bound = 20
        upper_bound = 40

        patients: list[PatientInfo] = get_firestore_age_range(firestore, num_of_patients, lower_bound, upper_bound)

        for patient in patients: 
            with self.subTest(patient = patient):
                self.assertTrue(lower_bound <= patient.age <= upper_bound, "Should be within given range")
            with self.subTest(patient = patient):
                self.assertEqual(calculate_age(patient.birth_date), patient.age, "Should be equal")


    def test_assign_patient_age(self):

        num_of_patients = 100
        new_age = 20

        patients: list[PatientInfo] = get_firestore_age_range(firestore, num_of_patients, 10, 100)

        for patient in patients: 
            updated_patient = assign_age_to_patient(patient, new_age)
            with self.subTest(patient = patient):
                self.assertEqual(calculate_age(updated_patient.birth_date), new_age, "Should be equal")
            with self.subTest(patient = patient):
                self.assertEqual(patient.age, new_age, "Should be equal")


if __name__ == '__main__':

    firestore = initialize_firestore()
    unittest.main()

    