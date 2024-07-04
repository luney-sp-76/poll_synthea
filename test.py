import psutil
from main import initialize_firestore, get_firestore_age_range, hl7_folder_path, produce_ADT_A01_from_firestore, produce_OML_O21_from_firestore
from generators.utilities import PatientInfo, assign_age_to_patient, calculate_age, count_patient_records, parse_HL7_message
import unittest, datetime, numbers, os, os.path
# from dummy_client import send_ORU_R01
# from tcp_server import run_tcp_server, HL7_FILE_PATH
# import time 
# from pathlib import Path
# import subprocess


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
        

if __name__ == '__main__':
    firestore = initialize_firestore()
    unittest.main()