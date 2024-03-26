import subprocess
import shutil
import os
from pathlib import Path

BASE_DIR = Path.cwd()

# Path to the fhir output directory
output_fhir_folder_path = BASE_DIR / "output/fhir"

# Path to the Work fhir folder
work_fhir_folder_path = BASE_DIR / "Work"

# Path to the metadata folder
metadata_folder_path = BASE_DIR / "output/metadata"


def run_synthea(x,age, sex):
    # Command to run Synthea
    command = [
        "java",
        "-jar",
        "synthea-with-dependencies.jar",  # Assuming the JAR is in the same directory
        "-p",
        str(x),
        "-a",
        str(age),
        "-g",
        str(sex),
        "--exporter.fhir.export=true",
        "--exporter.fhir.transaction_bundle=true",
        "--exporter.years_of_history=0",
        "--exporter.only_alive_patients=true",
        "--exporter.fhir.use_uuid_filenames=false",
        "--exporter.hospital.fhir.export=false",
        "--exporter.practitioner.fhir.export=false",
        "--exporter.fhir.use_shr_extensions=false",
        "--exporter.fhir.use_us_core_ig=false",
        "--exporter.fhir.use_us_core_r4_ig=false",
        "--exporter.fhir.use_synthea_extensions=false"

    ]
    temp_count: int = 0
    work_count: int = 0

    # Run the synthea command
    subprocess.run(command)

    # Ensure the Work folder exists
    os.makedirs(work_fhir_folder_path, exist_ok=True)

    # Check if Temp_Work/fhir folder exists
    if os.path.exists(output_fhir_folder_path):
        print("checking synthea-ouput completed successfully...")

    # Count and keep a record of number of json files in output
    for file in output_fhir_folder_path.glob("*.json"):
        if not file.exists():
            pass
        else:
            temp_count += 1

    print("output from synthea completed ✓ \n total output = ", temp_count)

    # remove extra patient files if generated
    if temp_count > x != 1:
        for files in output_fhir_folder_path.glob("*.json"):
            if not files.exists():
                pass
            else:
                while temp_count > x:
                    if not files.exists():
                        pass
                    else:
                        os.remove(files)
                    temp_count -= 1

    # Count the number of files already in the Work folder before this run
    existing_files_count = len(list(work_fhir_folder_path.glob("*.json")))

    # Copy contents of temporary fhir folder to Work fhir folder
    if os.path.exists(output_fhir_folder_path):
        for item in os.listdir(output_fhir_folder_path):
            source_path = os.path.join(output_fhir_folder_path, item)
            dest_path = os.path.join(work_fhir_folder_path, item)
            shutil.copy2(source_path, dest_path)
        print("Copied to Work folder successfully ✓")

        # Clean up temporary fhir folder
        shutil.rmtree(os.path.join(output_fhir_folder_path))
        print("Transferred files to Work folder successfully ✓")
    else:
        print("No files found in Temp_Work/fhir x")
        print("Temp_Work/fhir folder not found x")

    # Clean up output fhir folder (remove only files)
    if os.path.exists(output_fhir_folder_path):
        for item in os.listdir(output_fhir_folder_path):
            item_path = os.path.join(output_fhir_folder_path, item)
            if os.path.isfile(item_path):
                os.remove(item_path)

    # Clean up metadata folder
    if os.path.exists(metadata_folder_path):
        shutil.rmtree(metadata_folder_path)

    # count the number of files created in Work
    for file in work_fhir_folder_path.glob("*.json"):
        if file.exists():
            work_count += 1

    print("files in Work  = ", work_count)

    if work_count < temp_count:
        print("incomplete transfer missing ", temp_count - work_count, "files x")
    else:
        if work_count > temp_count:
            # Calculate the difference between the number of files in Work before and after this run
            new_files_added = work_count - existing_files_count
            print(new_files_added, " new files added to work ✓ ")
            if new_files_added > x:
                print(new_files_added - x, " files extra than requested have been added")
            else:
                if x > new_files_added:
                    print(x - new_files_added, " files less than requested have been added")

    print("Done! ✓ ")


'''Check the validity of the users patient number request to be 
numeric digit and non-alphabetical and not a negative number'''
def get_valid_positive_integer_input():
    while True:
        user_input = input("Enter the amount of patients to create:")
        if user_input == 'max' or user_input == 'MAX' or user_input == 'Max':
            return 60000
        else:
            try:
                check_number: int = int(user_input)
                if 0 <= check_number <= 60000:
                    return check_number
                else:
                    print("Please enter a non-negative number or a number no greater than 60000.")
            except ValueError:
                print(f"'{user_input}' is not a valid numeric value. Please enter a valid number.")


def check_number(num):
    check_number: int = num
    if 0 <= check_number <= 100:
        False
        return check_number
    else:
        print("Please enter a non-negative number or a number no greater than 100.")


'''define the age of patients to be created and check the validity of the users input to be'''
def get_valid_lower_positive_integer_input():
    user_input = input("Enter the lower age of patients to create:")
    try:
        number: int = int(user_input)
        return number
    except ValueError: 
        print(f"'{user_input}' is not a valid numeric value. Please enter a valid number.")


'''define the age of patients to be created and check the validity of the users input'''
def get_valid_upper_positive_integer_input():
    user_input = input("Enter the upper age of patients to create:")
    try:
        number: int = int(user_input)
        return number
    except ValueError: 
        print(f"'{user_input}' is not a valid numeric value. Please enter a valid number.")


'''define the sex of patients to be created and check the validity of the users input to be'''
def get_valid_sex_input():
    while True:
        user_input = input("Enter the Sex of patients to create:")
        if user_input.upper() == 'M' or user_input.upper() == 'F':
            return user_input.upper()
        else:
            print("Please enter M or F")
        

#TODO: Add and test sex of patient
def call_for_patients():
    number_of_patients = get_valid_positive_integer_input()
    age_from = get_valid_lower_positive_integer_input()
    age_to = get_valid_upper_positive_integer_input()
    age = f"{age_from}-{age_to}" 
    sex = get_valid_sex_input()
    print(age)
    print(sex)
    run_synthea(number_of_patients, age, sex)


#call_for_patients()