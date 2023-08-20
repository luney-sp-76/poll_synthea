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


def run_synthea(x):
    # Command to run Synthea
    command = [
        "java",
        "-jar",
        "synthea-with-dependencies.jar",  # Assuming the JAR is in the same directory
        "-p",
        str(x),
        "--exporter.fhir.export=true",
        "--exporter.fhir.transaction_bundle=true",
        "--exporter.years_of_history=0",
        "--exporter.fhir.use_uuid_filenames=false",
        "--exporter.hospital.fhir.export=false",
        "--exporter.practitioner.fhir.export=false",
        "--exporter.fhir.use_shr_extensions=false",
        "--exporter.fhir.use_us_core_ig=false",
        "--exporter.fhir.use_us_core_r4_ig=false",
        "--exporter.fhir.use_synthea_extensions=true"

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
        try:
            check_number: int = int(user_input)
            if 0 <= check_number <= 60000:
                return check_number
            else:
                print("Please enter a non-negative number or a number no greater than 60000.")
        except ValueError:
            print(f"'{user_input}' is not a valid numeric value. Please enter a valid number.")


number_of_patients = get_valid_positive_integer_input()
run_synthea(number_of_patients)
