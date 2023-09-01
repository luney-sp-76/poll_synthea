# poll_synthea
Dummy Patient data 

This is a simple FHIR patient/ HL7 v2 generation tool that works with the Synthea Project https://github.com/synthetichealth/synthea

## running the whole script

To run the script, you will need to add the synthea.jar file to the project root and install Java JDK 11 or above on your local machine

You can run the script by navigating to the project directory and running  ```python main.py```

The script is set to ask for the number of patients you would like to generate. If you have already generated patients and do not want to add more, you can enter 0.

The maximum number of patients is 60,000, although your storage may suffer.

This will generate the patients and upload them to a Firestore database. The database is set to the default project in the firebase folder. You can change this by updating the code in main.py to point to your project.

The script will also generate a basic HL7 v2 messages OMLO01 for each patient and adds them to the HL7_v2 folder. You can change this by updating the code in main.py to point to your project.

# Prerequisites
The program assumes you have a Firestore database with a collection called full_fhir and the following document attributes:

id: String
birth_date: Timestamp
gender: String
ssn: String
first_name: String
last_name: String
city: String
state: String
country: String
postal_code: String
age: Integer

You will need to create a 'firebase' folder in the project root and update the code to name the credentials .json file accordingly.

# Alternative
You can run the poll_synthea alone by running - python3 poll_synthea.call_for_patients()

This will output the FHIR .json files to the Work folder and not upload patients to Firestore.

