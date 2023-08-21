# poll_synthea
Dummy Patient data 

<<<<<<< HEAD
=======

This is a simple FHIR patient generation tool that works with the Synthea Project https://github.com/synthetichealth/synthea

## running the whole script

To run the script, you will need to add the synthea.jar file to the project root and install Java JDK 11 or above on your local machine

You can run the script by navigating to the project directory and running  python3 parse_fhir.py

The script is set to ask for the number of patients you would like to generate. If you have already generated patients and do not want to add more, you can enter 0.

The maximum number of patients is 60,000, although your storage may suffer.

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

=======
<<<<<<< HEAD
This is a simple patient fhir generation tool that works with the Synthea Project https://github.com/synthetichealth/synthea
=======
=======

This is a simple FHIR patient generation tool that works with the Synthea Project https://github.com/synthetichealth/synthea
>>>>>>> 1b1a35e (Update README.md)

## running the whole script

To run the script, you will need to add the synthea.jar file to the project root and install Java JDK 11 or above on your local machine

<<<<<<< HEAD
The script is set to ask for the number of patients you would like to generate
=======
You can run the script by navigating to the project directory and running  python3 parse_fhir.py

The script is set to ask for the number of patients you would like to generate. If you have already generated patients and do not want to add more, you can enter 0.

The maximum number of patients is 60,000, although your storage may suffer.

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

>>>>>>> 1b1a35e (Update README.md)
>>>>>>> main
