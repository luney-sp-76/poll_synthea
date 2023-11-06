# utilities.py
import random
import string
from datetime import date
import datetime

def create_placer_order_num():
    order_id = "".join(
        ["{}".format(random.randint(0, 9)) for _ in range(0, 3)]
    ) + "".join(
        ["{}".format(random.choice(string.ascii_uppercase)) for _ in range(0, 2)]
    )
    return order_id

def create_filler_order_num():
    # allocate a random number between 1 and 999999999
    random_number = random.randint(1, 999999999)
    # format the random number as 1^^23^4
    filler_order_id = f"1^^{random_number // 10000}^{random_number % 10000}"
    return filler_order_id

# Creates a random visit institution for the HL7 message
def create_visit_instiution():
    visit_institution = "".join(
        ["{}".format(random.randint(0, 9)) for _ in range(0, 3)]
    ) + "".join(
        ["{}".format(random.choice(string.ascii_uppercase)) for _ in range(0, 2)]
    )
    return visit_institution

# Creates a random visit number for the HL7 message
def create_visit_number():
    visit_number = "".join(["{}".format(random.randint(0, 9)) for _ in range(0, 3)])
    return visit_number

# Creates a random control ID for the HL7 message
def create_control_id():
    current_date_time = datetime.datetime.now()
    formatted_date_minutes_milliseconds = current_date_time.strftime("%Y%m%d%H%M%S.%f")
    control_id = formatted_date_minutes_milliseconds.replace(".", "")
    return control_id
