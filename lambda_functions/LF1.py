import json
import boto3
from datetime import datetime

# ----------- AWS clients ----------- #
sqs = boto3.client('sqs', region_name='us-east-1')
QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/244086559221/Dining-Email"

# ----------- Constants ----------- #
ALLOWED_CUISINES = ["Italian", "Chinese", "Mexican", "Indian", "Japanese"]

# ----------- Helper Functions ----------- #
def push_to_sqs(data):
    print("Sending to SQS:", json.dumps(data))
    response = sqs.send_message(
        QueueUrl=QUEUE_URL,
        MessageBody=json.dumps(data)
    )
    print("SQS response:", response)

def close(fulfillment_state, message, event):
    intent = event["sessionState"]["intent"]
    return {
        "sessionState": {
            "dialogAction": {"type": "Close"},
            "intent": {
                "name": intent["name"],
                "state": fulfillment_state
            }
        },
        "messages": [
            {
                "contentType": "PlainText",
                "content": message
            }
        ]
    }

def elicit_slot(event, slot_to_elicit, message):
    intent = event["sessionState"]["intent"]
    return {
        "sessionState": {
            "dialogAction": {
                "type": "ElicitSlot",
                "slotToElicit": slot_to_elicit
            },
            "intent": {
                "name": intent["name"],
                "slots": intent["slots"],
                "state": "InProgress"
            }
        },
        "messages": [
            {
                "contentType": "PlainText",
                "content": message
            }
        ]
    }

def get_slot_value(slots, slot_name):
    slot = slots.get(slot_name)
    if slot and "value" in slot and "interpretedValue" in slot["value"]:
        return slot["value"]["interpretedValue"]
    return None

# ----------- Validation Functions ----------- #
def is_valid_email(email):
    return email and "@" in email

def is_valid_number(num):
    try:
        return int(num) > 0
    except:
        return False

def is_valid_cuisine(cuisine):
    try:
        cuisine = cuisine.strip().capitalize()
        return cuisine in ALLOWED_CUISINES
    except:
        return False

def is_valid_date(date_str):
    try:
        date_obj = datetime.strptime(date_str, "%Y-%m-%d")
        return date_obj.date() >= datetime.today().date()
    except:
        return False

def is_valid_time(time_str):
    try:
        datetime.strptime(time_str, "%H:%M")
        return True
    except:
        return False

# ----------- Lambda Handler ----------- #
def lambda_handler(event, context):
    print("EVENT:", json.dumps(event, indent=2))
    intent_name = event["sessionState"]["intent"]["name"]
    invocation_source = event.get("invocationSource")
    slots = event["sessionState"]["intent"]["slots"]

    if intent_name == "DiningSuggestionsIntent":
        # Extract slot values
        location = get_slot_value(slots, "Location")
        cuisine = get_slot_value(slots, "Cuisine")
        dining_date = get_slot_value(slots, "DiningDate")
        dining_time = get_slot_value(slots, "DiningTime")
        num_people = get_slot_value(slots, "NumberOfPeople")
        email = get_slot_value(slots, "Email")

        # ✅ Only validate if Lex is still collecting slots
        if invocation_source == "DialogCodeHook":

            if cuisine and not is_valid_cuisine(cuisine):
                return elicit_slot(event, "Cuisine",
                    f"Sorry, I don’t know that one. Try one of these cuisines: {', '.join(ALLOWED_CUISINES)}.")

            if email and not is_valid_email(email):
                return elicit_slot(event, "Email", "That doesn’t look like a valid email. Please try again.")

            if dining_date and not is_valid_date(dining_date):
                return elicit_slot(event, "DiningDate", "That date looks invalid or in the past. Try another one.")

            if dining_time and not is_valid_time(dining_time):
                return elicit_slot(event, "DiningTime", "That time format looks wrong. Use HH:MM (24-hour).")

            if num_people and not is_valid_number(num_people):
                return elicit_slot(event, "NumberOfPeople", "Please enter a number greater than 0.")

            # ✅ All good so far — tell Lex to keep going normally
            return {
                "sessionState": {
                    "dialogAction": {"type": "Delegate"},
                    "intent": event["sessionState"]["intent"]
                }
            }

        # ✅ Once Lex calls FulfillmentCodeHook (final step)
        elif invocation_source == "FulfillmentCodeHook":
            if cuisine:
                cuisine = cuisine.strip().lower()
            data = {
                "location": location,
                "cuisine": cuisine,
                "dining_time": dining_time,
                "dining_date": dining_date,
                "num_people": num_people,
                "email": email
            }
            push_to_sqs(data)
            message = "Thanks! We’ve received your request. You’ll get restaurant suggestions soon."
            return close("Fulfilled", message, event)