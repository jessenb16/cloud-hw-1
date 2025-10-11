import json
import datetime
import uuid
import boto3
import traceback
import os

# Initialize Lex client
lex_client = boto3.client("lexv2-runtime", region_name="us-east-1")

# Environment variables
BOT_ID = os.environ.get("LEX_BOT_ID")
BOT_ALIAS_ID = os.environ.get("LEX_BOT_ALIAS_ID")
LOCALE_ID = os.environ.get("LEX_LOCALE_ID", "en_US")

def lambda_handler(event, context):
    headers = {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Headers": "Content-Type",
        "Access-Control-Allow-Methods": "OPTIONS,POST"
    }

    # Handle preflight CORS
    if event.get("httpMethod") == "OPTIONS":
        return {"statusCode": 200, "headers": headers, "body": ""}

    try:
        # Parse the request body
        body = json.loads(event.get("body", "{}"))
        user_message = body["messages"][0]["unstructured"]["text"]
        print("User message:", user_message)

        # 👇 Reuse a fixed session to keep context
        session_id = "test-session"

        # Call Lex
        lex_response = lex_client.recognize_text(
            botId=BOT_ID,
            botAliasId=BOT_ALIAS_ID,
            localeId=LOCALE_ID,
            sessionId=session_id,
            text=user_message
        )
        print("Lex response:", lex_response)

        # Extract the Lex reply text
        reply_text = "No response from Lex."
        if lex_response.get("messages"):
            reply_text = " ".join([m.get("content") for m in lex_response["messages"]])

        # Construct frontend-compatible response
        bot_response = {
            "messages": [
                {
                    "type": "unstructured",
                    "unstructured": {
                        "id": str(uuid.uuid4()),
                        "text": reply_text,
                        "timestamp": datetime.datetime.now().isoformat()
                    }
                }
            ]
        }

    except Exception as e:
        print("Exception:", e)
        traceback.print_exc()
        bot_response = {
            "messages": [
                {
                    "type": "unstructured",
                    "unstructured": {
                        "id": str(uuid.uuid4()),
                        "text": "Sorry, something went wrong in Lambda.",
                        "timestamp": datetime.datetime.now().isoformat()
                    }
                }
            ]
        }

    # Return API Gateway response
    return {
        "statusCode": 200,
        "headers": headers,
        "body": json.dumps(bot_response)
    }