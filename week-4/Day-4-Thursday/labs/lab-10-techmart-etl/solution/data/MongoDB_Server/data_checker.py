import json
from collections import defaultdict

def validate_json(file_path):
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)

        # Check if data is a list
        if not isinstance(data, list):
            print("The JSON data should be a list of customer profiles.")
            return

        email_count = defaultdict(int)
        null_emails = 0

        for profile in data:
            email = profile.get('personal_info', {}).get('email')
            if email is None:
                null_emails += 1
                # Optionally replace null emails with a placeholder
                profile['personal_info']['email'] = 'placeholder@example.com'
            else:
                email_count[email] += 1

        # Print the first few entries to verify JSON structure
        print("First few entries in JSON data:")
        for profile in data[:3]:
            print(profile)

        # Report null emails and confirm replacement
        if null_emails > 0:
            print(f"Found {null_emails} profiles with null email addresses. Replaced with placeholder.")
            print("Sample profile with replaced email:", data[0])

        # Report duplicate emails
        duplicates = {email: count for email, count in email_count.items() if count > 1}
        if duplicates:
            print("Duplicate email addresses found:")
            for email, count in duplicates.items():
                print(f"Email: {email}, Count: {count}")
        else:
            print("No duplicate email addresses found.")

        # Check for duplicate customer_ids
        customer_id_count = defaultdict(int)
        for profile in data:
            customer_id = profile.get('customer_id')
            if customer_id is not None:
                customer_id_count[customer_id] += 1

        # Report duplicate customer_ids
        duplicate_customer_ids = {cid: count for cid, count in customer_id_count.items() if count > 1}
        if duplicate_customer_ids:
            print("Duplicate customer IDs found:")
            for cid, count in duplicate_customer_ids.items():
                print(f"Customer ID: {cid}, Count: {count}")
        else:
            print("No duplicate customer IDs found.")

        # Optionally save the modified data back to the file
        with open(file_path, 'w') as f:
            json.dump(data, f, indent=4)

    except Exception as e:
        print(f"An error occurred: {e}")

# Path to your JSON file
json_file_path = 'customer_profiles.json'
validate_json(json_file_path)