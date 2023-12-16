import torch
import os
import torch
import torch.nn as nn
import torch.optim as optim
import json
from database import r


class SimpleModel(nn.Module):
    def __init__(self):
        super(SimpleModel, self).__init__()
        self.fc = nn.Linear(10, 1)

    def forward(self, x):
        return self.fc(x)


def load_model_from_pth(model_path):
    try:
        model = SimpleModel()  # Assuming SimpleModel is your model class
        model.load_state_dict(torch.load(model_path))
        model.eval()  # Set the model to evaluation mode
        return model
    except Exception as e:
        print(f"Error loading model {e}")
        raise  # Re-raise the exception


def get_pth_files(job, jobname):
    try:
        # Define the main 'job' folder path and the specific 'jobname' folder path
        job_folder_path = os.path.join(job, jobname)

        # Check if the 'jobname' folder exists
        full_path = os.path.join(os.getcwd(), job_folder_path)
        if not os.path.exists(full_path):
            raise FileNotFoundError(f"The folder '{jobname}' does not exist.")

        # List the contents of the 'jobname' folder and find all .pth files
        folder_contents = os.listdir(full_path)
        pth_files = [item for item in folder_contents if item.endswith(".pth")]

        if not pth_files:
            raise FileNotFoundError(f"No .pth files found in the folder '{jobname}'.")

        # Create relative paths for the .pth files
        pth_file_paths = [os.path.join(job_folder_path, file) for file in pth_files]

        return pth_file_paths

    except FileNotFoundError as e:
        print(e)
        return []
    except Exception as e:
        print(f"An error occurred: {e}")
        return []


def update_model_record(model_name, new_percentage, validator_wallet):
    try:
        # Key for the 'models' hash
        key = "models"

        # Retrieve the existing record
        existing_value = r.hget(key, model_name)
        if existing_value is None:
            return f"No record found for model {model_name}"

        # Convert the JSON string back to a dictionary
        data = json.loads(existing_value)

        # Ensure 'validators' is a list, not an integer
        if not isinstance(data.get("validators"), list):
            data["validators"] = []

        # Check if the validator_wallet is already present
        if validator_wallet in data["validators"]:
            return f"Validator {validator_wallet} already exists for model {model_name}"

        # Update percentage and validators
        data["percentage"] += new_percentage
        data["validators"].append(validator_wallet)

        # Convert the data back to a JSON string and save it
        r.hset(key, model_name, json.dumps(data))

        return f"Model {model_name} record updated successfully."
    except Exception as e:
        return f"An error occurred: {e}"


def check_model_record(model_name, validator_wallet):
    try:
        # Key for the 'models' hash
        key = "models"

        # Retrieve the existing record
        existing_value = r.hget(key, model_name)
        if existing_value is None:
            return False

        # Convert the JSON string back to a dictionary
        data = json.loads(existing_value)

        # Ensure 'validators' is a list, not an integer
        if not isinstance(data.get("validators"), list):
            return False

        # Check if the validator_wallet is already present
        return validator_wallet in data["validators"]

    except Exception as e:
        print(f"An error occurred: {e}")
        return False


def create_model_record(model_name, percentage, validators):
    try:
        # Key for the 'models' hash
        key = "models"

        # Convert the data to a JSON string
        value = json.dumps(
            {"percentage": percentage, "last_active_time": 0, "validators": validators}
        )

        # Store the data in the 'models' hash under the model's name
        r.hset(key, model_name, value)

        return f"Model {model_name} record created successfully."
    except Exception as e:
        return f"An error occurred: {e}"


def model_exe(job, job_folder_path):
    try:
        pth_files = get_pth_files(job, job_folder_path)
    except Exception as e:
        print(f"Error in fetching .pth files: {e}")
        return

    models = []
    for model_path in pth_files:
        try:
            model = load_model_from_pth(model_path)
            models.append(model)
        except Exception as e:
            print(f"Error loading model from {model_path}: {e}")
            continue  # Skip this file and continue with the next

    if not models:
        print("No models loaded.")
        return

    print("models", models)

    final_folder = "./Models"
    combined_model = SimpleModel()

    for model in models:
        try:
            combined_model.load_state_dict(model.state_dict(), strict=False)
        except Exception as e:
            print(f"Error combining model states: {e}")
            return

    try:
        os.makedirs(final_folder, exist_ok=True)
        combined_model_save_path = os.path.join(final_folder, f"{job_folder_path}.pth")
        torch.save(combined_model.state_dict(), combined_model_save_path)
        print(f"Combined model saved to {combined_model_save_path}")
        create_model_record(combined_model_save_path, 0, [])
        return True, combined_model_save_path
    except Exception as e:
        print(f"Error saving combined model: {e}")


# model_exe("Job", "jobInode2988473")
