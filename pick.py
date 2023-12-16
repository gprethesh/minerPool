import json
import time
import datetime
from database import r


def pick_model_for_processing():
    try:
        # Fetch all models from the 'models' hash
        all_models = r.hgetall("models")

        # Variables to track the model with the oldest last_active_time
        oldest_last_active_time = datetime.datetime.max
        model_with_oldest_time = None
        model_with_no_last_active = None

        # List to keep track of models to be deleted
        models_to_delete = []

        for model_name, model_data in all_models.items():
            # Check and decode the model data if it's in bytes
            if isinstance(model_data, bytes):
                model_data = model_data.decode("utf-8")

            # Load the model data as JSON
            model_info = json.loads(model_data)

            # Check if the percentage is 51 or more and mark for deletion
            if float(model_info.get("percentage", 0)) >= 51:
                models_to_delete.append(model_name)
                continue

            # Check for models with last_active_time as 0
            last_active_time_str = model_info.get("last_active_time")
            if last_active_time_str == 0 or last_active_time_str == "0":
                model_with_no_last_active = model_name
                break

            # For models with less than 51 percentage and valid last_active_time
            if float(model_info.get("percentage", 100)) < 51 and isinstance(
                last_active_time_str, str
            ):
                last_active_time = datetime.datetime.fromisoformat(last_active_time_str)
                if last_active_time < oldest_last_active_time:
                    oldest_last_active_time = last_active_time
                    model_with_oldest_time = model_name

        # Delete models with 51 or more percentage
        for model in models_to_delete:
            r.hdel("models", model)

        # Determine which model to return
        selected_model = model_with_no_last_active or model_with_oldest_time

        # If a suitable model is found
        if selected_model:
            # Decode the model name if it's in bytes
            if isinstance(selected_model, bytes):
                selected_model = selected_model.decode("utf-8")

            # Update the model's last_active_time to the current timestamp in ISO format
            model_info = json.loads(r.hget("models", selected_model))
            model_info["last_active_time"] = datetime.datetime.now().isoformat()
            r.hset("models", selected_model, json.dumps(model_info))

            return selected_model

        return None  # Return None if no suitable model is found
    except Exception as e:
        print(f"An error occurred: {e}")
        return None


# Example usage
# selected_model = pick_model_for_processing()
# print("Selected model:", selected_model if selected_model else "None")
