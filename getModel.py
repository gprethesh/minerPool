import os
import time


def find_model(destination, file_name):
    try:
        destination_path = os.path.join(os.getcwd(), destination)

        # Check if the 'destination' folder exists
        if not os.path.exists(destination_path):
            raise FileNotFoundError(f"The '{destination}' folder does not exist.")

        # Construct the full path of the file
        file_path = os.path.join(destination_path, file_name)

        # Check if the file exists
        if not os.path.isfile(file_path):
            raise FileNotFoundError(
                f"The file '{file_name}' does not exist in the '{destination}' folder."
            )

        return file_path

    except FileNotFoundError as e:
        print(e)
        return None
    except Exception as e:
        print(f"An error occurred: {e}")
        return None


file_path = find_model("Models", "jobInode2988473.pth")

if file_path:
    print("File found at:", file_path)
else:
    print("File not found.")
