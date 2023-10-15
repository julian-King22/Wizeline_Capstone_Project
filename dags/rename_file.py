import glob
import os

def rename_file(directory, pattern):

  matching_files = glob.glob(os.path.join(directory, pattern))

  for file_path in matching_files:
      # Extract the current file name
      current_file_name = os.path.basename(file_path)
      
      # Create the new file name (e.g., replacing "old_" with "new_")
      new_file_name = "classified_movie_reviews.csv"

      # Construct the new file path
      new_file_path = os.path.join(directory, new_file_name)
      
      # Rename the file
      os.rename(file_path, new_file_path)
      
      print(f"Renamed: {file_path} to {new_file_path}")

      return True
