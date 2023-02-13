#  Copyright 2022 Exactpro (Exactpro Systems Limited)
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.import os
import shutil

def add_text_to_file(file_path, text_path):
    # Check if file is a python script
    if not file_path.endswith('.py'):
        return
    
    # Read the text to be added from the argument file
    with open(text_path, 'r') as text_file:
        text = text_file.read()
    
    # Read the contents of the target file
    with open(file_path, 'r') as target_file:
        target = target_file.read()
    
    # Check if the text is already present in the target file
    if not target.startswith(text):
        # If not, add the text to the target file
        with open(file_path, 'w') as target_file:
            target_file.write(text + target)
        print("Updating " + file_path)

def add_text_to_files_in_dir(dir_path, text_path):
    # Loop through all items in the directory
    for item in os.listdir(dir_path):
        item_path = os.path.join(dir_path, item)
        # If the item is a directory, call the function recursively
        if os.path.isdir(item_path):
            add_text_to_files_in_dir(item_path, text_path)
        else:
            add_text_to_file(item_path, text_path)

if __name__ == '__main__':
    import sys
    text_path = sys.argv[1]
    dir_path = sys.argv[2]
    add_text_to_files_in_dir(dir_path, text_path)
