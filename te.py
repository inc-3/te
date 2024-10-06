def process_uid_names():
    # Ask user for input and output file paths
    input_file = input("Enter the input file path: ")
    output_file = input("Enter the output file path: ")
    
    try:
        with open(input_file, 'r') as infile, open(output_file, 'w') as outfile:
            for line in infile:
                # Split the UID and name parts
                parts = line.strip().split('|')

                if len(parts) == 2:
                    uid, name = parts
                    # Split the name into words (first and last)
                    name_parts = name.split()

                    # If there are more than one name parts, keep only the first name
                    if len(name_parts) > 1:
                        first_name = name_parts[1]
                        # Write the UID and first name only
                        outfile.write(f"{uid}|{first_name}\n")
                    else:
                        # If only one name (first name), write it as is
                        outfile.write(f"{uid}|{name}\n")
                        
        print(f"Processing complete. Output saved to {output_file}")
    
    except FileNotFoundError:
        print("Error: The input file was not found.")
    except Exception as e:
        print(f"An error occurred: {e}")

# Call the function to run
process_uid_names()
