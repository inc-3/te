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
                    # Split the name into parts (first, middle, last)
                    name_parts = name.split()

                    if len(name_parts) == 3:
                        # If the name has three parts, write first, middle, and last name separately
                        first_name = name_parts[0]
                        middle_name = name_parts[1]
                        last_name = name_parts[2]
                        outfile.write(f"{uid}|{first_name}\n")
                        outfile.write(f"{uid}|{middle_name}\n")
                        outfile.write(f"{uid}|{last_name}\n")
                    elif len(name_parts) == 2:
                        # If the name has two parts, write the first and last name separately
                        first_name = name_parts[0]
                        last_name = name_parts[1]
                        outfile.write(f"{uid}|{first_name}\n")
                        outfile.write(f"{uid}|{last_name}\n")
                    else:
                        # If there's only one part, write it as is
                        outfile.write(f"{uid}|{name}\n")

        print(f"Processing complete. Output saved to {output_file}")

    except FileNotFoundError:
        print("Error: The input file was not found.")
    except Exception as e:
        print(f"An error occurred: {e}")

# Call the function to run
process_uid_names()
