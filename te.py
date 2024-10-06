from BD import bdn

# Step 1: Remove duplicates from the data
def remove_duplicates(input_file, temp_file1):
    with open(input_file, 'r') as infile, open(temp_file1, 'w') as outfile:
        seen = set()
        for line in infile:
            if line not in seen:
                outfile.write(line)
                seen.add(line)

# Step 2: Separate Md names into tempfile1
def separate_md_names(temp_file1, temp_file2):
    prefixes = ["Md", "Md.", "MD", "Sk"]
    with open(temp_file1, 'r') as infile, open(temp_file2, 'w') as outfile:
        md_lines = [line for line in infile if any(prefix in line.split("|")[1].split()[0] for prefix in prefixes)]
        outfile.writelines(md_lines)

# Step 3: Process names and save in temp file 2
def process_names(temp_file2, temp_file3):
    with open(temp_file2, 'r') as infile, open(temp_file3, 'w') as outfile:
        for line in infile:
            parts = line.strip().split('|')
            if len(parts) == 2:
                uid, name = parts
                name_parts = name.split()

                if len(name_parts) == 3:
                    # Split into first, middle, and last name
                    first_name, middle_name, last_name = name_parts
                    outfile.write(f"{uid}|{first_name}\n")
                    outfile.write(f"{uid}|{middle_name}\n")
                    outfile.write(f"{uid}|{last_name}\n")
                elif len(name_parts) == 2:
                    # Split into first and last name
                    first_name, last_name = name_parts
                    outfile.write(f"{uid}|{first_name}\n")
                    outfile.write(f"{uid}|{last_name}\n")
                else:
                    # Only one name part
                    outfile.write(f"{uid}|{name}\n")

# Step 4: Check BD name in tempfile 3 (Modify this to fit your BD name checking logic)
def check_bd_names(temp_file3, output_file):
    # List of BD names to keep
    names_to_keep = ["Abdul", "Akhtar"]
    
    with open(temp_file3, 'r') as infile, open(output_file, 'w') as outfile:
        filtered_data = [line for line in infile if any(name == line.strip().split("|")[1] for name in bdn)]
        outfile.writelines(filtered_data) # Save only BD names

# Main logic
input_file = '/sdcard/1.txt'
temp_file1 = '/sdcard/temp1.txt'
temp_file2 = '/sdcard/temp2.txt'
temp_file3 = '/sdcard/temp3.txt'
output_file = '/sdcard/y.txt'

# Execute steps
remove_duplicates(input_file, temp_file1)
separate_md_names(temp_file1, temp_file2)
process_names(temp_file2, temp_file3)
check_bd_names(temp_file3, output_file)
