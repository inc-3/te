import os
from concurrent.futures import ThreadPoolExecutor
from BD import bdn

def remove_duplicates(input_file, temp_file):
    with open(input_file, 'r') as infile, open(temp_file, 'w') as outfile:
        seen = set()
        for line in infile:
            if line not in seen:
                outfile.write(line)
                seen.add(line)

def separate_md_names(input_file, temp_file):
    prefixes = ["Md", "Md.", "MD", "Sk"]
    with open(input_file, 'r') as infile:
        lines = infile.readlines()
    md_lines = []
    non_md_lines = []

    for line in lines:
        parts = line.strip().split("|")
        if len(parts) == 2:
            uid, name = parts
            name_parts = name.split()
            if len(name_parts) > 0 and any(prefix in name_parts[0] for prefix in prefixes):
                md_lines.append(line)
            else:
                non_md_lines.append(line)
        else:
            non_md_lines.append(line)

    with open(temp_file, 'w') as outfile:
        outfile.writelines(md_lines)
    with open(input_file, 'w') as outfile:
        outfile.writelines(non_md_lines)

def process_names(input_file, temp_file):
    with open(input_file, 'r') as infile, open(temp_file, 'w') as outfile:
        for line in infile:
            parts = line.strip().split('|')
            if len(parts) == 2:
                uid, name = parts
                name_parts = name.split()
                if len(name_parts) == 3:
                    first_name, middle_name, last_name = name_parts
                    outfile.write(f"{uid}|{first_name}\n")
                    outfile.write(f"{uid}|{middle_name}\n")
                    outfile.write(f"{uid}|{last_name}\n")
                elif len(name_parts) == 2:
                    first_name, last_name = name_parts
                    outfile.write(f"{uid}|{first_name}\n")
                    outfile.write(f"{uid}|{last_name}\n")
                else:
                    outfile.write(f"{uid}|{name}\n")

def process_files(input_file, output_file):
    temp_file1 = 'temp1.txt'
    temp_file2 = 'temp2.txt'
    temp_file3 = 'temp3.txt'
    temp_file4 = 'temp4.txt'

    # Step 1: Remove duplicates (must complete before the next step)
    remove_duplicates(input_file, temp_file1)

    # Step 2: Separate MD names (must complete before processing names)
    separate_md_names(temp_file1, temp_file2)

    # Step 3: Process names (can be done in parallel with other independent tasks)
    process_names(temp_file1, temp_file3)

    # Step 4: Optimized check_bd_names and remove_specific_names
    check_bd_names(temp_file3, temp_file4, temp_file2)
    remove_specific_names(temp_file4, output_file)

    # Clean up temporary files
    os.remove(temp_file1)
    os.remove(temp_file2)
    os.remove(temp_file3)
    os.remove(temp_file4)

def check_bd_names(input_file, temp_file, temp_file2):
    bdn_set = set(bdn)

    def process_chunk(lines, output_list):
        local_output = []
        for line in lines:
            parts = line.strip().split("|")
            if len(parts) == 2:
                uid, name = parts
                if name in bdn_set:
                    local_output.append(line)
                else:
                    name_parts = name.split()
                    if len(name_parts) > 0 and name_parts[0] in ["Md", "MD", "Sk", "Md.", "Mst"]:
                        name = " ".join(name_parts[1:])
                        local_output.append(f"{uid}|{name}\n")
        output_list.extend(local_output)

    with open(input_file, 'r') as infile:
        lines = infile.readlines()

    chunk_size = max(1, len(lines) // 15)
    chunks = [lines[i:i + chunk_size] for i in range(0, len(lines), chunk_size)]
    output_data = []

    with ThreadPoolExecutor(max_workers=15) as executor:
        futures = [executor.submit(process_chunk, chunk, output_data) for chunk in chunks]
        for future in futures:
            future.result()

    with open(temp_file, 'w') as outfile:
        outfile.writelines(output_data)

    with open(temp_file2, 'r') as infile2:
        with open(temp_file, 'a') as outfile:
            for line in infile2:
                parts = line.strip().split('|')
                if len(parts) == 2:
                    uid, name = parts
                    name_parts = name.split()
                    if len(name_parts) > 0 and name_parts[0] in ["Md", "MD", "Sk", "Md.", "Mst"]:
                        name = " ".join(name_parts[1:])
                    outfile.write(f"{uid}|{name}\n")

def remove_specific_names(input_file, output_file):
    names_to_exclude = [
        "Ahmed", "Rahman", "Hossain", "Alam", "Ullah", "Uddin", "Islam", "Haque", "Siddiqui", 
        "Karim", "Chowdhury", "Ali", "Kamal", "Mahmud", "Mollah", "Bashar", "Mohammad", "Hasan"
    ]
    with open(input_file, 'r') as infile, open(output_file, 'w') as outfile:
        for line in infile:
            parts = line.strip().split('|')
            if len(parts) == 2:
                uid, name = parts
                if name not in names_to_exclude:
                    outfile.write(line)

# Input and output files
input_file = input("Input file: ")
output_file = input("Output file: ")

process_files(input_file, output_file)
