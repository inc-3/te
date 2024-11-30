from BD import bdn
from concurrent.futures import ThreadPoolExecutor

def remove_duplicates(input_file, temp_file1):
    with open(input_file, 'r') as infile, open(temp_file1, 'w') as outfile:
        seen = set()
        for line in infile:
            if line not in seen:
                outfile.write(line)
                seen.add(line)

def separate_md_names(temp_file1, temp_file2):
    prefixes = ["Md", "Md.", "MD", "Sk"]
    with open(temp_file1, 'r') as infile:
        lines = infile.readlines()
    md_lines = []
    non_md_lines = []

    for line in lines:
        parts = line.strip().split("|")
        if len(parts) == 2:
            uid, name = parts
            name_parts = name.split()
            if len(name_parts) > 0 and name_parts[0] in prefixes:
                md_lines.append(line)
            else:
                non_md_lines.append(line)
        else:
            non_md_lines.append(line)

    with open(temp_file2, 'w') as md_out, open(temp_file1, 'w') as non_md_out:
        md_out.writelines(md_lines)
        non_md_out.writelines(non_md_lines)

def process_names(temp_file1, temp_file3):
    processed_lines = []
    with open(temp_file1, 'r') as infile:
        for line in infile:
            parts = line.strip().split('|')
            if len(parts) == 2:
                uid, name = parts
                name_parts = name.split()
                if len(name_parts) == 3:
                    processed_lines.append(f"{uid}|{name_parts[0]}\n")
                    processed_lines.append(f"{uid}|{name_parts[1]}\n")
                    processed_lines.append(f"{uid}|{name_parts[2]}\n")
                elif len(name_parts) == 2:
                    processed_lines.append(f"{uid}|{name_parts[0]}\n")
                    processed_lines.append(f"{uid}|{name_parts[1]}\n")
                else:
                    processed_lines.append(f"{uid}|{name}\n")
    with open(temp_file3, 'w') as outfile:
        outfile.writelines(processed_lines)

def process_file_for_bd_names(temp_file, bdn_set):
    processed_lines = []
    with open(temp_file, 'r') as infile:
        for line in infile:
            parts = line.strip().split('|')
            if len(parts) == 2:
                uid, name = parts
                if name in bdn_set:
                    processed_lines.append(line)
    return processed_lines

def process_file_for_prefix_removal(temp_file, prefixes_to_remove):
    processed_lines = []
    with open(temp_file, 'r') as infile:
        for line in infile:
            parts = line.strip().split('|')
            if len(parts) == 2:
                uid, name = parts
                name_parts = name.split()
                if len(name_parts) > 0 and name_parts[0] in prefixes_to_remove:
                    name = " ".join(name_parts[1:])
                processed_lines.append(f"{uid}|{name}\n")
    return processed_lines

def check_bd_names(temp_file3, output_file, temp_file2, prefixes_to_remove):
    bdn_set = set(bdn)

    with ThreadPoolExecutor(max_workers=60) as executor:
        future1 = executor.submit(process_file_for_bd_names, temp_file3, bdn_set)
        future2 = executor.submit(process_file_for_prefix_removal, temp_file2, prefixes_to_remove)

        processed_lines = future1.result() + future2.result()

    with open(output_file, 'w') as outfile:
        outfile.writelines(processed_lines)

def remove_specific_names(output_file, final_output_file, names_to_exclude):
    with open(output_file, 'r') as infile, open(final_output_file, 'w') as outfile:
        for line in infile:
            parts = line.strip().split('|')
            if len(parts) == 2:
                uid, name = parts
                if name not in names_to_exclude:
                    outfile.write(line)

input_file = input("Input file: ")
temp_file1 = '/sdcard/temp1.txt'
temp_file2 = '/sdcard/temp2.txt'
temp_file3 = '/sdcard/temp3.txt'
output_file = '/sdcard/y.txt'
final_output_file = '/sdcard/final_output.txt'

prefixes = {"Md", "Md.", "MD", "Sk", "Mst"}
names_to_exclude = {"Rahaman", "Rakibul"}

remove_duplicates(input_file, temp_file1)
separate_md_names(temp_file1, temp_file2)
process_names(temp_file1, temp_file3)
check_bd_names(temp_file3, output_file, temp_file2, prefixes)
remove_specific_names(output_file, final_output_file, names_to_exclude)
