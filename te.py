from BD import bdn

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
            if len(name_parts) > 0 and any(prefix in name_parts[0] for prefix in prefixes):
                md_lines.append(line)
            else:
                non_md_lines.append(line)
        else:
            non_md_lines.append(line)

    with open(temp_file2, 'w') as outfile:
        outfile.writelines(md_lines)
    with open(temp_file1, 'w') as outfile:
        outfile.writelines(non_md_lines)

def process_names(temp_file1, temp_file3):
    with open(temp_file1, 'r') as infile, open(temp_file3, 'w') as outfile:
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

def check_bd_names(temp_file3, output_file, temp_file2):
    with open(output_file, 'a') as outfile:
        with open(temp_file3, 'r') as infile:
            filtered_data = [line for line in infile if any(name == line.strip().split("|")[1] for name in bdn)]
            outfile.writelines(filtered_data)

        with open(temp_file2, 'r') as infile2:
            for line in infile2:
                parts = line.strip().split('|')
                if len(parts) == 2:
                    uid, name = parts
                    name_parts = name.split()
                    if len(name_parts) > 0 and name_parts[0] in ["Md", "MD", "Sk", "Md.", "Mst"]:
                        name = " ".join(name_parts[1:])
                    outfile.write(f"{uid}|{name}\n")

def remove_specific_names(output_file, final_output_file):
    names_to_exclude = ["Rahman", "Rakibul"]
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

remove_duplicates(input_file, temp_file1)
separate_md_names(temp_file1, temp_file2)
process_names(temp_file1, temp_file3)
check_bd_names(temp_file3, output_file, temp_file2)
remove_specific_names(output_file, final_output_file)
