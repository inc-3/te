from concurrent.futures import ThreadPoolExecutor
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

def check_bd_names(temp_file3, temp_file4, temp_file2):
    bdn_set = set(bdn)  # Convert to set for faster lookups

    def process_chunk(lines, result):
        chunk_result = []
        for line in lines:
            parts = line.strip().split("|")
            if len(parts) == 2:
                uid, name = parts
                if name in bdn_set:
                    chunk_result.append(line)
                else:
                    name_parts = name.split()
                    if len(name_parts) > 0 and name_parts[0] in ["Md", "MD", "Sk", "Md.", "Mst"]:
                        name = " ".join(name_parts[1:])
                        chunk_result.append(f"{uid}|{name}\n")
        result.extend(chunk_result)

    with open(temp_file3, 'r') as infile:
        lines = infile.readlines()

    chunk_size = max(1, len(lines) // 15)  # Divide into 15 chunks
    chunks = [lines[i:i + chunk_size] for i in range(0, len(lines), chunk_size)]
    results = []

    # Process chunks in parallel
    with ThreadPoolExecutor(max_workers=15) as executor:
        futures = [executor.submit(process_chunk, chunk, results) for chunk in chunks]
        for future in futures:
            future.result()  # Ensure all tasks are complete

    # Write processed data to temp_file4
    with open(temp_file4, 'w') as outfile:
        outfile.writelines(results)

    # Process temp_file2 sequentially
    with open(temp_file2, 'r') as infile2:
        with open(temp_file4, 'a') as outfile:
            for line in infile2:
                parts = line.strip().split('|')
                if len(parts) == 2:
                    uid, name = parts
                    name_parts = name.split()
                    if len(name_parts) > 0 and name_parts[0] in ["Md", "MD", "Sk", "Md."]:
                        name = " ".join(name_parts[1:])
                    outfile.write(f"{uid}|{name}\n")

def remove_specific_names(temp_file4, final_output_file):
    names_to_exclude = [
        "Ahmed", "Rahman", "Hossain", "Alam", "Ullah", "Uddin", "Islam", "Haque",
        "Siddiqui", "Karim", "Chowdhury", "Ali", "Kamal", "Mahmud", "Mollah",
        "Bashar", "Mohammad", "Hasan"
    ]
    with open(temp_file4, 'r') as infile, open(final_output_file, 'w') as outfile:
        for line in infile:
            parts = line.strip().split('|')
            if len(parts) == 2:
                uid, name = parts
                if name not in names_to_exclude:
                    outfile.write(line)

# Main script
input_file = input("Input file: ")
temp_file1 = '/sdcard/temp1.txt'
temp_file2 = '/sdcard/temp2.txt'
temp_file3 = '/sdcard/temp3.txt'
temp_file4 = '/sdcard/temp4.txt'
final_output_file = input("Output file: ")

# Processing steps
remove_duplicates(input_file, temp_file1)
separate_md_names(temp_file1, temp_file2)
process_names(temp_file1, temp_file3)
check_bd_names(temp_file3, temp_file4, temp_file2)
remove_specific_names(temp_file4, final_output_file)
