import os
import csv

source_folder = r"D:\Code\READ_PH_WEB"  
target_folder = r"D:\Code\READ_PH_WEB\Converted_CSV"  

if not os.path.exists(target_folder):
    os.makedirs(target_folder)

for filename in os.listdir(source_folder):
    if filename.endswith(".dat"):  
        dat_path = os.path.join(source_folder, filename)
        clean_filename = filename.replace(" ", "_").replace(".dat", ".csv")
        csv_path = os.path.join(target_folder, clean_filename)

        with open(dat_path, 'r', encoding='utf-8') as dat_file, \
             open(csv_path, 'w', newline='', encoding='utf-8') as csv_file:
            # 用 CSV 阅读器自动处理引号和分隔符
            reader = csv.reader(dat_file, delimiter=',', quotechar='"')
            writer = csv.writer(csv_file)
            
            rows = list(reader)  # 读取所有行
            
            # 提取第二行作为表头
            header = rows[1] if len(rows) > 1 else []
            # 从第五行开始加载数据
            data_rows = rows[4:] if len(rows) > 4 else []
            
            if header:
                writer.writerow(header)

            for row in data_rows:
                writer.writerow(row)

print("=== 转换完成 ===")