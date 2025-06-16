import pandas as pd
import pymysql
from datetime import datetime

# 数据库配置
config = {
    'host': 'localhost',
    'user': 'root',
    'password': '123456',
    'database': 'ph_monitor',
    'charset': 'utf8mb4'
}

# 读取 CSV 并清洗数据
try:
    df = pd.read_csv('processed_data.csv', skiprows=1)  # 跳过表头
    
    # 解析时间列
    df['TIMESTAMP'] = pd.to_datetime(df['TIMESTAMP'], errors='coerce')
    
    # 删除时间或RECORD列包含NaN的行
    df = df.dropna(subset=['TIMESTAMP', 'RECORD'])
    
    # 确保RECORD是整数类型
    df['RECORD'] = df['RECORD'].astype(int)
    
    # 处理Measure_4列：将字符串形式的"NAN"或"NaN"替换为NaN值
    measure_cols = ['Measure_1', 'Measure_2', 'Measure_3', 'Measure_4']
    for col in measure_cols:
        if col in df.columns:
            # 将字符串"NAN"或"NaN"替换为NaN值
            df[col] = df[col].replace(['NAN', 'NaN', 'nan'], pd.NA)
            # 尝试转换为浮点数，无法转换的值设为NaN
            df[col] = pd.to_numeric(df[col], errors='coerce')
            # 填充NaN值为0
            df[col] = df[col].fillna(0)
    
    print(f"成功读取 CSV 文件，共 {len(df)} 行有效数据")
except FileNotFoundError:
    print("错误：找不到 processed_data.csv 文件")
    exit(1)
except Exception as e:
    print(f"CSV 数据清洗失败：{e}")
    exit(1)

# 连接数据库
try:
    conn = pymysql.connect(**config)
    cursor = conn.cursor()
    print("成功连接到 MySQL 数据库")
except pymysql.Error as e:
    print(f"数据库连接失败：{e}")
    exit(1)

# 插入数据
insert_sql = """
INSERT INTO sensor_data (TIMESTAMP, RECORD, Measure_1, Measure_2, Measure_3, Measure_4)
VALUES (%s, %s, %s, %s, %s, %s)
"""

success_count = 0
error_count = 0

try:
    for idx, row in df.iterrows():
        try:
            cursor.execute(insert_sql, (
                row['TIMESTAMP'],
                row['RECORD'],
                row['Measure_1'],
                row['Measure_2'],
                row['Measure_3'],
                row['Measure_4']
            ))
            success_count += 1
        except Exception as e:
            error_count += 1
            # 打印详细的错误行信息，帮助调试
            print(f"插入行 {idx+1} 失败：{e}")
            print(f"失败行数据: {row.to_dict()}")
    
    conn.commit()
    print(f"✅ 数据导入完成！成功: {success_count} 行, 失败: {error_count} 行")

except Exception as e:
    conn.rollback()
    print(f"导入过程中发生错误：{e}")

finally:
    cursor.close()
    conn.close()
    print("数据库连接已关闭")