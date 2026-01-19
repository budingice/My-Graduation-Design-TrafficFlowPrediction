import pandas as pd
import os

# 加载一个生成好的路径文件
path_file = "path_data/20181024_d1_0830_0900_paths.parquet" # 替换为你的文件名
df_path = pd.read_parquet(path_file)

# 1. 查看基础统计信息
print(f"总计提取路径数: {len(df_path)}")
print(f"平均路径长度 (路段数): {df_path['path_len'].mean():.2f}")

# 2. 抽样查看具体路径内容
# 期望看到结果如: ['edge_1', 'edge_2', 'edge_3']
sample = df_path.sample(3)
for _, row in sample.iterrows():
    print(f"\n车辆 ID: {row['track_id']}")
    print(f"开始时间: {row['timestamp']}")
    print(f"路径序列: {row['edge_id']}")