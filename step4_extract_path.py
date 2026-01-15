import pandas as pd
import os
import glob
from tqdm import tqdm

def extract_path_sequences():
    input_dir = "matched_data"
    output_dir = "path_data"
    
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    matched_files = glob.glob(os.path.join(input_dir, "*_matched.parquet"))
    
    print(f"🚀 开始提取路径序列，共 {len(matched_files)} 个文件...")

    for file_path in matched_files:
        file_name = os.path.basename(file_path)
        df = pd.read_parquet(file_path)

        # 1. 确保按时间和车辆排序
        df = df.sort_values(by=['track_id', 'timestamp'])

        # 2. 核心逻辑：去除连续重复的路段
        # 比如：[A, A, A, B, B, C, C, C] -> [A, B, C]
        # shift() 函数可以对比当前行与上一行是否一致
        df['edge_changed'] = df['edge_id'] != df.groupby('track_id')['edge_id'].shift()
        
        # 只保留 edge_id 发生变化的行
        paths_df = df[df['edge_changed']].copy()

        # 3. 按车辆分组，将路段 ID 聚合成一个列表（即路径）
        # 同时保留进入该路段的最早时间（用于后续时间窗口聚合）
        path_results = paths_df.groupby('track_id').agg({
            'edge_id': lambda x: list(x),
            'timestamp': 'first'  # 记录这趟行程的开始时间
        }).reset_index()

        # 4. 过滤掉过短的路径（比如只在 1 个路段上晃悠的，不算“路径”）
        path_results['path_len'] = path_results['edge_id'].apply(len)
        path_results = path_results[path_results['path_len'] >= 2]

        # 5. 保存结果
        output_file = os.path.join(output_dir, file_name.replace("_matched", "_paths"))
        path_results.to_parquet(output_file)
        print(f"✅ 已保存: {os.path.basename(output_file)} (包含 {len(path_results)} 条有效路径)")

if __name__ == "__main__":
    extract_path_sequences()