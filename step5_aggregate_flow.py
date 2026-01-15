import pandas as pd
import os
import glob

def aggregate_path_level_flow(interval_min=5):
    input_dir = "path_data"
    output_file = "final_path_flow_results.parquet"
    
    path_files = glob.glob(os.path.join(input_dir, "*_paths.parquet"))
    all_records = []

    print(f"📊 正在聚合【路径级】流量，时间步长: {interval_min} 分钟...")

    for file_path in path_files:
        df = pd.read_parquet(file_path)
        
        # 1. 转换时间窗
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df['time_window'] = df['timestamp'].dt.floor(f'{interval_min}min')

        # 2. 将列表格式的 edge_id 转换为唯一的字符串路径 (例如 "A -> B -> C")
        # 这样相同的路径序列才能被分到一组
        df['path_id'] = df['edge_id'].apply(lambda x: " -> ".join(x))

        # 3. 按【时间窗】和【唯一路径ID】计数
        path_counts = df.groupby(['time_window', 'path_id']).size().reset_index(name='path_flow_count')
        all_records.append(path_counts)

    # 4. 合并并最终汇总
    final_df = pd.concat(all_records)
    final_df = final_df.groupby(['time_window', 'path_id'])['path_flow_count'].sum().reset_index()

    # 5. 保存
    final_df.to_parquet(output_file)
    print(f"✅ 聚合完成！")
    print(f"统计出唯一路径总数: {final_df['path_id'].nunique()}")
    print(f"数据已保存至: {output_file}")

if __name__ == "__main__":
    aggregate_path_level_flow(5)