import pandas as pd
import numpy as np
import torch
import os
import glob

def build_st_features_batch():
    # --- 配置 ---
    input_dir = "path_data"
    output_dir = "model_inputs"
    num_top_paths = 50  # 选取的路径节点数量
    time_step_sec = 60  # 时间步长，60秒（可改为10秒以增加样本量）
    
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # 1. 扫描所有路径文件
    path_files = sorted(glob.glob(os.path.join(input_dir, "*_paths.parquet")))
    if not path_files:
        print("❌ 找不到路径数据，请确认 step4 已运行。")
        return
    
    print(f"🚀 开始多文件批处理，共检测到 {len(path_files)} 个片段...")

    # 2. 全局路径库构建：从所有片段中找出最频繁的 P 条路径
    print("🔍 正在扫描全局高频路径...")
    all_path_series = []
    for f in path_files:
        temp_df = pd.read_parquet(f)
        temp_df['path_tuple'] = temp_df['edge_id'].apply(tuple)
        all_path_series.append(temp_df['path_tuple'])
    
    global_paths = pd.concat(all_path_series).value_counts().head(num_top_paths).index.tolist()
    path_to_idx = {path: i for i, path in enumerate(global_paths)}
    print(f"✅ 全局路径库构建完成，节点数: {len(global_paths)}")

    # 3. 逐个文件处理，生成时空张量块
    st_chunks = []
    
    for file_path in path_files:
        file_name = os.path.basename(file_path)
        df = pd.read_parquet(file_path)
        df['path_tuple'] = df['edge_id'].apply(tuple)
        
        # 确定该片段的时间范围
        start_t = df['timestamp'].min()
        # 强制设为 15 分钟（针对 pNEUMA 无人机续航特性）
        num_steps = 15 
        
        # 初始化当前片段的张量: (Time, Nodes, Feature)
        X_chunk = np.zeros((num_steps, num_top_paths, 1))
        
        for _, row in df.iterrows():
            if row['path_tuple'] in path_to_idx:
                # 计算分钟偏移
                t_idx = int((row['timestamp'] - start_t).total_seconds() // time_step_sec)
                p_idx = path_to_idx[row['path_tuple']]
                if 0 <= t_idx < num_steps:
                    X_chunk[t_idx, p_idx, 0] += 1
        
        st_chunks.append(X_chunk)
        print(f"📦 已处理片段: {file_name} -> Tensor {X_chunk.shape}")

    # 4. 构建路径邻接矩阵 A_path (全局唯一)
    print("🕸️  正在构建路径邻接矩阵...")
    A_path = np.zeros((num_top_paths, num_top_paths))
    for i in range(num_top_paths):
        for j in range(num_top_paths):
            # 基于路径间路段重叠定义相关性
            if set(global_paths[i]) & set(global_paths[j]):
                A_path[i, j] = 1
    
    # 5. 保存结果
    # 最终保存为一个包含多个张量的列表，训练时每个张量是一个独立的序列
    final_data = {
        'x_list': st_chunks,       # List of [15, 50, 1] tensors
        'adj': A_path,             # [50, 50] matrix
        'path_labels': global_paths
    }
    
    output_path = os.path.join(output_dir, "st_batch_data.pt")
    torch.save(final_data, output_path)
    print(f"\n✨ 全部完成！结果已保存至: {output_path}")
    print(f"📊 总样本片段数: {len(st_chunks)}")

if __name__ == "__main__":
    build_st_features_batch()