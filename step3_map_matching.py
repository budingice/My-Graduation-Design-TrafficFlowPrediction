import pandas as pd
import osmnx as ox
import os
import glob

def map_matching():
    # --- 配置 ---
    graph_file = "athens_road_network.graphml"
    processed_dir = "processed_data"  # step1 输出的文件夹
    output_dir = "matched_data"       # 存放匹配结果
    
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # 1. 加载路网
    if not os.path.exists(graph_file):
        print(f"❌ 找不到路网文件: {graph_file}，请先运行可视化脚本下载。")
        return
    
    print(f"📍 正在加载路网...")
    G = ox.load_graphml(graph_file)

    # 2. 获取所有待处理的 Parquet 文件
    parquet_files = glob.glob(os.path.join(processed_dir, "*.parquet"))
    if not parquet_files:
        print(f"❌ 在 {processed_dir} 中没找到数据。")
        return

    print(f"🚀 开始处理文件匹配（已自动过滤 info 文件）...")

    for file_path in parquet_files:
        file_name = os.path.basename(file_path)
        
        # 【修改点 1】过滤掉不含轨迹点的静态信息文件
        if "_info" in file_name:
            print(f"⏭️  跳过信息文件: {file_name}")
            continue
            
        print(f"--- 正在匹配轨迹文件: {file_name} ---")
        df = pd.read_parquet(file_path)
        
        # 【修改点 2】健壮性检查：确保列名存在
        if 'lon' not in df.columns or 'lat' not in df.columns:
            print(f"⚠️  警告：文件 {file_name} 缺少 'lon' 或 'lat' 列，跳过。")
            continue

        # 3. 核心：向量化匹配最近的路段
        print(f"   正在计算 {len(df)} 个点的最近路段...")
        
        # nearest_edges 返回 (u, v, key) 元组列表
        try:
            edges = ox.nearest_edges(G, X=df['lon'], Y=df['lat'])
            
            # 将匹配结果存回 DataFrame
            # 格式化为 "起点_终点" 的字符串，方便后续 GCN 构建邻接矩阵
            df['u'] = [e[0] for e in edges]
            df['v'] = [e[1] for e in edges]
            df['edge_id'] = df['u'].astype(str) + "_" + df['v'].astype(str)
            
            # 4. 保存匹配后的结果
            output_path = os.path.join(output_dir, file_name.replace('.parquet', '_matched.parquet'))
            df.to_parquet(output_path, index=False)
            print(f"✅ 成功保存至: {output_path}")
            
        except Exception as e:
            print(f"❌ 处理文件 {file_name} 时出错: {e}")

if __name__ == "__main__":
    map_matching()