import pandas as pd
import osmnx as ox

def build_full_dataset():
    # 1. 加载
    G = ox.load_graphml("athens_road_network.graphml")
    df_t = pd.read_pickle('trajectories_1hz.pkl')
    
    print(f"🚀 正在为全量 {len(df_t)} 个轨迹点进行地图匹配，请耐心等待...")
    
    # 2. 全量匹配 (这次不限数量)
    # 注意：OSMnx 的 nearest_edges 在处理大规模数据时非常吃 CPU
    df_t['edge_id'] = ox.nearest_edges(G, X=df_t['lon'], Y=df_t['lat'])
    
    # 3. 数据清理：我们将 (u, v, key) 转换成字符串格式，方便后续处理
    df_t['edge_id_str'] = df_t['edge_id'].apply(lambda x: f"{x[0]}_{x[1]}")
    
    # 4. 保存为最终版数据
    df_t.to_pickle('final_matched_trajectories.pkl')
    print("✅ 全量数据匹配并保存成功！文件名为: final_matched_trajectories.pkl")

if __name__ == "__main__":
    build_full_dataset()