import pandas as pd
import osmnx as ox
import os

def map_matching():
    # 1. 加载路网和轨迹
    G = ox.load_graphml("athens_road_network.graphml")
    df_t = pd.read_pickle('trajectories_1hz.pkl')

    # 2. 为了速度，先测试前 1000 个轨迹点
    df_test = df_t.head(1000).copy()

    print(" 正在进行地图匹配（将 GPS 点吸附到最近的路段）...")
    
    # 3. 核心：获取每个点最近的边的 ID
    # return_dist=True 可以返回 GPS 点距离道路的实际物理距离（米）
    ne = ox.nearest_edges(G, X=df_test['lon'], Y=df_test['lat'])
    
    # 将匹配结果存回 DataFrame
    # ne 是一个包含 (u, v, key) 的列表
    df_test['edge_id'] = ne
    
    print(" 匹配完成！示例数据：")
    print(df_test[['track_id', 'lat', 'lon', 'edge_id']].head())

    # 4. 保存匹配后的数据
    df_test.to_pickle('trajectories_matched_test.pkl')

if __name__ == "__main__":
    map_matching()