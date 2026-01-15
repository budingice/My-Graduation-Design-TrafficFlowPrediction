import pandas as pd
import osmnx as ox
import matplotlib.pyplot as plt
import os

def visualize():
    # --- 配置 ---
    graph_file = "athens_road_network.graphml" # 本地地图文件名
    # 开启 OSMnx 的内置缓存功能，减少重复下载
    ox.settings.use_cache = True
    ox.settings.log_console = True

    # 1. 加载轨迹
    if not os.path.exists('trajectories_1hz.pkl'):
        print(" 找不到 trajectories_1hz.pkl")
        return
    df_t = pd.read_pickle('trajectories_1hz.pkl')

    # 2. 获取路网（优先从本地读取）
    if os.path.exists(graph_file):
        print(f" 正在从本地加载路网: {graph_file}...")
        G = ox.load_graphml(graph_file)
    else:
        print(" 第一次运行，正在从网络下载雅典路网...")
        avg_lat, avg_lon = df_t['lat'].mean(), df_t['lon'].mean()
        # 下载范围稍微大一点，涵盖所有轨迹
        G = ox.graph_from_point((avg_lat, avg_lon), dist=1000, network_type='drive')
        # 立即保存到本地，下次就不用下载了
        ox.save_graphml(G, graph_file)
        print(f" 路网已保存至本地: {graph_file}")

    # 3. 绘图（只画前10辆车，避免卡顿）
    sample_tracks = df_t['track_id'].unique()[:10]
    df_sample = df_t[df_t['track_id'].isin(sample_tracks)]
    
    print(" 正在生成地图...")
    fig, ax = ox.plot_graph(G, show=False, close=False, edge_color='#555555', edge_linewidth=0.8, node_size=0)

    for tid in sample_tracks:
        track_data = df_sample[df_sample['track_id'] == tid]
        ax.scatter(track_data['lon'], track_data['lat'], s=3, zorder=3)

    plt.title(f"Athens Traffic - {len(sample_tracks)} Vehicles")
    plt.show()

if __name__ == "__main__":
    visualize()