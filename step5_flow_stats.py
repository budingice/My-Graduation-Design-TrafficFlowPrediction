import pandas as pd

def analyze_flow():
    # 1. 加载全量匹配后的数据
    print("正在读取匹配数据...")
    df = pd.read_pickle('final_matched_trajectories.pkl')
    
    # 2. 统计每个路段的流量 (有多少辆不重复的车经过)
    # 我们按 track_id 去重，确保一辆车在一条路上只被算一次流量
    flow_counts = df.groupby('edge_id_str')['track_id'].nunique().sort_values(ascending=False)
    
    print("\n🔥 流量最高的前 10 个路段:")
    print(flow_counts.head(10))
    
    # 3. 看看流量的分布情况
    print(f"\n总计有 {len(flow_counts)} 条路段产生了流量。")
    print(f"平均每条路通过车辆: {flow_counts.mean():.2f}")

if __name__ == "__main__":
    analyze_flow()