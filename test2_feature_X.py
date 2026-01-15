import torch
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

plt.rcParams['font.sans-serif'] = ['SimHei']  # 设置黑体
plt.rcParams['axes.unicode_minus'] = False     # 解决负号显示问题

def audit_traffic_matrix():
    # 1. 重新读取匹配后的原始 DataFrame，看看时间列到底长什么样
    df = pd.read_pickle('final_matched_trajectories.pkl')
    
    t_min = df['time'].min()
    t_max = df['time'].max()
    t_duration = t_max - t_min
    
    print(f"📊 时间戳审计:")
    print(f"数据起始秒数: {t_min:.2f}")
    print(f"数据结束秒数: {t_max:.2f}")
    print(f"总时长: {t_duration/60:.2f} 分钟")

    # 2. 强制生成 6 个时间段 (即使后面没车也要占位)
    # 假设每 300 秒一个 Slot，我们要确保 0 到 5 都有
    df['time_slot'] = ((df['time'] - t_min) // 300).astype(int)
    
    # 只取前 30 分钟的数据 (Slot 0-5)
    df = df[df['time_slot'] < 6]
    
    # 3. 重新透视
    flow_pivot = df.pivot_table(index='edge_id_str', columns='time_slot', 
                               values='track_id', aggfunc='nunique')
    
    # 关键：用 reindex 强制填充缺失的 Slot (0,1,2,3,4,5)
    flow_pivot = flow_pivot.reindex(columns=range(6), fill_value=0)
    
    X = flow_pivot.values
    print(f"修正后的矩阵维度: {X.shape} (预期应该是 34, 6)")

    # 4. 可视化每条轨迹的生命周期 (审计为何结束时间不一)
    plt.figure(figsize=(10, 4))
    sample_df = df.sample(n=min(500, len(df)))
    plt.scatter(sample_df['time'], sample_df['track_id'], s=1, alpha=0.5)
    plt.title("Vehicle Trajectory Timeline (Sample)")
    plt.xlabel("Time (seconds)")
    plt.ylabel("Vehicle ID")
    plt.show()

    # 5. 保存修正后的特征
    torch.save(torch.FloatTensor(X), 'features_X_fixed.pt')
    print("✅ 修正后的流量矩阵已保存为 features_X_fixed.pt")

if __name__ == "__main__":
    audit_traffic_matrix()
    