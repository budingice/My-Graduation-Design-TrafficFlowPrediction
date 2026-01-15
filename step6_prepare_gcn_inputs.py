import pandas as pd
import numpy as np
import osmnx as ox
import torch

def prepare_gcn():
    # 1. 加载数据
    df = pd.read_pickle('final_matched_trajectories.pkl')
    
    # 2. 确定我们要研究的路段（就是那 34 条有流量的路）
    active_edges = df['edge_id_str'].unique().tolist()
    edge_to_idx = {edge: i for i, edge in enumerate(active_edges)}
    num_nodes = len(active_edges)
    print(f"研究路段数: {num_nodes}")

    # 3. 构建邻接矩阵 A (Adjacency Matrix)
    # 如果路段 A 的终点是路段 B 的起点，则 A 和 B 相连
    adj = np.zeros((num_nodes, num_nodes))
    for e1 in active_edges:
        for e2 in active_edges:
            if e1 == e2: continue
            # 解析 u_v 结构
            u1, v1 = e1.split('_')
            u2, v2 = e2.split('_')
            # 如果 e1 的终点是 e2 的起点，说明车流可以直接过去
            if v1 == u2:
                adj[edge_to_idx[e1], edge_to_idx[e2]] = 1
    
    print(f"邻接矩阵构建完成，总连接数: {np.sum(adj)}")

    # 4. 构建特征矩阵 X (流量随时间变化)
    # 我们把 30 分钟切成 6 个 Slot (每 5 分钟一段)
    df['time_slot'] = (df['time'] // 300).astype(int) 
    # 统计每个路段在每个 slot 的流量
    flow_pivot = df.pivot_table(index='edge_id_str', columns='time_slot', 
                               values='track_id', aggfunc='nunique').fillna(0)
    
    # 确保行顺序和邻接矩阵一致
    X = flow_pivot.loc[active_edges].values
    
    # 5. 保存为 PyTorch 张量
    torch.save(torch.FloatTensor(X), 'features_X.pt')
    torch.save(torch.FloatTensor(adj), 'adj_A.pt')
    print("✅ GCN 输入数据准备完毕：features_X.pt (流量), adj_A.pt (拓扑)")

if __name__ == "__main__":
    prepare_gcn()