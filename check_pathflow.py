import torch
import numpy as np
import pandas as pd

def print_path_flow_details():
    # 1. 加载数据
    data_path = "model_inputs/st_batch_data.pt"
    try:
        data = torch.load(data_path)
    except FileNotFoundError:
        print("❌ 找不到数据文件，请先运行 step5_build_st_features_batch.py")
        return

    x_list = data['x_list']        # 每个元素是 (15, 50, 1)
    path_labels = data['path_labels'] # 50条路径的元组列表
    
    print(f"========================================")
    print(f"📊 路径流量详细分析报告")
    print(f"片段总数: {len(x_list)} | 路径总数: {len(path_labels)}")
    print(f"========================================\n")

    # 2. 统计每条路径在所有片段中的表现
    # 将所有片段拼接成一个大矩阵 (Total_Time, 50)
    all_data = np.concatenate(x_list, axis=0).squeeze(-1) 
    
    # 计算统计指标
    mean_flow = all_data.mean(axis=0)
    max_flow = all_data.max(axis=0)
    active_rate = (all_data > 0).mean(axis=0) * 100 # 有车经过的时间占比

    # 3. 格式化打印前 10 条路径（或全部 50 条）
    print(f"{'ID':<4} | {'平均流量':<8} | {'最大值':<6} | {'活跃度%':<8} | {'路径构成 (前3个路段)'}")
    print("-" * 80)
    
    for i in range(len(path_labels)):
        # 路径构成描述
        path_desc = " -> ".join(list(path_labels[i])[:3]) 
        if len(path_labels[i]) > 3:
            path_desc += " ..."
            
        print(f"{i:<4} | {mean_flow[i]:<10.2f} | {max_flow[i]:<8.0f} | {active_rate[i]:<9.1f} | {path_desc}")
        
        # 如果只想看前10条，可以取消下面注释
        # if i == 9: break

    # 4. 选择一个片段打印“时间-路径”流量矩阵明细
    # 打印第 1 个 15 分钟片段的前 5 条路径明细
    print(f"\n[明细预览] 第 1 个 15 分钟片段 (前 5 条路径流量):")
    chunk_0 = x_list[0].squeeze(-1) # (15, 50)
    
    header = "分钟 | " + " | ".join([f"P{i}" for i in range(5)])
    print(header)
    print("-" * len(header))
    
    for t in range(chunk_0.shape[0]):
        row_values = " | ".join([f"{val:3.0f}" for val in chunk_0[t, :5]])
        print(f"T{t+1:02d}  | {row_values}")

if __name__ == "__main__":
    print_path_flow_details()