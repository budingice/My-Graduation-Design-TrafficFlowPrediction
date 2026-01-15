import torch
import numpy as np
import torch
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

# 加载数据
A = torch.load('adj_A.pt').numpy()
# 之前在 step6 中我们定义的 active_edges 顺序非常重要
# 我们可以重新推导一下顺序，或者直接看连接统计
num_links = np.sum(A)
rows, cols = np.where(A == 1)

print(f"📊 空间连接审计:")
print(f"路段总数: {len(A)}")
print(f"识别到的连接总数: {num_links}")

# 打印前 5 个连接的具体情况
for i in range(min(5, len(rows))):
    print(f"连接 {i+1}: 路段索引 {rows[i]} -> 路段索引 {cols[i]}")

if num_links == 0:
    print("⚠️ 警告：邻接矩阵为空，说明路段之间没有识别到头尾相连，请检查 step6 的解析逻辑！")

