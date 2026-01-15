import pandas as pd

# 读取最新的路径流量聚合结果
df = pd.read_parquet("final_path_flow_results.parquet")

# 1. 基础信息展示
print("--- 📂 路径级流量 (Path Flow) 预览 ---")
# 展示前5行，重点看 path_id
pd.set_option('display.max_colwidth', 50) # 限制路径显示的宽度
print(df.head(10))

# 2. 统计最热门的“路径”
print("\n--- 🔥 流量最高的路径 Top 5 ---")
top_paths = df.groupby('path_id')['path_flow_count'].sum().sort_values(ascending=False).head(5)
print(top_paths)

# 3. 路径复杂度分析
print("\n--- 📈 路径规模统计 ---")
print(f"唯一路径(Path)总数: {df['path_id'].nunique()}")
print(f"平均每条路径包含的路段数: {df['path_id'].apply(lambda x: len(x.split(' -> '))).mean():.1f}")
print(f"最大流量值: {df['path_flow_count'].max()}")