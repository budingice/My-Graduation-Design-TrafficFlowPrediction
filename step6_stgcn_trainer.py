import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import DataLoader, Dataset
import numpy as np
import matplotlib.pyplot as plt

# ==========================================
# 1. 数据集定义：处理多个 15 分钟片段
# ==========================================
class TrafficDataset(Dataset):
    def __init__(self, pt_path, window_size=5, horizon=1):
        data = torch.load(pt_path)
        self.x_list = data['x_list']  # List of (15, 50, 1)
        self.window_size = window_size
        self.horizon = horizon
        
        self.samples = []
        for chunk in self.x_list:
            # chunk shape: (15, 50, 1) -> (Time, Nodes, Features)
            # 在每个 15 分钟片段内进行滑动窗口采样
            for t in range(len(chunk) - window_size - horizon + 1):
                x = chunk[t : t + window_size, :, :]
                y = chunk[t + window_size : t + window_size + horizon, :, 0] # 预测流量值
                self.samples.append((x, y))

    def __len__(self):
        return len(self.samples)

    def __getitem__(self, idx):
        x, y = self.samples[idx]
        return torch.FloatTensor(x), torch.FloatTensor(y)

# ==========================================
# 2. ST-GCN 模型定义
# ==========================================
class SimpleSTGCN(nn.Module):
    def __init__(self, adj, num_nodes, in_channels, hidden_channels, out_channels):
        super(SimpleSTGCN, self).__init__()
        # 标准化邻接矩阵 A = D^-0.5 * A * D^-0.5
        adj = torch.FloatTensor(adj)
        d = torch.diag(torch.pow(adj.sum(1), -0.5))
        self.adj = d @ adj @ d 
        
        # 空间卷积层 (Graph Convolution)
        self.gcn = nn.Linear(in_channels, hidden_channels)
        
        # 时间处理层 (Temporal - 这里用简单的全连接压缩时间维度)
        self.temporal_fc = nn.Linear(5, 1) # 假设 window_size=5
        
        # 输出层
        self.out_fc = nn.Linear(hidden_channels, out_channels)

    def forward(self, x):
        # x shape: (Batch, Time, Nodes, Features) -> (B, 5, 50, 1)
        batch_size, T, N, F = x.shape
        
        # 1. 空间维度卷积 (GCN)
        # 将时间维度并入 Batch 方便矩阵运算
        x = x.view(-1, N, F) # (B*T, 50, 1)
        # 聚合邻居信息: A * X
        x = torch.matmul(self.adj.to(x.device), x) 
        x = torch.relu(self.gcn(x)) # (B*T, 50, Hidden)
        
        # 2. 时间维度聚合
        x = x.view(batch_size, T, N, -1) # (B, 5, 50, Hidden)
        x = x.transpose(1, 2).transpose(2, 3) # (B, 50, Hidden, 5)
        x = self.temporal_fc(x) # (B, 50, Hidden, 1)
        
        # 3. 输出
        x = x.squeeze(-1) # (B, 50, Hidden)
        x = self.out_fc(x) # (B, 50, 1)
        return x.squeeze(-1) # (B, 50)

# ==========================================
# 3. 训练主程序
# ==========================================
def main():
    # 参数设置
    WINDOW_SIZE = 5   # 用过去 5 分钟预测
    BATCH_SIZE = 8
    EPOCHS = 50
    LEARNING_RATE = 0.001
    
    # 加载原始数据获取邻接矩阵
    raw_data = torch.load("model_inputs/st_batch_data.pt")
    adj = raw_data['adj']
    num_nodes = adj.shape[0]

    # 准备 DataLoader
    dataset = TrafficDataset("model_inputs/st_batch_data.pt", window_size=WINDOW_SIZE)
    train_size = int(0.8 * len(dataset))
    test_size = len(dataset) - train_size
    train_db, test_db = torch.utils.data.random_split(dataset, [train_size, test_size])
    
    train_loader = DataLoader(train_db, batch_size=BATCH_SIZE, shuffle=True)
    test_loader = DataLoader(test_db, batch_size=BATCH_SIZE)

    # 模型初始化
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    model = SimpleSTGCN(adj, num_nodes, 1, 64, 1).to(device)
    optimizer = optim.Adam(model.parameters(), lr=LEARNING_RATE)
    criterion = nn.MSELoss()

    # 训练循环
    print(f"开始训练... 总样本数: {len(dataset)}, 训练集: {train_size}")
    train_losses = []
    
    for epoch in range(EPOCHS):
        model.train()
        epoch_loss = 0
        for x, y in train_loader:
            x, y = x.to(device), y.to(device).squeeze(1) # y: (B, 50)
            
            optimizer.zero_grad()
            output = model(x)
            loss = criterion(output, y)
            loss.backward()
            optimizer.step()
            epoch_loss += loss.item()
        
        avg_loss = epoch_loss / len(train_loader)
        train_losses.append(avg_loss)
        if (epoch+1) % 10 == 0:
            print(f"Epoch [{epoch+1}/{EPOCHS}], Loss: {avg_loss:.4f}")

    # 可视化结果
    model.eval()
    with torch.no_grad():
        # 取一个 batch 看看预测效果
        test_x, test_y = next(iter(test_loader))
        pred = model(test_x.to(device)).cpu().numpy()
        true = test_y.squeeze(1).numpy()
        
        plt.figure(figsize=(10, 5))
        plt.plot(true[0], label='Actual Flow', alpha=0.7, marker='o')
        plt.plot(pred[0], label='Predicted Flow', alpha=0.7, marker='x')
        plt.title("Path Flow Prediction (Top 50 Paths)")
        plt.xlabel("Path ID")
        plt.ylabel("Vehicle Count / min")
        plt.legend()
        plt.show()

    print("✅ 训练完成！")

if __name__ == "__main__":
    main()