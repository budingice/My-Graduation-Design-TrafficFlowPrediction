import csv
import pandas as pd

def read_pneuma_data(file_path, sampling_rate=25):
    """
    file_path: pNEUMA原始csv文件的路径
    sampling_rate: 采样率。建议设为25（即1Hz），否则数据量大会卡死。
    """
    trajectory_list = []
    
    print("正在解析数据，请稍候...")
    
    with open(file_path, 'r', encoding='utf-8') as f:
        # pNEUMA的数据是用分号 ; 分隔的
        reader = csv.reader(f, delimiter=';')
        
        # 跳过第一行（表头）
        header = next(reader)
        
        for row in reader:
            # 清理行数据，去掉空格和空字符
            row = [x.strip() for x in row if x.strip()]
            if len(row) < 10: continue
            
            # 前4列是基本信息
            track_id = row[0]
            vehicle_type = row[1]
            
            # 第11列开始是轨迹数据 (每6列一组: lat, lon, speed, lon_acc, lat_acc, time)
            dynamic_data = row[10:]
            
            # 按 sampling_rate 跳着读取，实现降采样
            for i in range(0, len(dynamic_data), 6 * sampling_rate):
                chunk = dynamic_data[i : i + 6]
                if len(chunk) == 6:
                    trajectory_list.append({
                        'track_id': track_id,
                        'type': vehicle_type,
                        'lat': float(chunk[0]),
                        'lon': float(chunk[1]),
                        'speed': float(chunk[2]),
                        'time': float(chunk[5])
                    })
    
    # 将解析结果转换为 DataFrame
    df = pd.DataFrame(trajectory_list)
    print(f"解析完成！提取了 {len(df)} 条轨迹点数据。")
    return df

# --- 运行示例 ---
# 请确保你的文件名和路径正确
# df = read_pneuma_data('20181024_d1_0830_0900.csv')
# print(df.head())