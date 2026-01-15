import pandas as pd
import csv
import time
import os

def run_parser():
    input_file = '20181024_d1_0830_0900.csv' 
    output_file = 'trajectories_1hz.pkl' 
    sampling_rate = 25 

    if not os.path.exists(input_file):
        print(f" 找不到文件: {input_file}")
        return

    vehicles_list = []
    trajectories_list = []
    
    print(f" 开始解析: {input_file}")
    start_time = time.time()

    with open(input_file, 'r', encoding='utf-8') as f:
        reader = csv.reader(f, delimiter=';')
        header = next(reader)
        
        for row in reader:
            # 清理数据：去掉空格和空值
            row = [x.strip() for x in row if x]
            if len(row) < 10: continue
            
            track_id = int(row[0])
            v_type = row[1]
            avg_speed = float(row[3])
            
            vehicles_list.append({
                'track_id': track_id,
                'type': v_type,
                'avg_speed': avg_speed
            })
            
            # 动态数据处理
            # row[0:10] 是元数据，row[10:] 是轨迹点 (6列一组)
            dynamic_data = row[10:]
            
            # 为了获取 1Hz 数据，我们每隔 sampling_rate * 6 采样一次
            # 重点：我们需要记录这辆车的“生命周期”
            for i in range(0, len(dynamic_data), 6 * sampling_rate):
                chunk = dynamic_data[i : i + 6]
                if len(chunk) >= 6:
                    try:
                        trajectories_list.append({
                            'track_id': track_id,
                            'lat': float(chunk[0]),
                            'lon': float(chunk[1]),
                            'speed': float(chunk[2]),
                            'time': float(chunk[5])
                        })
                    except ValueError:
                        continue

    df_v = pd.DataFrame(vehicles_list)
    df_t = pd.DataFrame(trajectories_list)
    
    # --- 关键审计：打印时间跨度 ---
    if not df_t.empty:
        t_min = df_t['time'].min()
        t_max = df_t['time'].max()
        print(f"\n 🚀 数据审计结果:")
        print(f"   起始时间: {t_min:.2f} s")
        print(f"   结束时间: {t_max:.2f} s")
        print(f"   解析出的时间跨度: {(t_max - t_min)/60:.2f} 分钟")
    
    print(f"\n 解析完成！耗时: {time.time() - start_time:.2f} 秒")
    print(f" 统计：车辆总数 {len(df_v)}, 轨迹点总数 {len(df_t)}")
    
    df_t.to_pickle(output_file)
    df_v.to_pickle('vehicles_info.pkl')
    print(" 全部完成！")

if __name__ == "__main__":
    run_parser()