from flask import Flask, request, jsonify
import datetime

app = Flask(__name__)

# 建立一個 /webhook 的接收路由，只允許 POST 請求
@app.route('/webhook', methods=['POST'])
def webhook():
    if request.method == 'POST':
        # 接收來自 TradingView 的 JSON 資料
        data = request.json
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        print(f"[{now}] 收到 TradingView 訊號: {data}")

        action = data.get('action')
        z_score = data.get('z_score')

        # 根據訊號執行對應的交易邏輯
        if action == 'long_spread':
            print(f"👉 執行：做多價差 (Long PAXG, Short XAUT) | Z: {z_score}")
            # 在這裡呼叫你的 KCEX 自動化下單程式碼
            
        elif action == 'short_spread':
            print(f"👉 執行：做空價差 (Short PAXG, Long XAUT) | Z: {z_score}")
            
        elif action == 'exit_spread':
            print(f"👉 執行：均值回歸，雙腿平倉 | Z: {z_score}")
            
        elif action == 'stop_loss':
            print(f"⚠️ 執行：觸發停損，強制平倉 | Z: {z_score}")

        return jsonify({"status": "success", "message": "Signal received"}), 200

if __name__ == '__main__':
    # 啟動伺服器，監聽 5000 port
    print("等待 TradingView 訊號中 (Port: 5000)...")
    app.run(port=5000)