import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import plotly.graph_objs as go
import sqlite3
import pandas as pd

DB_PATH = "live_metrics.db"
# MAX_POINTS 代表畫面上要保留「幾個分鐘」的資料
MAX_POINTS = 200 

app = dash.Dash(__name__)

app.layout = html.Div([
    html.H2("PAXG/XAUT 即時 Ratio 與 MA (1分鐘線即時更新)", style={'color': 'white', 'textAlign': 'center', 'fontFamily': 'Arial', 'margin': '10px 0'}),
    dcc.Graph(id='live-graph', animate=False, style={'height': '85vh'}), 
    dcc.Interval(id='graph-update', interval=1000, n_intervals=0)
], style={'backgroundColor': '#0B0E11', 'padding': '10px', 'height': '100vh', 'boxSizing': 'border-box'})

@app.callback(
    Output('live-graph', 'figure'),
    [Input('graph-update', 'n_intervals')]
)
def update_graph(n):
    try:
        conn = sqlite3.connect(DB_PATH)
        # 【關鍵修改 1】使用 GROUP BY 來將資料按「分鐘」打包，並使用 MAX(ts) 確保抓到該分鐘最新的一筆 tick
        query = f"""
            SELECT 
                MAX(ts) as ts, 
                ratio, 
                mean 
            FROM metrics 
            GROUP BY CAST(ts / 60 AS INT) 
            ORDER BY ts DESC 
            LIMIT {MAX_POINTS}
        """
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        # 時間順序導正 (舊 -> 新)
        df = df.sort_values('ts')
        
        # 【關鍵修改 2】將 UNIX 時間戳轉換為 datetime，並無條件捨去到「分鐘」 (.dt.floor('min'))
        # 這樣圖表上的 X 軸座標就會牢牢固定在 12:00, 12:01... 而不會有秒數，完美重現 TradingView 效果
        if not df.empty:
            df['plot_time'] = pd.to_datetime(df['ts'], unit='s', utc=True).dt.tz_convert('Asia/Taipei').dt.floor('min')

    except Exception as e:
        df = pd.DataFrame(columns=['plot_time', 'ratio', 'mean'])

    fig = go.Figure()

    if not df.empty:
        # 加入 Ratio 實線
        fig.add_trace(go.Scatter(
            x=df['plot_time'], 
            y=df['ratio'], 
            name='Ratio (1m close/live)', 
            mode='lines', 
            line=dict(color='#F6C343', width=2, shape='linear') # 加上 shape='spline' 讓線條更平滑
        ))
        
        # 加入 MA 均線 虛線
        fig.add_trace(go.Scatter(
            x=df['plot_time'], 
            y=df['mean'], 
            name='Rolling MA', 
            mode='lines', 
            line=dict(color='#00ffcc', width=2, dash='dash')
        ))

        y_min, y_max = df[['ratio', 'mean']].min().min(), df[['ratio', 'mean']].max().max()
    else:
        y_min, y_max = 0.3, 0.5 

    fig.update_layout(
        template='plotly_dark', 
        paper_bgcolor='#0B0E11', 
        plot_bgcolor='#0B0E11',
        margin=dict(l=50, r=50, t=20, b=50),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        xaxis=dict(
            showgrid=False,
            tickformat='%H:%M' # 【關鍵修改 3】X 軸刻度強制只顯示 時:分
        ),
        yaxis=dict(
            title="",
            range=[y_min - 0.05, y_max + 0.05] if not df.empty else None,
            showgrid=True, 
            gridcolor='#1F2933',
            side='right' 
        )
    )

    return fig

if __name__ == '__main__':
    app.run(debug=True, port=8050)