import streamlit as st
import pandas as pd
import os
from google.cloud import bigquery
import plotly.graph_objects as go
from google.oauth2 import service_account # 追加: サービスアカウント情報を扱うため
import json # 追加: JSON文字列を辞書にパースするため
from plotly.subplots import make_subplots
import db_dtypes # PandasがBigQueryのデータ型をより良く扱うために推奨

# --- 設定 ---
CREDENTIALS_FILENAME = "streamlit-bq-access.json"  # サービスアカウントキーJSONファイル名
PROJECT_ID = "python-op-373206" # JSONファイルから取得したプロジェクトID
# TODO: 以下のデータセットIDとテーブルIDを実際の値に置き換えてください
DATASET_ID = "JPX_web_data"
TABLE_ID = "Key_IV_Points"

# --- ローカル開発用の認証情報フォールバック (オプション) ---
# Streamlit Cloudでは st.secrets を使用しますが、ローカルで st.secrets が利用できない場合に
# 従来の環境変数やローカルファイルベースの認証を試みるためのものです。
# Streamlit Cloudでのデプロイ時には、st.secrets["gcp_service_account"] が優先されます。
LOCAL_DEV_AUTH = os.environ.get("LOCAL_DEV_AUTH", "false").lower() == "true"
if LOCAL_DEV_AUTH and "GOOGLE_APPLICATION_CREDENTIALS" not in os.environ:
    local_credentials_path = os.path.join(os.path.dirname(__file__), CREDENTIALS_FILENAME)
    if os.path.exists(local_credentials_path):
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = local_credentials_path

# --- ページ設定 ---
st.set_page_config(layout="wide") # ページレイアウトをワイドに設定

# --- デバッグ情報表示 ---
st.sidebar.subheader("デバッグ情報 (ローカル開発用)")
st.sidebar.write(f"LOCAL_DEV_AUTH: `{LOCAL_DEV_AUTH}` (環境変数 LOCAL_DEV_AUTH で 'true' を設定)")
gac_env_val = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
st.sidebar.write(f"GOOGLE_APPLICATION_CREDENTIALS: `{gac_env_val}`")
if LOCAL_DEV_AUTH and gac_env_val:
    st.sidebar.write(f"GAC ファイル存在確認: `{os.path.exists(gac_env_val)}`")
st.sidebar.markdown("---")

# Streamlit タイトル
st.title("📈 BigQuery データビューア")

@st.cache_data # パフォーマンス向上とBigQueryコスト削減のためデータをキャッシュ
def load_data_from_bigquery(project_id: str, dataset_id: str, table_id: str) -> pd.DataFrame:
    """指定されたBigQueryテーブルからデータを読み込みます。"""
    client = None # client変数を初期化
    try:
        if LOCAL_DEV_AUTH:
            # ローカル開発用の認証を試行
            if "GOOGLE_APPLICATION_CREDENTIALS" not in os.environ:
                st.error("load_data_from_bigquery (local dev): LOCAL_DEV_AUTH is true, but GOOGLE_APPLICATION_CREDENTIALS 環境変数が設定されていません。")
                return pd.DataFrame()

            gac_path_func = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
            if not (gac_path_func and os.path.exists(gac_path_func)):
                st.error(f"load_data_from_bigquery (local dev): 認証情報ファイルが見つかりません。パス: '{gac_path_func}'。")
                return pd.DataFrame()

            client = bigquery.Client.from_service_account_json(gac_path_func, project=project_id)
            st.info("BigQuery client initialized using local GOOGLE_APPLICATION_CREDENTIALS (for local development).")
        else: # LOCAL_DEV_AUTH is False (Streamlit Cloud or local secrets.toml mode)
            # Streamlit Cloud または ローカルの secrets.toml から認証情報を取得
            try:
                creds_data = st.secrets.get("gcp_service_account")
            except FileNotFoundError: # ローカルで secrets.toml が見つからない場合
                st.error("load_data_from_bigquery: ローカル実行で secrets.toml が見つかりません。LOCAL_DEV_AUTH=true を設定するか、.streamlit/secrets.toml を作成してください。")
                return pd.DataFrame()

            if not creds_data:
                st.error("load_data_from_bigquery: Streamlit Secretsに 'gcp_service_account' が設定されていません。")
                return pd.DataFrame()

            creds_info = None
            creds_info = creds_data

            # クライアント初期化に使用するプロジェクトIDを決定
            # creds_infoにproject_idがあればそれを使用、なければグローバルなPROJECT_IDを使用
            client_project_id = creds_info.get("project_id", project_id)
            if not client_project_id:
                st.error("load_data_from_bigquery: プロジェクトIDが認証情報またはグローバル設定で見つかりません。")
                return pd.DataFrame()

            credentials = service_account.Credentials.from_service_account_info(creds_info)
            client = bigquery.Client(credentials=credentials, project=client_project_id)
            # 初期化に成功したときのコメント
            # st.success("BigQuery client initialized successfully using st.secrets['gcp_service_account']!")

        if not client: # 上記のロジックでclientが設定されなかった場合の最終チェック
            st.error("load_data_from_bigquery: BigQueryクライアントの初期化に失敗しました（予期せぬ状態）。")
            return pd.DataFrame()
        query = f"""
        SELECT
            Datetime,
            future_price,
            ATM  # ATMカラムを追加
        FROM
          `{project_id}.{dataset_id}.{table_id}` # バッククォートで囲むのが標準的
        ORDER BY
          Datetime DESC # 最新のデータを取得するために降順にする
        LIMIT 400; # 最新から400件を取得
        """
        # ステータスメッセージ
        # st.info(f"クエリ実行中: `{project_id}.{dataset_id}.{table_id}`...")
        df = client.query(query).to_dataframe()
        # グラフ表示等のためにDatetimeで昇順にソートし直す
        if not df.empty and "Datetime" in df.columns:
            df = df.sort_values(by="Datetime", ascending=True).reset_index(drop=True)
        
        #st.success("データの読み込みに成功しました！")
        
        return df
    except Exception as e: # その他の認証エラーやクエリエラー
        st.error(f"BigQuery の処理中にエラーが発生しました: {e}")
        st.exception(e) # デバッグ用に完全なトレースバックを表示
        return pd.DataFrame() # エラー時は空のDataFrameを返す

# --- メインアプリロジック ---
# 認証とデータ取得は load_data_from_bigquery 関数に任せます。
df = load_data_from_bigquery(PROJECT_ID, DATASET_ID, TABLE_ID)

if not df.empty:

    # グラフ表示
    st.subheader("📊 時系列グラフ")
    if "Datetime" in df.columns and "future_price" in df.columns and "ATM" in df.columns:
        # キャッシュされたDataFrameを直接変更しないようにコピーを作成
        df_display = df.copy()
        df_display["Datetime"] = pd.to_datetime(df_display["Datetime"])
        # X軸の表示フォーマットを「年-月-日 時:分」に設定
        df_display["Datetime"] = df_display["Datetime"].dt.strftime('%Y-%m-%d %H:%M')

        # Plotlyを使用して2軸グラフを作成
        fig = make_subplots(specs=[[{"secondary_y": True}]])

        # future_price のトレース (左Y軸)
        fig.add_trace(
            go.Scatter(x=df_display["Datetime"], y=df_display["future_price"], name="Future Price", line=dict(color='royalblue')),
            secondary_y=False,
        )

        # ATM のトレース (右Y軸)
        fig.add_trace(
            go.Scatter(x=df_display["Datetime"], y=df_display["ATM"], name="ATM", line=dict(color='orangered'), # Added comma
                       hovertemplate='<b>Datetime</b>: %{x}<br><b>ATM</b>: %{y:.2%}<extra></extra>' # Show datetime and ATM
            ), # This parenthesis now correctly closes go.Scatter
            secondary_y=True # This is the second argument to fig.add_trace
        )

        # グラフのレイアウト設定
        fig.update_layout(
            title_text="Future Price と ATM の時系列推移",
            xaxis_title="日時",
            xaxis_type='category',  # X軸のタイプをカテゴリに変更
            legend_title_text='系列'
        )

        # Y軸の範囲を異常値（3σルール）を除外して設定
        for col, is_secondary_y, color in [("future_price", False, 'royalblue'), ("ATM", True, 'orangered')]:
            if col in df_display.columns:
                series = df_display[col].dropna()

                y_axis_config = {
                    "title_text": f"<b>{col.replace('_', ' ').title()}</b>",
                    "secondary_y": is_secondary_y,
                    "color": color
                }
                if col == "ATM":
                    y_axis_config["title_text"] = f"<b>{col.replace('_', ' ').title()} (%)</b>"
                    y_axis_config["tickformat"] = '.2%'  # Format ticks as percentage with 2 decimal places (e.g., 0.1234 -> 12.34%)
                elif col == "future_price":
                    # future_priceの軸目盛りを整数（カンマ区切り）で表示し、「k」のような省略を避ける
                    y_axis_config["tickformat"] = ',d'

                if not series.empty:
                    mean = series.mean()
                    std = series.std()

                    # 3σルールを適用（stdが0またはNaNでない場合）
                    if pd.notna(std) and std > 0:
                        lower_bound = mean - 3 * std
                        upper_bound = mean + 3 * std
                        filtered_series = series[(series >= lower_bound) & (series <= upper_bound)]
                        # フィルター後にデータが残っているか確認
                        if filtered_series.empty:
                            filtered_series = series # 3σで全て除外されたら元データで範囲決定
                    else:
                        # stdが0またはNaNの場合、フィルターなし
                        filtered_series = series

                    if not filtered_series.empty:
                        current_min = filtered_series.min()
                        current_max = filtered_series.max()

                        if current_min == current_max:
                            padding = abs(current_min * 0.1) if current_min != 0 else 0.01
                            padding = max(padding, 0.005) # 最小パディングを保証 (ATMが0.1なら0.005は5%に相当)
                            y_axis_config["range"] = [current_min - padding, current_max + padding]
                        else:
                            margin = (current_max - current_min) * 0.05
                            y_axis_config["range"] = [current_min - margin, current_max + margin]

                fig.update_yaxes(**y_axis_config)

        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("グラフ描画に必要な列（'Datetime', 'future_price', 'ATM'）がデータに含まれていません。")
else:
    st.warning("データが取得できませんでした。")
