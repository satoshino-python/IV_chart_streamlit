import streamlit as st
import pandas as pd
import os
from google.cloud import bigquery
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import db_dtypes # PandasがBigQueryのデータ型をより良く扱うために推奨

# --- 設定 ---
CREDENTIALS_FILENAME = "streamlit-bq-access.json"  # サービスアカウントキーJSONファイル名
PROJECT_ID = "python-op-373206" # JSONファイルから取得したプロジェクトID
# TODO: 以下のデータセットIDとテーブルIDを実際の値に置き換えてください
DATASET_ID = "JPX_web_data"
TABLE_ID = "Key_IV_Points"

# --- Google Cloud 認証情報の設定 ---
# Streamlit Cloudなどの環境で環境変数 GOOGLE_APPLICATION_CREDENTIALS が設定されていない場合の
# ローカル開発用のフォールバックとして、ローカルのJSONファイルを使用します。
if "GOOGLE_APPLICATION_CREDENTIALS" not in os.environ:
    local_credentials_path = os.path.join(os.path.dirname(__file__), CREDENTIALS_FILENAME)
    if os.path.exists(local_credentials_path):
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = local_credentials_path
    # ローカルファイルが存在せず、環境変数も設定されていない場合、
    # 後続の bigquery.Client() 呼び出しがこの環境変数に依存していると失敗します。
    # そのエラーは以下の try-except ブロックで捕捉されます。

# --- ページ設定 ---
st.set_page_config(layout="wide") # ページレイアウトをワイドに設定

# Streamlit タイトル
st.title("📈 BigQuery データビューア")

@st.cache_data # パフォーマンス向上とBigQueryコスト削減のためデータをキャッシュ
def load_data_from_bigquery(project_id: str, dataset_id: str, table_id: str) -> pd.DataFrame:
    """指定されたBigQueryテーブルからデータを読み込みます。"""
    client = None
    gac_path_func = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")

    try:
        if gac_path_func and os.path.exists(gac_path_func):
            # 環境変数から取得したJSONファイルのパスを明示的に使用してクライアントを初期化
            client = bigquery.Client.from_service_account_json(
                gac_path_func, project=project_id
            )
            st.success("BigQuery client initialized successfully using explicit JSON path!")
        elif gac_path_func:
            # 環境変数は設定されているが、ファイルが存在しない場合
            st.error(f"load_data_from_bigquery: 認証情報ファイルが見つかりません。パス: '{gac_path_func}'。Streamlit Secretsの設定を確認してください。")
            return pd.DataFrame()
        else:
            # 環境変数が設定されていない場合
            st.error("load_data_from_bigquery: GOOGLE_APPLICATION_CREDENTIALS 環境変数が設定されていません。Streamlit Secretsの設定を確認してください。")
            return pd.DataFrame()

        if not client: # 上記の条件分岐で client が None のままだった場合 (念のため)
            st.error("load_data_from_bigquery: BigQueryクライアントの初期化に失敗しました。")
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
        st.info(f"クエリ実行中: `{project_id}.{dataset_id}.{table_id}`...")
        df = client.query(query).to_dataframe()
        # グラフ表示等のためにDatetimeで昇順にソートし直す
        if not df.empty and "Datetime" in df.columns:
            df = df.sort_values(by="Datetime", ascending=True).reset_index(drop=True)
        st.success("データの読み込みに成功しました！")
        return df
    except Exception as e:
        st.error(f"BigQuery のクエリ実行中にエラーが発生しました: {e}")
        st.exception(e) # デバッグ用に完全なトレースバックを表示
        return pd.DataFrame() # エラー時は空のDataFrameを返す

# --- メインアプリロジック ---
try:
    # この呼び出しで認証情報が使用されます。
    # Streamlit Cloudでは、GOOGLE_APPLICATION_CREDENTIALS はSecretsから設定されるべきです。
    # ローカルでは、上記のロジックによりファイルが存在すれば設定されるべきです。
    gac_path_main = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if gac_path_main and os.path.exists(gac_path_main):
        # 環境変数から取得したJSONファイルのパスを明示的に使用してクライアントを初期化 (テスト用)
        bigquery_client_test = bigquery.Client.from_service_account_json(
            gac_path_main, project=PROJECT_ID
        )
        # st.info("Test BigQuery client explicitly initialized using service account JSON.") # デバッグ用
    elif gac_path_main:
        # 環境変数は設定されているが、ファイルが存在しない場合
        st.error(
            f"認証エラー: GOOGLE_APPLICATION_CREDENTIALS が '{gac_path_main}' に設定されていますが、"
            "ファイルが見つかりません。Streamlit CloudのSecrets設定を確認してください。"
        )
        st.stop()
    else:
        # 環境変数が設定されていない場合
        st.error(
            "認証エラー: GOOGLE_APPLICATION_CREDENTIALS 環境変数が設定されていません。"
            "Streamlit CloudのSecrets設定、またはローカルの認証情報ファイルを確認してください。"
        )
        st.stop()
except Exception as e:
    # 既存のエラーメッセージに加えて、より詳細な情報を表示
    st.error(f"BigQueryクライアントの初期化または認証に失敗しました (明示的初期化試行中): {e}")
    st.error("詳細なエラー情報と解決策については、アプリケーションのログと上記のメッセージを確認してください。")
    st.exception(e) # デバッグ用に完全なトレースバックを表示
    st.stop()

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
