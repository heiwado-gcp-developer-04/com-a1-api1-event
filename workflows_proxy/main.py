import os
import json
import logging
from datetime import datetime, timezone

import functions_framework
from google.cloud.workflows import executions_v1
from google.api_core import exceptions
import google.cloud.logging

# ==========================================
# ログの初期設定 (GCP Cloud Logging との統合)
# ==========================================
try:
    log_client = google.cloud.logging.Client()
    log_client.setup_logging()
except Exception:
    logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)

# ==========================================
# 環境変数 & Workflows Client
# ==========================================
WF_PROJECT_ID = os.environ.get("WF_PROJECT_ID")
WF_LOCATION = os.environ.get("LOCATION")

_client = executions_v1.ExecutionsClient()

# ==========================================
# ルーティングテーブル
# ==========================================
ROUTING_TABLE = {
    "IVRCreationAmtRsltWeekly": "dev-dwh-e36-wf01-01-fresh-order-send-to-dwh",
}

# ==========================================
# タイムスタンプフォーマット変換関数
# ==========================================
def format_to_job_utc(dt: datetime) -> str:
    """日時のフォーマットを整える（Cloud Run Jobsの引数用）"""
    return dt.strftime('%Y-%m-%d %H:%M:%S')

# ==============================================================================
# 【メイン処理】 Eventarc (ファイル検知) 起動用ハンドラー
# ==============================================================================
@functions_framework.http
def trigger_workflow(request):
    ce_id = request.headers.get('ce-id')
    ce_time = request.headers.get('ce-time', '')
    ce_source = request.headers.get('ce-source', '')
    
    logger.info(f"[Eventarc Mode] Received ce-id: {ce_id}, ce-time: {ce_time}")

    request_json = request.get_json(silent=True) or {}
    file_name = request_json.get('name', '')

    # 1. フォルダ名とファイル名の抽出
    if '/' in file_name:
        folder_name = file_name.split('/')[0]
        file_basename = file_name.split('/')[-1]  # ファイル名部分だけを抽出
    else:
        msg = f"ルート直下のファイルは無視します: {file_name}"
        logger.info(msg)
        return json.dumps({"status": "Ignored", "message": msg}, ensure_ascii=False), 200, {'Content-Type': 'application/json'}

    # ---------------------------------------------------------
    # 起因の制御（正常配置のIFファイル以外はすべて弾く）
    # ---------------------------------------------------------
    # ① 階層（深さ）チェック：サブフォルダの場合は、さらに理由を分類して弾く
    if file_name.count('/') > 1:
        if file_basename.startswith('ccr_dp_'):
            # IFファイル名なのにサブフォルダにある ＝ バックアップ処理と判定
            msg = f"バックアップ処理による配置のためスキップします: {file_name}"
        else:
            # IFファイル名ではない ＝ 中間ファイル（parquet等）と判定
            msg = f"中間ファイル出力処理による配置のためスキップします: {file_name}"
            
        logger.info(msg)
        return json.dumps({"status": "Skipped", "message": msg}, ensure_ascii=False), 200, {'Content-Type': 'application/json'}

    # ② ファイル名チェック：ルート直下でも、正規のIFプレフィックスでなければ弾く
    if not file_basename.startswith('ccr_dp_'):
        msg = f"対象のIFファイル(ccr_dp_〜)ではないためスキップします: {file_name}"
        logger.info(msg)
        return json.dumps({"status": "Skipped", "message": msg}, ensure_ascii=False), 200, {'Content-Type': 'application/json'}
    # ---------------------------------------------------------
    
    # 2. ルーティングの決定
    workflow_id = ROUTING_TABLE.get(folder_name)
    if not workflow_id:
        msg = f"フォルダ '{folder_name}' に対応するWorkflows設定がありません。"
        logger.warning(msg)
        return json.dumps({"status": "Ignored", "message": msg}, ensure_ascii=False), 200, {'Content-Type': 'application/json'}

    # 3. 時間のパース（冪等性の担保）
    if ce_time:
        clean_string = ce_time.replace('Z', '+00:00')
        target_dt = datetime.fromisoformat(clean_string).astimezone(timezone.utc)
    else:
        target_dt = datetime.now(timezone.utc)

    run_job_scheduled_time = format_to_job_utc(target_dt)

    # 環境変数の安全チェック
    if not WF_PROJECT_ID or not WF_LOCATION:
        error_msg = "環境変数 'WF_PROJECT_ID' または 'LOCATION' が設定されていません。"
        logger.error(f"【環境変数エラー】{error_msg}")
        return json.dumps({"status": "Internal Server Error", "message": error_msg}, ensure_ascii=False), 500, {'Content-Type': 'application/json'}

    # 4. Workflowsへの引数組み立て
    workflow_args = {
        "scheduled_time": run_job_scheduled_time,
        "event_id": ce_id,
        "event_source": ce_source,
        "folder_name": folder_name,
        "file_name": file_name
    }

    parent = f"projects/{WF_PROJECT_ID}/locations/{WF_LOCATION}/workflows/{workflow_id}"
    logger.info(f"Attempting to trigger workflow(Eventarc): {workflow_id} for Cloud Run Jobs execution date: {run_job_scheduled_time}")

    # 5. GCP例外ハンドリング（Workflowsの起動）
    try:
        execution = executions_v1.Execution(argument=json.dumps(workflow_args))
        response = _client.create_execution(parent=parent, execution=execution)

        logger.info(f"ワークフローを正常に起動しました(Eventarc)。対象: {workflow_id} | 実行名: {response.name}")

        success_response = {
            "status": "Success",
            "message": f"Workflow execution started for {workflow_id}",
            "execution_name": response.name
        }
        return json.dumps(success_response, ensure_ascii=False), 200, {'Content-Type': 'application/json'}

    except exceptions.NotFound as e:
        error_msg = f"指定されたワークフロー '{workflow_id}' が見つかりませんでした。"
        logger.error(f"【404エラー】{error_msg} (パス: {parent} | 詳細: {e.message})")
        return json.dumps({"status": "Not Found", "message": error_msg, "details": e.message}, ensure_ascii=False), 404, {'Content-Type': 'application/json'}
    
    except exceptions.PermissionDenied as e:
        error_msg = "ワークフローを起動する権限がありません。"
        logger.error(f"【403エラー】{error_msg} (詳細: {e.message})")
        return json.dumps({"status": "Permission Denied", "message": error_msg, "details": e.message}, ensure_ascii=False), 403, {'Content-Type': 'application/json'}
    
    except exceptions.GoogleAPICallError as e:
        error_msg = "Google Cloud APIの呼び出し中にエラーが発生しました。"
        logger.error(f"【GCP APIエラー】{error_msg} (詳細: {e.message})")
        return json.dumps({"status": "GCP API Error", "message": error_msg, "details": e.message}, ensure_ascii=False), 500, {'Content-Type': 'application/json'}
    
    except Exception as e:
        error_msg = "プログラム内部で予期せぬエラーが発生しました。"
        logger.error(f"【500システムエラー】{error_msg} (詳細: {str(e)})", exc_info=True)
        return json.dumps({"status": "Internal Server Error", "message": error_msg, "details": str(e)}, ensure_ascii=False), 500, {'Content-Type': 'application/json'}