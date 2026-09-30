import functions_framework
from google.cloud import workflows_v1
from google.cloud.workflows import executions_v1
from google.api_core import exceptions
import json
from pydantic import BaseModel, Field, ValidationError
from datetime import datetime, timezone
import logging
import os
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
# 環境変数
# ==========================================
WF_PROJECT_ID = os.getenv("WF_PROJECT_ID", "")
WF_LOCATION = os.getenv("LOCATION", "")

# ==========================================
# Workflows Client
# ==========================================
_client = executions_v1.ExecutionsClient()

# ==============================================================================
# 【NEW】 Eventarc用 ルーティングテーブル
# GCSの「第1階層フォルダ名」と「起動するWorkflows」の紐付け
# ==============================================================================
ROUTING_TABLE = {
    "IVRCreationAmtRsltWeekly": "dev-dwh-e36-wf01-01-fresh-order-send-to-dwh",
    # 例: "NewIFName": "dev-dwh-e36-wf02-01-main",
}

# ==========================================
# タイムスタンプフォーマット変換関数
# ==========================================
def format_to_job_utc(dt: datetime) -> str:
    """
    datetimeオブジェクトを、Cloud Run Jobのアプリ側がパースできる形式
    (2026-05-26 07:51:00.000000+00:00)に変換して返す
    """
    dt_utc = dt.astimezone(timezone.utc)
    return dt_utc.strftime('%Y-%m-%d %H:%M:%S.%f') + '+00:00'

# ==========================================
# データモデル定義 (Scheduler用)
# ==========================================
class SchedulerRequest(BaseModel):
    workflow_id: str = Field(..., description="起動対象のWorkflows ID（必須）")

# ==============================================================================
# 【NEW】 Eventarc (ファイル検知) 起動時の専用ハンドラー (フルエラーハンドリング付き)
# ==============================================================================
def handle_eventarc_request(request):
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
    # 【第1の関所】起因の制御（IFファイル以外は弾く）
    # ---------------------------------------------------------
    if not file_basename.startswith('ccr_dp_'):
        msg = f"IFファイル(ccr_dp_)ではないためスキップします: {file_name}"
        logger.info(msg)
        return json.dumps({"status": "Skipped", "message": msg}, ensure_ascii=False), 200, {'Content-Type': 'application/json'}

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
    
    # ---------------------------------------------------------
    # 【第2の関所】重複起動の制御（現在実行中なら弾く）
    # ---------------------------------------------------------
    try:
        # 対象ワークフローの最新履歴を取得（直近10件）
        list_req = executions_v1.ListExecutionsRequest(parent=parent, page_size=10)
        recent_executions = _client.list_executions(request=list_req)
        
        for exec_item in recent_executions:
            if exec_item.state == executions_v1.Execution.State.ACTIVE:
                msg = f"ワークフロー '{workflow_id}' は現在すでに実行中です。後続ファイル({file_name})による重複起動をスキップします。"
                logger.info(msg)
                return json.dumps({"status": "Skipped", "message": msg}, ensure_ascii=False), 200, {'Content-Type': 'application/json'}
    except Exception as check_e:
        logger.warning(f"実行中ステータスの確認に失敗しました。本来の起動処理を継続します: {check_e}")

    logger.info(f"Attempting to trigger workflow(Eventarc): {workflow_id} for Cloud Run Jobs execution date: {run_job_scheduled_time}")

    # 5. GCP例外ハンドリング（Scheduler側と完全一致）
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
        error_response = {"status": "Not Found", "message": error_msg, "details": e.message}
        logger.error(f"【404エラー】{error_msg} (パス: {parent} | 詳細: {e.message})")
        return json.dumps(error_response, ensure_ascii=False), 404, {'Content-Type': 'application/json'}

    except exceptions.PermissionDenied as e:
        error_msg = "ワークフローを起動する権限がありません。"
        error_response = {"status": "Permission Denied", "message": error_msg, "details": e.message}
        logger.error(f"【403エラー】{error_msg} (詳細: {e.message})")
        return json.dumps(error_response, ensure_ascii=False), 403, {'Content-Type': 'application/json'}

    except exceptions.GoogleAPICallError as e:
        error_msg = "Google Cloud APIの呼び出し中にエラーが発生しました。"
        error_response = {"status": "GCP API Error", "message": error_msg, "details": e.message}
        logger.error(f"【GCP APIエラー】{error_msg} (詳細: {e.message})")
        return json.dumps(error_response, ensure_ascii=False), 500, {'Content-Type': 'application/json'}

    except Exception as e:
        error_msg = "プログラム内部で予期せぬエラーが発生しました。"
        error_response = {"status": "Internal Server Error", "message": error_msg, "details": str(e)}
        logger.error(f"【500システムエラー】{error_msg} (詳細: {str(e)})", exc_info=True)
        return json.dumps(error_response, ensure_ascii=False), 500, {'Content-Type': 'application/json'}


# ==========================================
# メインハンドラー
# ==========================================
@functions_framework.http
def trigger_workflow(request):
    # ─── 【NEW】 Eventarcからのリクエストなら専用処理へ逃がして終了 ───
    if request.headers.get('ce-id'):
        return handle_eventarc_request(request)


    # ─── 以下、既存のスケジューラ起動ソース（完全無修正） ───
    # ─── 1. カスタムヘッダーによる事前ウォーミング（空振り）の判定 ───
    # Scheduler側で "X-Warmup-Request: true" が付与されているかチェック
    is_warmup_request = request.headers.get('X-Warmup-Request', '').lower() == 'true'

    # 2. Cloud Scheduler が付与した「スケジュール時刻」を取得し、現在時刻と比較
    raw_schedule_time = request.headers.get('X-CloudScheduler-ScheduleTime', '')
    current_time = datetime.now(timezone.utc)

    is_manual_execution = False

    if raw_schedule_time:
        # ISO形式の文字列をUTCのdatetimeオブジェクトに変換
        clean_string = raw_schedule_time.replace('Z', '+00:00')
        header_dt = datetime.fromisoformat(clean_string).astimezone(timezone.utc)

        #【手動実行の判定】ヘッダーの予定時刻が、現在時刻よりも未来の場合は手動実行とみなす
        if header_dt >= current_time:
            is_manual_execution = True
            target_dt = current_time
        else:
            target_dt = header_dt
    else:
        # ヘッダー自体が存在しない場合も現在時刻を使用
        target_dt = current_time

    # Cloud Run Job用のフォーマットに変換
    run_job_scheduled_time = format_to_job_utc(target_dt)

    # 3. Cloud Scheduler の「本文（Body）」からJSONデータを取得
    request_json = request.get_json(silent=True) or {}

    try:
        validated_request = SchedulerRequest(**request_json)

        # 環境変数が設定されていない場合の安全チェック
        if not WF_PROJECT_ID or not WF_LOCATION:
            raise ValueError("環境変数 'WF_PROJECT_ID' または 'LOCATION' が設定されていません。")

    except (ValidationError, ValueError) as e:
        error_msg = e.errors() if isinstance(e, ValidationError) else str(e)
        error_response = {
            "status": "Bad Request",
            "message": "起動パラメータまたは環境変数に不備があります。",
            "errors": error_msg
        }
        logger.error(f"【バリデーションエラー】起動パラメータまたは環境変数に不備があります。詳細: {error_msg}")
        return json.dumps(error_response, ensure_ascii=False), 400, {'Content-Type': 'application/json'}

    # ─── 4. ウォーミング（空振り）モードなら、ここで安全に終了 ───
    if is_warmup_request:
        logger.info("==== コンテナのウォーミングが完了しました。ワークフローの実行をスキップします。 ====")
        success_response = {
            "status": "Success",
            "message": "Container successfully warmed up. Workflow execution skipped."
        }
        return json.dumps(success_response, ensure_ascii=False), 200, {'Content-Type': 'application/json'}

    # 5. Workflows への引数（JSON）を組み立てる
    workflow_args = validated_request.model_dump()
    workflow_args['scheduled_time'] = run_job_scheduled_time

    # ─── 変換前・実行モード・変換後のタイムスタンプをログに出力 ───
    logger.info(f"[Raw Timestamp] Received raw schedule_time: {raw_schedule_time if raw_schedule_time else 'None'}")

    if is_manual_execution:
        logger.info(f"[Execution Mode] **手動（強制）実行を検知** (ヘッダー時刻が未来のため)。現在時刻に上書きしました: {current_time.isoformat()}")
    else:
        logger.info(f"[Execution Mode] 通常のスケジュール自動実行です。")

    logger.info(f"[Target Timestamp] scheduled_time generated for Cloud Run Jobs: {run_job_scheduled_time}")

    # 6. Workflows Client を使って動的に指定されたワークフローを実行
    parent = f"projects/{WF_PROJECT_ID}/locations/{WF_LOCATION}/workflows/{validated_request.workflow_id}"

    logger.info(f"Attempting to trigger workflow: {validated_request.workflow_id} for Cloud Run Jobs execution date: {run_job_scheduled_time}")

    # ─── 7. GCP例外ハンドリング ───
    try:
        execution = executions_v1.Execution(argument=json.dumps(workflow_args))
        response = _client.create_execution(parent=parent, execution=execution)

        logger.info(f"ワークフローを正常に起動しました。対象: {validated_request.workflow_id} | 実行名: {response.name}")

        success_response = {
            "status": "Success",
            "message": f"Workflow execution started for {validated_request.workflow_id}",
            "execution_name": response.name
        }
        return json.dumps(success_response, ensure_ascii=False), 200, {'Content-Type': 'application/json'}

    except exceptions.NotFound as e:
        # ワークフローが存在しない（404）
        error_msg = f"指定されたワークフロー '{validated_request.workflow_id}' が見つかりませんでした。"
        error_response = {
            "status": "Not Found",
            "message": error_msg,
            "details": e.message
        }
        logger.error(f"【404エラー】{error_msg} (パス: {parent} | 詳細: {e.message})")
        return json.dumps(error_response, ensure_ascii=False), 404, {'Content-Type': 'application/json'}

    except exceptions.PermissionDenied as e:
        # 権限が足りない（403）
        error_msg = "ワークフローを起動する権限がありません。"
        error_response = {
            "status": "Permission Denied",
            "message": error_msg,
            "details": e.message
        }
        logger.error(f"【403エラー】{error_msg} (詳細: {e.message})")
        return json.dumps(error_response, ensure_ascii=False), 403, {'Content-Type': 'application/json'}

    except exceptions.GoogleAPICallError as e:
        # その他のGCP APIエラー（500系など）
        error_msg = "Google Cloud APIの呼び出し中にエラーが発生しました。"
        error_response = {
            "status": "GCP API Error",
            "message": error_msg,
            "details": e.message
        }
        logger.error(f"【GCP APIエラー】{error_msg} (詳細: {e.message})")
        return json.dumps(error_response, ensure_ascii=False), 500, {'Content-Type': 'application/json'}

    except Exception as e:
        # 予期せぬその他のシステムエラー
        error_msg = "プログラム内部で予期せぬエラーが発生しました。"
        error_response = {
            "status": "Internal Server Error",
            "message": error_msg,
            "details": str(e)
        }
        logger.error(f"【500システムエラー】{error_msg} (詳細: {str(e)})", exc_info=True)
        return json.dumps(error_response, ensure_ascii=False), 500, {'Content-Type': 'application/json'}