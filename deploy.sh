#!/usr/bin/env bash
# VG710 NMEA downsampler — deployment script (direct AWS CLI, geen CloudFormation)
# Gebruik: ./deploy.sh [--backfill] [--date-from YYYY-MM-DD] [--date-to YYYY-MM-DD]

set -euo pipefail

BUCKET="bmc-vg710-raw-eun1"
REGION="eu-north-1"
ACCOUNT_ID="203918854595"
ZIP_KEY="lambda/vg710-nmea-downsampler.zip"
LAMBDA_NAME="vg710-nmea-downsampler"
ROLE_NAME="vg710-nmea-downsampler-role"
DLQ_NAME="vg710-nmea-downsampler-dlq"
RULE_NAME="vg710-nmea-new-file"
RAW_PREFIX="vg710-raw"
OUTPUT_PREFIX="vg710-5min"
WINDOW_MINUTES="5"
ATHENA_DB="vg710_db"
ATHENA_OUTPUT="s3://bmc-vg710-athena-results-eun1/"

RUN_BACKFILL=false
BACKFILL_FROM=""
BACKFILL_TO=""
for arg in "$@"; do
  case $arg in
    --backfill)      RUN_BACKFILL=true ;;
    --date-from=*)   BACKFILL_FROM="${arg#*=}" ;;
    --date-to=*)     BACKFILL_TO="${arg#*=}" ;;
  esac
done

GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; NC='\033[0m'
log()  { echo -e "${GREEN}[$(date +%H:%M:%S)]${NC} $*"; }
warn() { echo -e "${YELLOW}[$(date +%H:%M:%S)] LET OP:${NC} $*"; }
err()  { echo -e "${RED}[$(date +%H:%M:%S)] FOUT:${NC} $*" >&2; exit 1; }

athena_run() {
  local sql="$1"
  local qid
  qid=$(aws athena start-query-execution \
    --query-string "$sql" \
    --query-execution-context Database="$ATHENA_DB" \
    --result-configuration OutputLocation="$ATHENA_OUTPUT" \
    --region "$REGION" \
    --query "QueryExecutionId" --output text)
  for i in $(seq 1 30); do
    local state
    state=$(aws athena get-query-execution \
      --query-execution-id "$qid" --region "$REGION" \
      --query "QueryExecution.Status.State" --output text)
    [ "$state" = "SUCCEEDED" ] && return 0
    if [ "$state" = "FAILED" ]; then
      local reason
      reason=$(aws athena get-query-execution \
        --query-execution-id "$qid" --region "$REGION" \
        --query "QueryExecution.Status.StateChangeReason" --output text)
      echo "$reason" | grep -qi "already exists" && return 0
      err "Athena query mislukt: $reason"
    fi
    sleep 2
  done
  err "Athena query timeout"
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# ---------------------------------------------------------------------------
log "Stap 0: AWS credentials controleren..."
# ---------------------------------------------------------------------------
aws sts get-caller-identity --region "$REGION" >/dev/null \
  || err "Geen geldige AWS credentials"
log "Account: $ACCOUNT_ID, Region: $REGION"

# ---------------------------------------------------------------------------
log "Stap 1: Lambda code inpakken en uploaden..."
# ---------------------------------------------------------------------------
TMP_ZIP=$(mktemp /tmp/vg710-lambda-XXXX)
TMP_ZIP="${TMP_ZIP}.zip"
cd "$SCRIPT_DIR/lambda/downsample_nmea"
zip -q "$TMP_ZIP" handler.py
cd "$SCRIPT_DIR"
aws s3 cp "$TMP_ZIP" "s3://${BUCKET}/${ZIP_KEY}" --region "$REGION"
rm -f "$TMP_ZIP"
log "Lambda zip geüpload: s3://${BUCKET}/${ZIP_KEY}"

# ---------------------------------------------------------------------------
log "Stap 2: SQS Dead Letter Queue aanmaken..."
# ---------------------------------------------------------------------------
DLQ_URL=$(aws sqs create-queue \
  --queue-name "$DLQ_NAME" \
  --attributes MessageRetentionPeriod=1209600 \
  --region "$REGION" \
  --query "QueueUrl" --output text 2>/dev/null || \
  aws sqs get-queue-url --queue-name "$DLQ_NAME" \
    --region "$REGION" --query "QueueUrl" --output text)

DLQ_ARN=$(aws sqs get-queue-attributes \
  --queue-url "$DLQ_URL" \
  --attribute-names QueueArn \
  --region "$REGION" \
  --query "Attributes.QueueArn" --output text)
log "DLQ: $DLQ_ARN"

# ---------------------------------------------------------------------------
log "Stap 3: IAM rol aanmaken..."
# ---------------------------------------------------------------------------
ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${ROLE_NAME}"

# Maak de rol aan (negeer fout als al bestaat)
aws iam create-role \
  --role-name "$ROLE_NAME" \
  --assume-role-policy-document '{
    "Version": "2012-10-17",
    "Statement": [{
      "Effect": "Allow",
      "Principal": {"Service": "lambda.amazonaws.com"},
      "Action": "sts:AssumeRole"
    }]
  }' \
  --region "$REGION" >/dev/null 2>/dev/null || true

# Basisrechten voor CloudWatch Logs
aws iam attach-role-policy \
  --role-name "$ROLE_NAME" \
  --policy-arn "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole" \
  2>/dev/null || true

# S3 lezen (ruw) en schrijven (output)
aws iam put-role-policy \
  --role-name "$ROLE_NAME" \
  --policy-name "S3ReadWrite" \
  --policy-document "{
    \"Version\": \"2012-10-17\",
    \"Statement\": [
      {
        \"Effect\": \"Allow\",
        \"Action\": \"s3:GetObject\",
        \"Resource\": \"arn:aws:s3:::${BUCKET}/${RAW_PREFIX}/*\"
      },
      {
        \"Effect\": \"Allow\",
        \"Action\": \"s3:PutObject\",
        \"Resource\": \"arn:aws:s3:::${BUCKET}/${OUTPUT_PREFIX}/*\"
      },
      {
        \"Effect\": \"Allow\",
        \"Action\": \"sqs:SendMessage\",
        \"Resource\": \"${DLQ_ARN}\"
      }
    ]
  }" --region "$REGION"

log "IAM rol: $ROLE_ARN"
# Wacht even zodat de rol propagated
sleep 8

# ---------------------------------------------------------------------------
log "Stap 4: Lambda functie aanmaken of updaten..."
# ---------------------------------------------------------------------------
if aws lambda get-function --function-name "$LAMBDA_NAME" --region "$REGION" >/dev/null 2>&1; then
  # Update bestaande functie
  aws lambda update-function-code \
    --function-name "$LAMBDA_NAME" \
    --s3-bucket "$BUCKET" \
    --s3-key "$ZIP_KEY" \
    --region "$REGION" >/dev/null
  aws lambda wait function-updated \
    --function-name "$LAMBDA_NAME" --region "$REGION"
  aws lambda update-function-configuration \
    --function-name "$LAMBDA_NAME" \
    --timeout 60 \
    --memory-size 128 \
    --environment "Variables={OUTPUT_PREFIX=${OUTPUT_PREFIX},WINDOW_MINUTES=${WINDOW_MINUTES}}" \
    --dead-letter-config "TargetArn=${DLQ_ARN}" \
    --region "$REGION" >/dev/null
  aws lambda wait function-updated \
    --function-name "$LAMBDA_NAME" --region "$REGION"
  log "Lambda bijgewerkt"
else
  # Nieuwe functie aanmaken
  aws lambda create-function \
    --function-name "$LAMBDA_NAME" \
    --description "Schrijft 5-minuten GPS posities naar S3 bij elk nieuw NMEA bestand" \
    --runtime python3.12 \
    --handler handler.handler \
    --role "$ROLE_ARN" \
    --code "S3Bucket=${BUCKET},S3Key=${ZIP_KEY}" \
    --timeout 60 \
    --memory-size 128 \
    --environment "Variables={OUTPUT_PREFIX=${OUTPUT_PREFIX},WINDOW_MINUTES=${WINDOW_MINUTES}}" \
    --dead-letter-config "TargetArn=${DLQ_ARN}" \
    --region "$REGION" >/dev/null
  aws lambda wait function-active \
    --function-name "$LAMBDA_NAME" --region "$REGION"
  log "Lambda aangemaakt"
fi

LAMBDA_ARN="arn:aws:lambda:${REGION}:${ACCOUNT_ID}:function:${LAMBDA_NAME}"

# ---------------------------------------------------------------------------
log "Stap 5: EventBridge regel aanmaken..."
# ---------------------------------------------------------------------------
RULE_ARN=$(aws events put-rule \
  --name "$RULE_NAME" \
  --description "Trigger NMEA downsampler bij nieuw bestand in S3" \
  --event-pattern "{
    \"source\": [\"aws.s3\"],
    \"detail-type\": [\"Object Created\"],
    \"detail\": {
      \"bucket\": {\"name\": [\"${BUCKET}\"]},
      \"object\": {\"key\": [{\"prefix\": \"${RAW_PREFIX}/\"}]}
    }
  }" \
  --state ENABLED \
  --region "$REGION" \
  --query "RuleArn" --output text)
log "EventBridge regel: $RULE_ARN"

# Koppel Lambda als doel
aws events put-targets \
  --rule "$RULE_NAME" \
  --targets "Id=NmeaDownsampler,Arn=${LAMBDA_ARN}" \
  --region "$REGION" >/dev/null

# Toestemming voor EventBridge om Lambda aan te roepen
aws lambda add-permission \
  --function-name "$LAMBDA_NAME" \
  --statement-id "AllowEventBridge" \
  --action "lambda:InvokeFunction" \
  --principal "events.amazonaws.com" \
  --source-arn "$RULE_ARN" \
  --region "$REGION" >/dev/null 2>/dev/null || true
log "Lambda gekoppeld aan EventBridge"

# ---------------------------------------------------------------------------
log "Stap 6: EventBridge inschakelen op S3 bucket..."
# ---------------------------------------------------------------------------
CURRENT_CFG=$(aws s3api get-bucket-notification-configuration \
  --bucket "$BUCKET" --region "$REGION" 2>/dev/null || echo "")

if echo "$CURRENT_CFG" | grep -q "EventBridgeConfiguration"; then
  log "EventBridge was al ingeschakeld op de bucket"
else
  NEW_CFG=$(python3 -c "
import json
raw = '''${CURRENT_CFG}'''
cfg = json.loads(raw) if raw.strip() else {}
cfg['EventBridgeConfiguration'] = {}
print(json.dumps(cfg))
")
  aws s3api put-bucket-notification-configuration \
    --bucket "$BUCKET" \
    --notification-configuration "$NEW_CFG" \
    --region "$REGION"
  log "EventBridge ingeschakeld op $BUCKET"
fi

# ---------------------------------------------------------------------------
log "Stap 7: Athena tabel aanmaken..."
# ---------------------------------------------------------------------------
athena_run "$(cat "$SCRIPT_DIR/athena/create_nmea_5min_table.sql")"
log "Athena tabel vg710_db.nmea_5min klaar"

# ---------------------------------------------------------------------------
log "Stap 8: CloudWatch alarmen aanmaken..."
# ---------------------------------------------------------------------------
aws cloudwatch put-metric-alarm \
  --alarm-name "vg710-nmea-downsampler-dlq-niet-leeg" \
  --alarm-description "Mislukte NMEA downsampler aanroepen in DLQ" \
  --namespace AWS/SQS \
  --metric-name ApproximateNumberOfMessagesVisible \
  --dimensions "Name=QueueName,Value=${DLQ_NAME}" \
  --statistic Sum \
  --period 300 \
  --evaluation-periods 1 \
  --threshold 1 \
  --comparison-operator GreaterThanOrEqualToThreshold \
  --treat-missing-data notBreaching \
  --region "$REGION" >/dev/null

aws cloudwatch put-metric-alarm \
  --alarm-name "vg710-nmea-downsampler-fouten" \
  --alarm-description "Lambda functie geeft te veel fouten" \
  --namespace AWS/Lambda \
  --metric-name Errors \
  --dimensions "Name=FunctionName,Value=${LAMBDA_NAME}" \
  --statistic Sum \
  --period 300 \
  --evaluation-periods 1 \
  --threshold 5 \
  --comparison-operator GreaterThanOrEqualToThreshold \
  --treat-missing-data notBreaching \
  --region "$REGION" >/dev/null
log "CloudWatch alarmen aangemaakt"

# ---------------------------------------------------------------------------
if [ "$RUN_BACKFILL" = true ]; then
  log "Stap 9: Backfill historische data starten (20 workers)..."
  BACKFILL_ARGS=""
  [ -n "$BACKFILL_FROM" ] && BACKFILL_ARGS="$BACKFILL_ARGS --date-from $BACKFILL_FROM"
  [ -n "$BACKFILL_TO"   ] && BACKFILL_ARGS="$BACKFILL_ARGS --date-to $BACKFILL_TO"
  python3 "$SCRIPT_DIR/backfill/backfill_nmea.py" --workers 20 $BACKFILL_ARGS
fi
# ---------------------------------------------------------------------------

echo ""
log "=========================================="
log " Deployment succesvol afgerond"
log "=========================================="
log " Lambda:       $LAMBDA_NAME"
log " EventBridge:  actief ($RULE_NAME)"
log " DLQ:          $DLQ_NAME"
log " Athena tabel: vg710_db.nmea_5min"
log " Output S3:    s3://${BUCKET}/${OUTPUT_PREFIX}/"
log ""
log " Nieuwe VG710 apparaten worden automatisch"
log " opgepikt zodra ze data naar S3 sturen."
log "=========================================="
