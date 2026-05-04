#!/usr/bin/env bash
# deploy_aws.sh - Launch an EC2 instance to run the envbench data pipeline.
#
# Usage:
#   ./scripts/deploy_aws.sh              # launch + run full pipeline
#   ./scripts/deploy_aws.sh --dry-run    # print user-data script, don't launch
#   ./scripts/deploy_aws.sh --status     # check running envbench instances
#
# Prerequisites:
#   - AWS CLI v2 configured with credentials for account 099841456154
#   - GitHub SSH deploy key or personal token (the instance uses HTTPS + token)
#   - IAM instance profile "gaia-benchmark-ec2" exists (already created)
#
# The script:
#   1. Creates a security group (if needed) allowing outbound-only traffic
#   2. Launches a t3.xlarge (4 vCPU, 16 GB RAM) with Amazon Linux 2023
#   3. The user-data script installs deps, clones repo, runs pipeline,
#      commits results, and terminates the instance on completion.

set -euo pipefail

# ── Configuration ────────────────────────────────────────────────────────────
REGION="us-east-1"
INSTANCE_TYPE="t3.xlarge"
INSTANCE_PROFILE="gaia-benchmark-ec2"
REPO_URL="https://github.com/anishesg/envbench.git"
BRANCH="main"
SG_NAME="envbench-pipeline-sg"
TAG_NAME="envbench-pipeline"

# Amazon Linux 2023 latest AMI (resolved dynamically)
AMI_ID=$(aws ec2 describe-images \
    --owners amazon \
    --filters "Name=name,Values=al2023-ami-2023*-x86_64" "Name=state,Values=available" \
    --query 'Images | sort_by(@, &CreationDate) | [-1].ImageId' \
    --output text \
    --region "$REGION" 2>/dev/null)

if [[ -z "$AMI_ID" || "$AMI_ID" == "None" ]]; then
    echo "ERROR: Could not resolve Amazon Linux 2023 AMI. Check AWS CLI config."
    exit 1
fi

# ── GitHub token ─────────────────────────────────────────────────────────────
# The instance needs a way to push commits back.  We read a GitHub personal
# access token from the environment or from gh CLI auth.
if [[ -n "${GITHUB_TOKEN:-}" ]]; then
    GH_TOKEN="$GITHUB_TOKEN"
else
    GH_TOKEN=$(gh auth token 2>/dev/null || true)
fi

if [[ -z "$GH_TOKEN" ]]; then
    echo "ERROR: No GitHub token found. Set GITHUB_TOKEN or run 'gh auth login'."
    exit 1
fi

# ── Helper: status check ────────────────────────────────────────────────────
if [[ "${1:-}" == "--status" ]]; then
    echo "=== Running envbench-pipeline instances ==="
    aws ec2 describe-instances \
        --filters "Name=tag:Name,Values=$TAG_NAME" \
            "Name=instance-state-name,Values=pending,running,stopping" \
        --query 'Reservations[*].Instances[*].[InstanceId,State.Name,LaunchTime,PublicIpAddress]' \
        --output table \
        --region "$REGION"
    exit 0
fi

# ── Security group (outbound-only, no inbound) ──────────────────────────────
SG_ID=$(aws ec2 describe-security-groups \
    --filters "Name=group-name,Values=$SG_NAME" \
    --query 'SecurityGroups[0].GroupId' \
    --output text \
    --region "$REGION" 2>/dev/null || echo "None")

if [[ "$SG_ID" == "None" || -z "$SG_ID" ]]; then
    echo "Creating security group '$SG_NAME' (outbound-only)..."
    SG_ID=$(aws ec2 create-security-group \
        --group-name "$SG_NAME" \
        --description "envbench pipeline - outbound only, no SSH" \
        --region "$REGION" \
        --output text \
        --query 'GroupId')

    # Revoke the default allow-all inbound rule that AWS adds
    aws ec2 revoke-security-group-ingress \
        --group-id "$SG_ID" \
        --protocol -1 \
        --port -1 \
        --cidr 0.0.0.0/0 \
        --region "$REGION" 2>/dev/null || true

    echo "  Created: $SG_ID"
else
    echo "Using existing security group: $SG_ID"
fi

# ── Build user-data script ──────────────────────────────────────────────────
USER_DATA=$(cat <<'USERDATA_OUTER'
#!/bin/bash
set -euxo pipefail
exec > >(tee /var/log/envbench-pipeline.log) 2>&1

echo "========================================="
echo "envbench pipeline starting at $(date -u)"
echo "========================================="

# --- System packages ---
# geopandas/shapely/pyogrio wheels bundle their own GDAL/GEOS/PROJ libs,
# so we only need Python + basic build tools.
dnf install -y git python3.12 python3.12-pip gcc gcc-c++

# --- Install uv ---
curl -LsSf https://astral.sh/uv/install.sh | sh
export PATH="$HOME/.local/bin:$PATH"

# --- Clone repo ---
cd /home/ec2-user
USERDATA_OUTER
)

# Inject the token (this part is NOT inside the heredoc to avoid quoting issues)
USER_DATA+="
git clone https://${GH_TOKEN}@github.com/anishesg/envbench.git
cd envbench
git checkout ${BRANCH}
"

USER_DATA+=$(cat <<'USERDATA_INNER'

# --- Python environment ---
uv sync

# --- Create .env ---
cat > .env << 'DOTENV'
CENSUS_API_KEY=
NOAA_API_KEY=
AWS_DEFAULT_REGION=us-east-1
DOTENV

# --- Run pipeline ---
echo "Pipeline start: $(date -u)"
set +e
uv run python scripts/run_pipeline.py 2>&1 | tee /var/log/envbench-run.log
PIPELINE_EXIT=${PIPESTATUS[0]}
set -e
echo "Pipeline exit code: $PIPELINE_EXIT at $(date -u)"

if [[ $PIPELINE_EXIT -eq 0 ]]; then
    # --- Commit and push results ---
    git config user.name "envbench-bot"
    git config user.email "envbench-bot@noreply.github.com"

    # Force-add data files even though data/ is in .gitignore
    git add -f data/benchmark/*.parquet data/benchmark/metadata.json \
             data/benchmark/hf_release/ 2>/dev/null || true

    if ! git diff --cached --quiet; then
        git commit -m "pipeline: add benchmark data from EC2 run $(date -u +%Y-%m-%dT%H:%M:%SZ)

Generated on $(curl -s -H "X-aws-ec2-metadata-token: $(curl -s -X PUT http://169.254.169.254/latest/api/token -H 'X-aws-ec2-metadata-token-ttl-seconds: 30')" http://169.254.169.254/latest/meta-data/instance-id 2>/dev/null || echo 'ec2')
Pipeline completed successfully."

        # Push with retries
        for attempt in 1 2 3; do
            if git push origin "${BRANCH:-main}"; then
                echo "Push succeeded on attempt $attempt"
                break
            fi
            echo "Push attempt $attempt failed, retrying in 10s..."
            sleep 10
        done
    else
        echo "No new benchmark files to commit."
    fi
else
    echo "ERROR: Pipeline failed with exit code $PIPELINE_EXIT"
fi

echo "========================================="
echo "envbench pipeline finished at $(date -u)"
echo "========================================="

# --- Self-terminate (IMDSv2 token-based) ---
IMDS_TOKEN=$(curl -s -X PUT "http://169.254.169.254/latest/api/token" \
    -H "X-aws-ec2-metadata-token-ttl-seconds: 60")
INSTANCE_ID=$(curl -s -H "X-aws-ec2-metadata-token: $IMDS_TOKEN" \
    http://169.254.169.254/latest/meta-data/instance-id)
AZ=$(curl -s -H "X-aws-ec2-metadata-token: $IMDS_TOKEN" \
    http://169.254.169.254/latest/meta-data/placement/availability-zone)
REGION="${AZ%?}"
aws ec2 terminate-instances --instance-ids "$INSTANCE_ID" --region "$REGION"
USERDATA_INNER
)

# ── Dry-run mode ─────────────────────────────────────────────────────────────
if [[ "${1:-}" == "--dry-run" ]]; then
    echo "=== User-data script (dry run) ==="
    echo "$USER_DATA"
    echo ""
    echo "AMI:              $AMI_ID"
    echo "Instance type:    $INSTANCE_TYPE"
    echo "Instance profile: $INSTANCE_PROFILE"
    echo "Security group:   $SG_ID"
    echo "Region:           $REGION"
    exit 0
fi

# ── Launch instance ──────────────────────────────────────────────────────────
echo ""
echo "Launching EC2 instance..."
echo "  AMI:              $AMI_ID"
echo "  Instance type:    $INSTANCE_TYPE"
echo "  Instance profile: $INSTANCE_PROFILE"
echo "  Security group:   $SG_ID"

INSTANCE_ID=$(aws ec2 run-instances \
    --image-id "$AMI_ID" \
    --instance-type "$INSTANCE_TYPE" \
    --iam-instance-profile "Name=$INSTANCE_PROFILE" \
    --security-group-ids "$SG_ID" \
    --user-data "$USER_DATA" \
    --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=$TAG_NAME}]" \
    --block-device-mappings '[{"DeviceName":"/dev/xvda","Ebs":{"VolumeSize":50,"VolumeType":"gp3","DeleteOnTermination":true}}]' \
    --metadata-options "HttpTokens=required,HttpEndpoint=enabled" \
    --region "$REGION" \
    --query 'Instances[0].InstanceId' \
    --output text)

echo ""
echo "=== Instance launched ==="
echo "  Instance ID: $INSTANCE_ID"
echo "  Region:      $REGION"
echo ""
echo "The instance will:"
echo "  1. Install Python 3.12, uv, git, GDAL"
echo "  2. Clone the repo and run the full pipeline (~2-4 hours)"
echo "  3. Commit and push benchmark data to GitHub"
echo "  4. Self-terminate when done"
echo ""
echo "Monitor progress:"
echo "  ./scripts/deploy_aws.sh --status"
echo "  aws ec2 get-console-output --instance-id $INSTANCE_ID --region $REGION --output text"
echo ""
echo "To cancel:"
echo "  aws ec2 terminate-instances --instance-ids $INSTANCE_ID --region $REGION"
