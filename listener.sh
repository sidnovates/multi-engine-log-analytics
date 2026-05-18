#!/usr/bin/env bash
set -euo pipefail

# Load environment variables from .env file
if [ -f .env ]; then
  export $(grep -v '^#' .env | xargs)
fi

# ROOT directory of the project
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Classpath and target settings
CLASSPATH_FILE="$ROOT/.classpath"
TARGET_DIR="$ROOT/target"
POM_FILE="$ROOT/pom.xml"

# Function to generate classpath if missing or pom.xml is newer
generate_classpath() {
    if [ ! -f "$CLASSPATH_FILE" ] || [ "$POM_FILE" -nt "$CLASSPATH_FILE" ]; then
        echo "📦 Generating project classpath (this may take a minute)..."
        mvn -f "$POM_FILE" dependency:build-classpath -Dmdep.outputFile="$CLASSPATH_FILE" > /dev/null
    fi
}

# Function to run analysis
run_analysis() {
    local exec_id=$1
    echo "📊 Running Master Analysis for Execution ID: $exec_id..."
    generate_classpath
    mvn -q -f "$POM_FILE" exec:java -Dexec.mainClass="analysis.MasterAnalyzer" -Dexec.args="$exec_id"
    # Notify frontend that analysis is complete
    echo "COMPLETED_ANALYSIS" > "$ROOT/.pipeline_status"
}

# Ensure classpath exists
generate_classpath

echo "🎧 Terminal Listener Active. Waiting for commands from UI..."

# Monitor the .pipeline_queue file
while true; do
    if [ -f "$ROOT/.pipeline_queue" ]; then
        CMD_DATA=$(cat "$ROOT/.pipeline_queue")
        rm "$ROOT/.pipeline_queue"

        # Check if the command is for analysis
        if [[ $CMD_DATA == ANALYZE* ]]; then
            EXEC_ID=$(echo $CMD_DATA | awk '{print $2}')
            run_analysis "$EXEC_ID"
        else
            # It's a pipeline run: dataset method batchSize querySelect
            DS=$(echo $CMD_DATA | awk '{print $1}')
            METHOD=$(echo $CMD_DATA | awk '{print $2}')
            BATCH=$(echo $CMD_DATA | awk '{print $3}')
            QUERY=$(echo $CMD_DATA | awk '{print $4}')
            if [ -z "$QUERY" ]; then
                QUERY="all"
            fi

            echo "🚀 Starting Pipeline: $METHOD on $DS (Query: $QUERY)"
            
            # Run the PipelineRunner
            mvn -q -f "$POM_FILE" exec:java -Dexec.mainClass="common.PipelineRunner_V2" -Dexec.args="$DS $METHOD $BATCH $QUERY"

            # Get the execution ID from the .latest_execution_id file
            if [ -f "$ROOT/.latest_execution_id" ]; then
                NEW_ID=$(cat "$ROOT/.latest_execution_id")
                echo "COMPLETED_PIPELINE $NEW_ID" > "$ROOT/.pipeline_status"
            else
                echo "COMPLETED_PIPELINE" > "$ROOT/.pipeline_status"
            fi
        fi
    fi
    sleep 1
done
