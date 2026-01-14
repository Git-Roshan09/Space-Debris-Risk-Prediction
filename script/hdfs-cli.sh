#!/bin/bash
# HDFS Helper Script - Quick commands for managing HDFS

echo "🗂️  HDFS Management Commands"
echo "=============================="
echo ""

case "${1:-help}" in
    "ls"|"list")
        echo "📂 Listing HDFS contents..."
        docker exec namenode hdfs dfs -ls ${2:-/}
        ;;
    
    "browse"|"web")
        echo "🌐 Open HDFS Web UI at: http://localhost:9870"
        echo "   Browse filesystem: http://localhost:9870/explorer.html"
        ;;
    
    "mkdir")
        if [ -z "$2" ]; then
            echo "❌ Usage: $0 mkdir <path>"
            exit 1
        fi
        echo "📁 Creating directory: $2"
        docker exec namenode hdfs dfs -mkdir -p "$2"
        ;;
    
    "rm"|"delete")
        if [ -z "$2" ]; then
            echo "❌ Usage: $0 rm <path>"
            exit 1
        fi
        echo "🗑️  Removing: $2"
        docker exec namenode hdfs dfs -rm -r "$2"
        ;;
    
    "cat"|"read")
        if [ -z "$2" ]; then
            echo "❌ Usage: $0 cat <file>"
            exit 1
        fi
        echo "📄 Reading file: $2"
        docker exec namenode hdfs dfs -cat "$2"
        ;;
    
    "du"|"usage")
        echo "💾 Disk usage for: ${2:-/}"
        docker exec namenode hdfs dfs -du -h ${2:-/}
        ;;
    
    "sgp4")
        echo "📊 Checking SGP4 vectors in HDFS..."
        docker exec namenode hdfs dfs -ls /space-debris/sgp4_vectors/ 2>/dev/null || \
            echo "⚠️  No SGP4 data found yet. Run the Spark pipeline first."
        ;;
    
    "count")
        echo "🔢 Counting files in: ${2:-/space-debris/sgp4_vectors/}"
        docker exec namenode hdfs dfs -count -h ${2:-/space-debris/sgp4_vectors/}
        ;;
    
    "report")
        echo "📈 HDFS Health Report:"
        docker exec namenode hdfs dfsadmin -report
        ;;
    
    "help"|*)
        echo "Available commands:"
        echo "  $0 ls [path]        - List HDFS directory contents"
        echo "  $0 browse           - Show HDFS Web UI URL"
        echo "  $0 mkdir <path>     - Create directory in HDFS"
        echo "  $0 rm <path>        - Remove file/directory from HDFS"
        echo "  $0 cat <file>       - Display file contents"
        echo "  $0 du [path]        - Show disk usage"
        echo "  $0 sgp4             - Check SGP4 vectors data"
        echo "  $0 count [path]     - Count files in directory"
        echo "  $0 report           - Show HDFS health report"
        echo ""
        echo "Examples:"
        echo "  $0 ls /space-debris/sgp4_vectors/"
        echo "  $0 sgp4"
        echo "  $0 browse"
        ;;
esac
