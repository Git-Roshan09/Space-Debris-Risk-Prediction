#!/bin/bash

# Simple simulation time advancement script using curl

API_URL="http://localhost:5001"

advance_simulation() {
    local days=${1:-1}
    echo "🚀 Advancing simulation by $days day(s)..."
    
    response=$(curl -s -X POST "$API_URL/api/simulation/advance" \
        -H "Content-Type: application/json" \
        -d "{\"days\": $days}")
    
    if [ $? -eq 0 ]; then
        echo "✅ Success:"
        echo "$response" | jq -r '"   Time: " + .current_simulated_time + " (Day " + (.elapsed_days | tostring) + ")"'
    else
        echo "❌ Error connecting to API"
        return 1
    fi
}

get_status() {
    echo "🕐 Getting simulation status..."
    
    response=$(curl -s "$API_URL/api/simulation/time")
    
    if [ $? -eq 0 ]; then
        echo "📊 Current Status:"
        echo "$response" | jq -r '"   Time: " + .current_simulated_time + " (Day " + (.elapsed_simulated_days | tostring) + ")"'
        echo "$response" | jq -r '"   Mode: " + .simulation_mode'
    else
        echo "❌ Error connecting to API"
        return 1
    fi
}

case "$1" in
    "status"|"--status")
        get_status
        ;;
    "help"|"--help")
        echo "Usage:"
        echo "  ./advance_sim.sh         # Advance by 1 day"
        echo "  ./advance_sim.sh 5       # Advance by 5 days"  
        echo "  ./advance_sim.sh status  # Show current status"
        ;;
    "")
        advance_simulation 1
        ;;
    *)
        if [[ "$1" =~ ^[0-9]+$ ]]; then
            advance_simulation "$1"
        else
            echo "Error: Invalid argument '$1'"
            echo "Use './advance_sim.sh help' for usage"
            exit 1
        fi
        ;;
esac