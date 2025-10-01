#!/bin/bash

# This script runs the end-to-end test for query3

# Kill previous session if it exists
tmux has-session -t query3_e2e 2>/dev/null && tmux kill-session -t query3_e2e

# Start session running the first command (server)
tmux new-session -d -s query3_e2e "make run-server"

# Add gateway
tmux split-window -h
tmux send-keys "make run-gateway" C-m
sleep 0.5

# Add filter
tmux split-window -v -t 0
tmux send-keys "make run-filter RUN_FUNCTION=stores" C-m
sleep 0.5

# Add filter
tmux split-window -v -t 0
tmux send-keys "make run-filter RUN_FUNCTION=transactions" C-m
sleep 0.5

# Add aggregator
tmux split-window -v -t 0
tmux send-keys "make run-aggregator RUN_FUNCTION=query3" C-m
sleep 0.5

# Add concat
tmux split-window -v -t 2
tmux send-keys "make run-concat" C-m
sleep 0.5

# Add sender
tmux split-window -v -t 3
tmux send-keys "make run-sender" C-m

# Attach to session
tmux set-option -t query3_e2e mouse on
tmux attach-session -t query3_e2e
