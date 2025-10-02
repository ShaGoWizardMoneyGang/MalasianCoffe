#!/bin/bash

# This script runs the end-to-end test for query1

# Kill previous session if it exists
tmux has-session -t query1_e2e 2>/dev/null && tmux kill-session -t query1_e2e

# Start session running the first command (server)
tmux new-session -d -s query1_e2e "make run-server"

# Add gateway
tmux split-window -h
tmux send-keys "make run-gateway" C-m
sleep 0.5

# Add filter
tmux split-window -v -t 0
tmux send-keys "make run-filter RUN_FUNCTION=transactions" C-m
sleep 0.5

# Add concat
tmux split-window -v -t 2
tmux send-keys "make run-concat" C-m
sleep 0.5

# Add sender
tmux split-window -v -t 3
tmux send-keys "make run-sender RUN_FUNCTION=Query1" C-m

# Enable mouse and attach
tmux set-option -t query1_e2e mouse on
tmux attach-session -t query1_e2e
