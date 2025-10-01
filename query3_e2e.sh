#!/bin/bash

# This script runs the end-to-end test for query3

SESSION="query3_e2e"

# Kill previous session if it exists
tmux has-session -t $SESSION 2>/dev/null && tmux kill-session -t $SESSION

# Start session running the first command (server)
tmux new-session -d -s $SESSION "make run-server"

# Add gateway (split right of server)
tmux split-window -h
tmux send-keys "make run-gateway" C-m
sleep 0.5

# Add filter (split below server)
tmux split-window -v -t 0
tmux send-keys "make run-filter RUN_FUNCTION=stores" C-m
sleep 0.5

# Add filter (split below gateway)
tmux split-window -v -t 1
tmux send-keys "make run-filter RUN_FUNCTION=transactions" C-m
sleep 0.5

# Add aggregator (split below stores)
tmux split-window -v -t 2
tmux send-keys "make run-aggregator RUN_FUNCTION=query3" C-m
sleep 0.5

# Add aggregator with query3global function (split below transactions)
tmux split-window -v -t 3
tmux send-keys "make run-aggregator RUN_FUNCTION=query3global" C-m
sleep 0.5

# Add concat (split below server again or pick pane 4)
tmux split-window -v -t 0
tmux send-keys "make run-concat" C-m
sleep 0.5

# Add sender (split below gateway again or pick pane 5)
tmux split-window -v -t 1
tmux send-keys "make run-sender" C-m
sleep 0.5

# Add joiner with Query3 function (split below concat)
tmux split-window -v -t 6
tmux send-keys "make run-joiner RUN_FUNCTION=Query3" C-m
sleep 0.5

# Attach to session
tmux set-option -t $SESSION mouse on
tmux attach-session -t $SESSION
