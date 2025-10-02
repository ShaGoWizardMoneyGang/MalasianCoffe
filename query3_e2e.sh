#!/bin/bash

# This script runs the end-to-end test for query3

SESSION="query3_e2e"

# Kill previous session if it exists
tmux has-session -t $SESSION 2>/dev/null && tmux kill-session -t $SESSION

# Start session with an idle shell (pane 0)
tmux new-session -d -s $SESSION

# Right pane (pane 1) for stores filter
tmux split-window -h
tmux send-keys -t 1 "make run-filter RUN_FUNCTION=stores" C-m

# Left pane (pane 0) runs everything else in background
tmux send-keys -t 0 "make run-server &" C-m
tmux send-keys -t 0 "make run-gateway &" C-m
tmux send-keys -t 0 "make run-filter RUN_FUNCTION=transactions &" C-m
tmux send-keys -t 0 "make run-partial-aggregator RUN_FUNCTION=query3 &" C-m
tmux send-keys -t 0 "make run-global-aggregator RUN_FUNCTION=query3Global &" C-m
tmux send-keys -t 0 "make run-sender RUN_FUNCTION=Query3 &" C-m
tmux send-keys -t 0 "make run-joiner RUN_FUNCTION=Query3 &" C-m
tmux send-keys -t 0 "wait" C-m

# Enable mouse and attach
tmux set-option -t $SESSION mouse on
tmux attach-session -t $SESSION
