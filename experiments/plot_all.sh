#!/bin/bash

SCRIPT=$(readlink -f $0)
SCRIPT_PATH=$(dirname "${SCRIPT}")

echo "plot Running Time Figure"
bash "${SCRIPT_PATH}/plot/running_time/plot.sh"
echo "output: ${SCRIPT_PATH}/output/figure/running_time/result.png"
echo ""

echo "plot Selectivity Q1 Epinions Figure"
bash "${SCRIPT_PATH}/plot/selectivity/q1_epinions/plot.sh"
echo "output: ${SCRIPT_PATH}/output/figure/selectivity/q1_epinions.png"
echo ""

echo "plot Selectivity Q2 Bitcoin Figure"
bash "${SCRIPT_PATH}/plot/selectivity/q2_bitcoin/plot.sh"
echo "output: ${SCRIPT_PATH}/output/figure/selectivity/q2_bitcoin.png"
echo ""

echo "plot Selectivity Q3 Epinions Figure"
bash "${SCRIPT_PATH}/plot/selectivity/q3_epinions/plot.sh"
echo "output: ${SCRIPT_PATH}/output/figure/selectivity/q3_epinions.png"
echo ""

echo "plot Parallel Processing Q1 Epinions Figure"
bash "${SCRIPT_PATH}/plot/parallel_processing/q1_epinions/plot.sh"
echo "output: ${SCRIPT_PATH}/output/figure/parallel_processing/q1_epinions.png"
echo ""

echo "plot Parallel Processing Q2 Bitcoin Figure"
bash "${SCRIPT_PATH}/plot/parallel_processing/q2_bitcoin/plot.sh"
echo "output: ${SCRIPT_PATH}/output/figure/parallel_processing/q2_bitcoin.png"
echo ""

echo "plot Parallel Processing Q3 Epinions Figure"
bash "${SCRIPT_PATH}/plot/parallel_processing/q3_epinions/plot.sh"
echo "output: ${SCRIPT_PATH}/output/figure/parallel_processing/q3_epinions.png"
echo ""

echo "plot Different Systems Figure"
bash "${SCRIPT_PATH}/plot/different_systems/plot.sh"
echo "output: ${SCRIPT_PATH}/output/figure/different_systems/result.png"
echo ""

echo "finish plotting."