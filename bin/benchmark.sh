#!/bin/sh
set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
BASE_DIR="$(dirname "${SCRIPT_DIR}")"
GEN_DIR="${BASE_DIR}/.gen"
GEN_BIN="${BASE_DIR}/target/release/gen"

init_test() {
  cd ${BASE_DIR}
  rm -rf ${GEN_DIR}
  ${GEN_BIN} init 2> /dev/null > /dev/null
}

get_size () {
  local filesize_mb=$(python -c "import os;size=os.path.getsize('${GEN_DIR}/default.db') / (1024 * 1024);print(f'{size:.4f}')")
  echo $filesize_mb
}

time_taken() {
  local start_time=$(date +%s%3N)
  "$@" > /dev/null 2> /dev/null
  local end_time=$(date +%s%3N)
  local duration=$((end_time - start_time))
  echo $duration
}


cargo build --release
echo "full import benchmark"
init_test
FULL_IMPORT=$(time_taken ${GEN_BIN} import -f ${BASE_DIR}/fixtures/chr22.fa.gz)
FULL_SIZE=$(get_size)
echo "shallow import benchmark"
init_test
SHALLOW_IMPORT=$(time_taken ${BASE_DIR}/target/release/gen import -f ${BASE_DIR}/fixtures/chr22.fa.gz --shallow)
SHALLOW_SIZE=$(get_size)
echo "Update with HG00096 benchmark"
init_test
${BASE_DIR}/target/release/gen import -f ${BASE_DIR}/fixtures/chr22.fa.gz --shallow 2> /dev/null > /dev/null
HG96_IMPORT=$(time_taken ${BASE_DIR}/target/release/gen update --vcf ${BASE_DIR}/fixtures/HG00096.vcf.gz)
HG96_SIZE=$(get_size)
echo "Update with HG00097 benchmark"
init_test
${BASE_DIR}/target/release/gen import -f ${BASE_DIR}/fixtures/chr22.fa.gz --shallow 2> /dev/null > /dev/null
HG97_IMPORT=$(time_taken ${BASE_DIR}/target/release/gen update --vcf ${BASE_DIR}/fixtures/HG00097.vcf.gz)
HG97_SIZE=$(get_size)
echo "Update with  HG00096 + HG00097 benchmark"
init_test
${BASE_DIR}/target/release/gen import -f ${BASE_DIR}/fixtures/chr22.fa.gz --shallow 2> /dev/null > /dev/null
HG96_IMPORT=$(time_taken ${BASE_DIR}/target/release/gen update --vcf ${BASE_DIR}/fixtures/HG00096.vcf.gz)
HG97_IMPORT=$(time_taken ${BASE_DIR}/target/release/gen update --vcf ${BASE_DIR}/fixtures/HG00097.vcf.gz)
SUM=$(echo "${HG96_IMPORT} + ${HG97_IMPORT}" | bc)
BOTH_SIZE=$(get_size)

echo "Benchmark results"
printf "%-35s %-10s %-10s\n" "Task" "Time (ms)" "Storage (mb)"
printf "%-35s %-10s %-10s\n" "---------------" "--------" "----------"
printf "%-35s %-10s %-10s\n" "Full import" ${FULL_IMPORT} ${FULL_SIZE}
printf "%-35s %-10s %-10s\n" "Shallow import" ${SHALLOW_IMPORT} ${SHALLOW_SIZE}
printf "%-35s %-10s %-10s\n" "HG00096 Update" ${HG96_IMPORT} ${HG96_SIZE}
printf "%-35s %-10s %-10s\n" "HG00097 Update" ${HG97_IMPORT} ${HG97_SIZE}
printf "%-35s %-10s %-10s\n" "HG00096 + HG00097 Update" ${SUM} ${BOTH_SIZE}