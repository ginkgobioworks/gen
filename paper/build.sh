#!/bin/sh
echo "Building the pdf version of the Gen paper..."

# Check if pandoc is available
if ! command -v pandoc >/dev/null 2>&1; then
    echo "Error: pandoc is not installed or not in PATH"
    echo "Please install pandoc: https://pandoc.org/installing.html"
    exit 0 # Don't abort the commit
fi

pandoc --citeproc  \
    --metadata-file=assets/pandoc_metadata.yaml \
    --lua-filter=assets/pandoc_filter.lua \
    --include-in-header=assets/pandoc_preamble.tex \
    --output main.pdf main.md

exit_code=$?

if [ $exit_code -eq 0 ]; then
    echo "Pandoc run succeeded"
    git add main.pdf
else
    echo "Pandoc failed with exit code: $exit_code"
    exit 0 # Don't abort the commit
fi

echo "Done!"