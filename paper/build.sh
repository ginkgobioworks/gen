#!/bin/sh
echo "Building the pdf version of the Gen paper..."

# Check if pandoc is available
if ! command -v pandoc >/dev/null 2>&1; then
    echo "Error: pandoc is not installed or not in PATH"
    echo "Please install pandoc: https://pandoc.org/installing.html"
    exit 0 # Don't abort the commit
fi

pandoc --citeproc --bibliography=assets/zotero.bib --csl=assets/chicago-author-date.csl \
    --variable documentclass=paper --variable classoption=twocolumn \
    --lua-filter=assets/pandoc_filter.lua --output main.pdf main.md

git add main.pdf

echo "Done!"