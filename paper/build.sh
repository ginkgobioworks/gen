#!/bin/sh

# Check if pandoc is available
if ! command -v pandoc >/dev/null 2>&1; then
    echo "Error: pandoc is not installed or not in PATH"
    echo "Please install pandoc: https://pandoc.org/installing.html"
    echo "Continuing without building the pdf version of the Gen paper..."
    exit 0 # Don't abort the commit
fi

pandoc --citeproc --bibliography=assets/zotero.bib --csl=assets/chicago-author-date.csl \
    --variable documentclass=paper --variable classoption=twocolumn \
    --lua-filter=assets/pandoc_filter.lua --output main.pdf main.md
