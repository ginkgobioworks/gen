#!/bin/sh

pandoc --citeproc --bibliography=assets/zotero.bib --csl=assets/chicago-author-date.csl \
    --variable documentclass=paper --variable classoption=twocolumn \
    --lua-filter=assets/pandoc_filter.lua --output main.pdf main.md
