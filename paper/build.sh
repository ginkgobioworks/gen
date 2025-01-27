#!/bin/sh
echo "Building the pdf version of the Gen paper..."

# Render any dot files that have been changed
if ! command -v dot >/dev/null 2>&1; then
    echo "Warning: dot is not installed or not in PATH"
    echo "Please install Graphviz: https://graphviz.gitlab.io/download/"
else
    for dotfile in $(git diff --name-only --cached | grep '\.dot$'); do
        echo "Rendering $dotfile to ${dotfile%.dot}.png"
        dot -Tpng -o ${dotfile%.dot}.png $dotfile
        git add ${dotfile%.dot}.png
    done
fi

# Check if pandoc is available
if ! command -v pandoc >/dev/null 2>&1; then
    echo "Error: pandoc is not installed or not in PATH"
    echo "Please install pandoc: https://pandoc.org/installing.html"
    exit 0 # Don't abort the commit
fi

# Execute pandoc from the paper directory to generate the pdf
current_dir=$(pwd)
repo_root=$(git rev-parse --show-toplevel)
cd "$repo_root/paper"



pandoc --metadata-file=assets/pandoc_metadata.yaml \
    --citeproc  \
    --output main.pdf main.md

#    --filter pandoc-tablenos \
# Tablenos and other numbering filters require a separate install:
# pip install git+https://github.com/TimothyElder/pandoc-xnos
# (there's a bug in the main pandoc-tablenos repo that hasn't been fixed yet)
# https://github.com/tomduck/pandoc-xnos/issues/28

# Needed for two-column layout (see yaml metadata)
#    --lua-filter=assets/pandoc_filter.lua \ 
#    --include-in-header=assets/pandoc_preamble.tex \ 


# Check if pandoc run was successful
exit_code=$?

if [ $exit_code -eq 0 ]; then
    echo "Pandoc run succeeded"
    git add main.pdf
else
    echo "Pandoc failed with exit code: $exit_code"
    exit 0 # Don't abort the commit
fi

# Return to the original directory
cd $current_dir

echo "Done!"