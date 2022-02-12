#!/bin/bash

echo "Starting data update workflow...."

main_branch="master"
gh_branch="gh-pages"

git config --global user.email "$EMAIL_ID"
git config --global user.name "$USER_NAME"


pip3 install -r requirements.txt

python3 main.py



git checkout "$gh_branch"

echo "moving updated data to main folder"
mv -v -f temp/* data/
rm -rf temp/

git status
git add .
git commit -m "datasets updated on - $(date)"

git push origin "$gh_branch"

echo "data update done."
