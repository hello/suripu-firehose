#!/usr/bin/env bash
CURR_BRANCH=`git rev-parse --abbrev-ref HEAD`
git checkout master
git pull
git checkout PRODUCTION
git merge master --no-edit
git push origin PRODUCTION
git checkout $CURR_BRANCH

REPO_NAME=`basename \`git rev-parse --show-toplevel\``
open 'https://travis-ci.com/hello/'${REPO_NAME}