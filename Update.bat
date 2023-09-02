@echo "Updating git"
call git reset --hard HEAD
call git pull
@echo "Updating dependencies"
@echo off
call pip install -r requirements.txt
call pip install -r requirements.txt