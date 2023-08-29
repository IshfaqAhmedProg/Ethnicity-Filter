@echo "Updating git"
call git reset --hard HEAD
call git pull
@echo "Updating dependencies"
call pip install -r requirements.txt