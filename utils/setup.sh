#!
# create directories
mkdir config
mkdir data
mkdir docs
mkdir logs
mkdir utils

# create python env
python -m venv transform_data

# initiate git repo and remote repo
# this assumes remote repo exists
# if not create one or you will get errors 
# when you try to commit
git init
git config user.name tropicalmentat
git config user.email gabtorres1011@gmail.com
git remote add origin https://github.com/tropicalmentat/transform_scraped_traffic_data.git

# add .gitignore
echo "data/ 
logs/ 
config/ 
transform_data
" >> .gitignore

# add .gitattributes
echo "# Auto detect text files and perform LF normalization
* text=auto" >> .gitattributes

# create readme markdown
touch README.md