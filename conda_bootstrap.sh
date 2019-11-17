#!bin/bash
# Downloading Miniconda from Miniconda Repository
# for Linux machine with x86_64 architecture
wget https://repo.continuum.io/miniconda/Miniconda3-4.7.12.1-Linux-x86_64.sh
# Installing Miniconda
bash Miniconda3-4.7.12.1-Linux-x86_64.sh -b -p ~/miniconda
# Removing Miniconda bash script from local directory
rm Miniconda3-4.7.12.1-Linux-x86_64.sh
# Activating conda base environment
echo 'export PATH=~/miniconda/bin:$PATH' >> ~/.bashrc
source ~/.bashrc
# Updating Anaconda
conda update conda
# Creating 'awstakehome' conda environment from
# the file 'environment.yml'
conda env create -f environment.yml
# Activating 'awstakehome' conda environment
source activate awstakehome
echo "Activated \`awstakehome\` conda environment."