# Song Popularity Prediction
Prediction of song hotness on the Million Songs Dataset

* Used AWS EC2 to process the data and send to S3

  * read.py: transforms data from h5 to csv and send to S3

* Used AWS EMR to do song hotness prediction using PySpark

  * train.ipynb: data processing, machine learning, and visulization on JupyterLab
