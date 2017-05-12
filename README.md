# ImageCorpusValidator

## About
This script allows us to check if all images (the list of images' name is in data.txt) are connected to the elasticsearch database.
 
## Requierements 
We only need the *elasticsearch* library for python3 in order to connect and send request to the database.

## Usage
We need to give some information (variable in the script) in order to handle the database :
* S_IMAGES_PATH : path to the folder that contains our images
* S_INDEX : ElasticSearch index name
* S_DOCTYPE : Doctype name of our data

We also need to choose a way to perform our treatment (*N_OPTION*)
* 1 : We associate tweets (ES database) with images (in order to detect missing image)
* 2 : We associate images (images folder) with tweets (in order to detect missing tweets)
* 3 : We use the file that we created in option 2 in order to gain some time

In option 2 and 3, we need to use a file to stock images' name, so we use *S_PATH_FILE_ES* to indicate file's path.