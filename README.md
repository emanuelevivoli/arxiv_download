# DOWNLOAD AWS S3 bucket

The main file is "download_manifest.py".
Some file is mandatory to have, generally all files that are in tools.
In order to download some operation has to be done.
1. First of all, you need a AWS account.
2. In the file "secret/config.ini" you must put your real ACCESS_KEY and SECRET_KEY
3. Put attention in the result of the run. The more files you get in "logs/log.txt"
4. Uncomment the "# subprocess" call in "get_file" in order to start downloading