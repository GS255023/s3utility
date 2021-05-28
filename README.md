# S3utility

## Prerequisites

### Directory Structure
     -> /s3/upload/
	 -> /s3/upload/config/
	 -> /s3/upload/processed/
	 -> /s3/upload/resume/
#### Config File
    -> /s3/upload/config/s3credentials
        Sample Data:
        {"AWS_ACCESS_KEY_ID":"ASIASrrrX7XTZQK3PLL","AWS_SECRET_ACCESS_KEY":"hE5DnfffM+udmKQWo","AWS_SESSION_TOKEN":"IQoJb3JpZ2","region":"eu-north-1"}


## Execution Steps

    1. java -jar s3utility.jar /s3/upload/

    2. Drop sample input file 1621679461-upload sample file to /s3/upload/

    3. Make sure contents of 1621679461-upload sample file exists on local file system

    4. Upload process will start

    5. Drop file 1621679530-pauseupload file /s3/upload/

    6. Upload process will be paused

    7. If you want to resume upload process drop the newly file created in  /s3/upload/resume/ to /s3/upload/
    
## Main Class
    
    -> aws.example.s3.WatchDir