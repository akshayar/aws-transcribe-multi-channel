# Transcribe and merge streams from local files
## Run from IDE
1. Import project as Maven project. 
2. To run for two files , execute src/main/java/com/sample/transcribestreamin/multichannel/TranscribeStreamingTwoFilesMain.java 
3. It creates two streams from src/test/resources/speech_ai.wav and src/test/resources/speech_nature.wav
4. Merges two channels before sending to transcribe service. 

## Run from Command Line
```shell
./runTwoFiles.sh src/test/resources/speech_ai.wav src/test/resources/speech_nature.wav
```
# Transcribe and merge streams from S3 event notification
## Run from IDE
1. Import project as Maven project.
2. To run for two files , execute src/main/java/com/sample/transcribestreamin/s3event/TranscribeS3FilesOnEventListenerMain.java
3. It will listen to S3 event notification from  SQS queue configured in src/main/resources/application.properties using s3.sqsQueueUrl property
4. For S3 to work, ensure that default region is same as the region configured in src/main/resources/application.properties

## Run from Command Line
```shell
./runS3EventFiles.sh

```
