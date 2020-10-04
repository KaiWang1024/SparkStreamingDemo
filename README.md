# SparkStreamingDemo
A demo project of Spark Streaming, Kafka, HBase.

There are two programs in this demo. One is a Kafka producer, and the other is Kafka consumer and using Spark Streaming to do some calculation then write the result to HBase.

## Kafka Producer

The Kafka producer creates random mock data and send the message to Kafka topic `user-behavior-topic`.
The generate the message with format like `page1|2|7.123|1`. Separated by `|`. The first one is the page ID. The second one is number of click action on this page. The third one is the time that user stays on this page. The last one means like or not. 

## Spark Consumer

This part as a consumer that read data from Kafka and using Spark Streaming to do some calculate. The fomula is `f(x,y,z)=0.8x+0.8y+z`. 

For example.

(page1, 1, 0.5, 1)

H(page1)=f(x,y,z)= 0.8x+0.8y+z=0.8*1+0.8*0.5+1*1=2.2

Then the top 10 rate of pages will be recorded into HBase.

## HBase commands
The commands using for creating HBase table and scan the table is in the file `hbaseCommands`.

## HBase screenshot
Screenshots of HBase table after running the program and Kafka console are in the `screenshots` folder.

## Demo video
https://web.microsoftstream.com/video/a0a25d25-2640-4874-9708-206536d48c96