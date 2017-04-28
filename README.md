# Bird-Prediction
There are inputs provided with only 1 lakh records. The original files have not been uploaded.


#STANDALONE MODE


Building and execution of Training-Prediction in Standalone mode.

1. Create a new Maven Project in the IDE.

2. Give the Group ID and the artifact ID.

3. A pom.xml will be created, add the required dependencies. 

4. The src/main/scala should contain the source code files.

5. Include the config folder which has the stand-alone configurations: core-site.xml, hdfs-site.xml, mapred-site.xml, yarn-site.xml.
	
#For training the model

1. Create an inputLabel folder which should contain the labeled.csv file 

2.In the Makefile for standalone mode change
job.name=Training
local.input=inputLabel
local.output=model
spark.root = spark location on your local system
jar.name = jar file that will be created
jar.path = target/${jar.name}

Update alone command: 
${spark.root}/bin/spark-submit --class ${job.name} --master local[*] ${jar.path} ${local.input} ${local.output}
 

3. pom.xml, Makefile, src, config and inputLabel folders should be in the same project directory.

After the above steps:
Run 2 commands into the terminal of the IDE

1. make switch-standalone.

2. make alone.

Once the make alone finishes execution we will get a target folder and a model folder. 


#For predicting the bird sightings

1. Create an inputUnlabel folder which should contain the unlabeled.csv file (provided in the assignment).

2. In the Makefile for standalone mode change
job.name=Prediction
local.input=model
local.input2=inputUnlabel
local.output=output

Update alone command: 
${spark.root}/bin/spark-submit --class ${job.name} --master local[*] ${jar.path} ${local.input} ${local.input2} ${local.output}

3. pom.xml, Makefile, src, config, inputUnlabel and model folders should be in the same project directory.

After the above steps:
Run ‘make alone’ on the terminal of the IDE.

We will get one output folder which will have the event sampling id and related prediction of the sighting of the red-winged bird.
 

#AWS EMR


1. These are the plugins and dependencies your pom.xml should contain
(We have added dependencies relating to mllib)

    <dependencies>

        <dependency>
            <groupId>org.scala-lang</groupId>
            <artifactId>scala-library</artifactId>
            <version>2.11.8</version>
        </dependency>

        <dependency>
            <groupId>commons-io</groupId>
            <artifactId>commons-io</artifactId>
            <version>2.5</version>
        </dependency>

        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-streaming_2.11</artifactId>
            <version>2.1.0</version>
        </dependency>

        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-core_2.11</artifactId>
            <version>2.1.0</version>
        </dependency>

        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-mllib_2.11</artifactId>
            <version>2.1.0</version>
        </dependency>

        <dependency>
            <groupId>junit</groupId>
            <artifactId>junit</artifactId>
            <version>3.8.1</version>
            <scope>test</scope>
        </dependency>

    </dependencies>

2. In the Makefile for AWS execution 
aws.region = to the region of your aws cluster
aws.bucket.name = bucket_name you want to create on S3
aws.subnet.id = subnet_id of your region from VPC subnet list


#For Training the model


1. Changes in the Makefile

job.name=Training
aws.input=inputLabel
aws.output=model
aws.log.dir=name of the log folder
aws.num.nodes=number of worker machines
aws.instance.type = type of the machine to use.

Update the cloud command (applicationsName, steps and added configurations option):

--applications Name=Spark \
--steps '[{"Name":"Spark Program", "Args":["--class", "${job.name}", "--master", "yarn", "--deploy-mode", "cluster", "s3://${aws.bucket.name}/${jar.name}", "s3://${aws.bucket.name}/${aws.input}","s3://${aws.bucket.name}/${aws.output}"],"Type":"Spark","Jar":"s3://${aws.bucket.name}/${jar.name}","ActionOnFailure":"TERMINATE_CLUSTER"}]' \
--log-uri s3://${aws.bucket.name}/${aws.log.dir} \

2. In the source code, replace 
val conf = new SparkConf(true).setAppName("Training").setMaster("local") with
val conf = new SparkConf(true).setAppName("Training").setMaster("yarn")

3. On the terminal go to the directory where your source code, Makefile, pom.xml, inputLabel folder and .jar file exists. In the empty inputLabel folder add the labeled.csv. 

Update upload command:
upload-input-aws:make-bucket
	aws s3 sync ${local.input} s3://${aws.bucket.name}/${aws.input}

Upload data on S3 with : make upload-input-aws

This will upload the data in your inputLabel folder and will make a bucket on S3 with the name as in your aws.bucket.name (in Makefile)

4. Run ‘make cloud’ on the terminal to launch your cluster on EMR.

5. Log into the console on AWS.

6. Go to the EMR cluster to see how the Cluster is running. Once it’s terminated with steps completed, check the syslog.

7. Go to S3, you will see a model folder created.

The commands executed from step 3-7 should all be executed in that same directory on the terminal.


#For Predicting the bird sightings

1. Changes in the MakeFile

job.name=Prediction
aws.input=model
aws.input2=inputUnlabel
aws.output=output
aws.log.dir=name of the log folder
aws.num.nodes=number of worker machines
aws.instance.type = type of the machine to use.


Update the cloud command (with an extra argument in steps):

--steps '[{"Name":"Spark Program", "Args":["--class", "${job.name}", "--master", "yarn", "--deploy-mode", "cluster", "s3://${aws.bucket.name}/${jar.name}", "s3://${aws.bucket.name}/${aws.input}”, "s3://${aws.bucket.name}/${aws.input2}”,”s3://${aws.bucket.name}/${aws.output}"],"Type":"Spark","Jar":"s3://${aws.bucket.name}/${jar.name}","ActionOnFailure":"TERMINATE_CLUSTER"}]' \

2. In the source code, replace 
val conf = new SparkConf(true).setAppName(“Prediction”).setMaster("local") with
val conf = new SparkConf(true).setAppName(“Prediction”).setMaster("yarn")

3. On the terminal go to the directory where your source code, Makefile, pom.xml, inputUnlabel folder and .jar file exists. In the empty inputUnlabel folder add the unlabeled.csv. 

Upload command updated:
upload-input2-aws:
	aws s3 sync ${local.input2} s3://${aws.bucket.name}/${aws.input2}

Upload data on S3 with : make upload-input2-aws 
(since bucket is already created, you don’t need to create it again)

This will upload the data in your inputUnlabel folder

4. Run ‘make cloud’ on the terminal to launch your cluster on EMR.

5. Log into the console on AWS.

6. Go to the EMR cluster to see how the Cluster is running. Once it’s terminated with steps completed, check the syslog.

7. Go to S3, you will see a output folder created, this will have the event sampling id and corresponding prediction

The commands executed from step 3-7 should all be executed in that same directory on the terminal.



#DIRECTORY STRUCTURE OF THE PROGRAMS

For the Intellij IDE

1. File Structure : project/src/main/scala

a. Main Files: 

1. Training.scala: Training the model by using RandomForest

2. Prediction.scala: Predicting the bird sightings based on the trained model obtained from Training.scala and input from the unlabeled data

For a normal run (not from the terminal) on the IDE,

Run->Edit Configurations->Applications
Click on the configuration tab on the right side pane
1. Enter your path for Training class
2. Program arguments for Training : inputLabel model 
 
Then click on the run button for a successful run

Run->Edit Configurations->Applications
Click on the configuration tab on the right side pane
1. Enter your path for Prediction class 
2. Program arguments for Prediction : model inputUnlabel output

The output folder will have the the event sampling and corresponding prediction
