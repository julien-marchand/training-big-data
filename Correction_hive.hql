/*
Putting data on hdfs
Put the files Emails.csv, Aliases.csv, Persons.cs and EmailReceivers.csv on hdfs at /user/ey/data/hilary (files are located in home/ey/data/hilary).
Hint: use the command hadoop fs –put <local_file> <hdfs_directory>
*/
cd /home/ey/data/hilary
hadoop fs -put * /user/ey/data/hilary

/*
Create the database hilary by executing the hive command: CREATE DATABASE hilary
*/
CREATE DATABASE hilary;


/*
Create the tables Aliases in the database hilary by executing the following command
*/
USE hilary;

CREATE TABLE Aliases (
  id bigint, 
  alias string, 
  personid bigint)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY ','
TBLPROPERTIES("skip.header.line.count"="1");


/*
Load the file into the table Aliases you just created.
*/
LOAD DATA INPATH '/user/ey/data/hilary/Aliases.csv' INTO TABLE Aliases;


/*
Create the table Emailreceivers and Persons. Use BIGINT type for all the Id fields. Moreover these tables also contains headers.
*/
CREATE TABLE `emailreceivers`(
  `id` bigint, 
  `emailid` bigint, 
  `personid` bigint)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY ','
TBLPROPERTIES("skip.header.line.count"="1");

CREATE TABLE `Persons`(
  `id` bigint, 
  `name` STRING)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY ','
TBLPROPERTIES("skip.header.line.count"="1");

/*
Load the files Emailreceivers.csv et Persons.csv being on hdfs into the corresponding tables
*/
LOAD DATA INPATH '/user/ey/data/hilary/EmailReceivers.csv' INTO TABLE EmailReceivers;
LOAD DATA INPATH '/user/ey/data/hilary/Persons.csv' INTO TABLE Persons;

/*
Create the table Emails in the database hilary and load the files Emails.csv in the table Emails
*/
CREATE TABLE Emails(
	Id STRING,
	DocNumber STRING,
	MetadataSubject STRING,
	MetadataTo STRING,
	MetadataFrom STRING,
	SenderPersonId STRING,
	MetadataDateSent STRING,
	MetadataDateReleased STRING,
	MetadataPdfLink STRING,
	MetadataCaseNumber STRING,
	MetadataDocumentClass STRING,
	ExtractedSubject STRING,
	ExtractedTo STRING,
	ExtractedFrom STRING,
	ExtractedCc STRING,
	ExtractedDateSent STRING,
	ExtractedCaseNumber STRING,
	ExtractedDocNumber STRING,
	ExtractedDateReleased STRING,
	ExtractedReleaseInPartOrFull STRING,
	ExtractedBodyText STRING,
	RawText STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ',',
   "quoteChar"     = '"',
   "escapeChar"    = '\\'
)
STORED AS TEXTFILE;

LOAD DATA INPATH '/user/ey/data/hilary/Emails.csv' INTO TABLE Emails;

/*
Observe that the Emails.csv file has been moved to the hive warehouse at /apps/hive/warehouse/hilary.db/emails on hdfs
 */
 hadoop fs -ls /user/ey/data/hilary
 hadoop fs -ls /apps/hive/warehouse/hilary.db/emails

/*
Type Describe Emails
*/
DESCRIBE Emails;

/*
Create a new table named Emails_new. This table should:
	- Use the ORC format

	- Have the following column (and type): 
		Id BIGINT,DocNumber STRING,MetadataSubject STRING,MetadataTo STRING,
		MetadataFrom STRING,SenderPersonId BIGINT,MetadataDateSent STRING,
		MetadataDateReleased STRING,MetadataPdfLink STRING,MetadataCaseNumber STRING,
		MetadataDocumentClass STRING,ExtractedSubject STRING,ExtractedTo STRING,
		ExtractedFrom STRING,ExtractedCc STRING,ExtractedDateSent STRING,
		ExtractedCaseNumber STRING,ExtractedDocNumber STRING,ExtractedDateReleased STRING,
		ExtractedReleaseInPartOrFull STRING,ExtractedBodyText STRING,RawText STRING

	- Your create table statement should look like:

		CREATE TABLE Emails_new (col_name type, ...)
		PARTITIONED BY (partioned_col_name type)
		STORED AS ORC;
*/
CREATE TABLE Emails_new (
	Id BIGINT,
	DocNumber STRING,
	MetadataSubject STRING,
	MetadataTo STRING,
	MetadataFrom STRING,
	SenderPersonId BIGINT,
	MetadataDateSent STRING,
	MetadataDateReleased STRING,
	MetadataPdfLink STRING,
	MetadataCaseNumber STRING,
	MetadataDocumentClass STRING,
	ExtractedSubject STRING,
	ExtractedTo STRING,
	ExtractedFrom STRING,
	ExtractedCc STRING,
	ExtractedDateSent STRING,
	ExtractedCaseNumber STRING,
	ExtractedDocNumber STRING,
	ExtractedDateReleased STRING,
	ExtractedReleaseInPartOrFull STRING,
	ExtractedBodyText STRING,
	RawText STRING)
PARTITIONED BY (MetadataYearSent INT)
STORED AS ORC;


/*
Insert data from Emails table to Emails_new table. Beware of the following
*/
SET hive.exec.dynamic.partition.mode = 'nonstrict';
INSERT INTO TABLE Emails_new PARTITION(MetadataYearSent)
SELECT
	cast(Id AS BIGINT),
	DocNumber,
	MetadataSubject,
	MetadataTo,
	MetadataFrom,
	cast(SenderPersonId AS BIGINT),
	MetadataDateSent,
	MetadataDateReleased,
	MetadataPdfLink,
	MetadataCaseNumber,
	MetadataDocumentClass,
	ExtractedSubject,
	ExtractedTo,
	ExtractedFrom,
	ExtractedCc,
	ExtractedDateSent,
	ExtractedCaseNumber,
	ExtractedDocNumber,
	ExtractedDateReleased,
	ExtractedReleaseInPartOrFull,
	ExtractedBodyText,
	RawText,
	substring(MetadataDateSent, 0, 4)
FROM emails;

/*
Verify that your Emails_new table is fine
	- Do a DESCRIBE
	- Do a SELECT * FROM Emails LIMIT 100 (always put a LIMIT when you deal with big data !)
*/
DESCRIBE Emails_new;
SELECT *
FROM Emails_new
LIMIT 100;

/*
Once you are sure that Emails_new is good drop the Emails table and rename the Emails_new table to Emails
*/
DROP TABLE Emails;
ALTER TABLE Emails_new RENAME TO Emails;

/*
Go to the hive warehouse on hdfs (/apps/hive/warehouse) and look at the folder containing the table Emails. You will see that there is one folder per year.
*/
hadoop fs -ls /apps/hive/warehouse/Emails


--Lets do some query to analyze our data. Answer to the following questions by executing some hive SELECT statements.
--	- List the top 10 persons who sent the most messages to hilary. (you need to join the Emails and Persons tables on Emails.SenderPersonId=Persons.Id)
SELECT p.Name, COUNT(p.Name) AS NumEmailsSent
FROM Emails e
JOIN Persons p ON e.SenderPersonId=p.Id
GROUP BY p.Name
ORDER BY NumEmailsSent DESC
LIMIT 10;

--	- List the top 10 persons who receive the most messages from hilary. (you need to join the Emails and EmailReceivers tables on Emails.SenderPersonId=EmailReceivers.EmailId and to join the EmailReceivers and Persons tables on EmailReceivers.PersonId=Persons.Id)
SELECT p.Name, COUNT(p.Name) AS NumEmailsReceived
FROM Emails e
JOIN EmailReceivers r ON r.EmailId=e.Id
JOIN Persons p ON r.PersonId=p.Id
GROUP BY p.Name
ORDER BY NumEmailsReceived DESC
LIMIT 10;

/*
Execute the following commands:
*/
ADD JAR /home/ey/udf/myudfs-0.0.1-bugged.jar;
CREATE TEMPORARY FUNCTION wordcount AS 'org.hive.udfs.myudfs.WordCount';

SELECT sum(wordcount(extractedbodytext))/count(*)
FROM emails;

/*
Which line of the file throws an exception ?
	=> It the 19th line of WordCount.java
*/

ADD JAR /home/ey/udf/myudfs-0.0.1-bugged.jar;
CREATE TEMPORARY FUNCTION wordcount AS 'org.hive.udfs.myudfs.WordCount';

SELECT sum(wordcount(extractedbodytext))/count(*)
FROM emails;