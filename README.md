#Trident Kafka Push

Enables you to push to kakfa from a trident topology. 

## Usage

Include the dependency:

	<groupId>com.github.quintona</groupId>
	<artifactId>trident-kafka-push</artifactId>
	<version>1.0-SNAPSHOT</version>
	
And the required repository:

	<repository>
		<id>clojars.org</id>
		<url>http://clojars.org/repo</url>
	</repository>
	
Then add a partition persist to your stream:

``` 
.partitionPersist(KafkaState.transactional("test1", new KafkaState.Options()), new Fields("text"),new KafkaStateUpdater("text"));
``` 

Opaque is not supported. 
Default options point to the local kafka instance.
You must tell the updater which field in the tuple contains the message contents, which are assumed to be string. 