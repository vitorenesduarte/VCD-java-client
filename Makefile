JAR=target/vcd-java-cli-0.1.jar

rel:
	mvn package

run: rel $(JAR)
	java -jar $(JAR) port=6000
