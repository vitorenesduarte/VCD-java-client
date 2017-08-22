JAR=target/vcd-java-client-0.1.jar

rel:
	mvn clean compile assembly:single
	mv target/vcd-java-client-0.1-jar-with-dependencies.jar $(JAR)

run: rel $(JAR)
	java -jar $(JAR) port=6000

clean:
	mvn clean
