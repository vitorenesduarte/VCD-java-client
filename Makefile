JAR=target/vcd-java-client-0.1.jar

test:
	mvn test

rel:
	mvn clean compile assembly:single
	mv target/vcd-java-client-0.1-jar-with-dependencies.jar $(JAR)

run: rel $(JAR)
	java -jar $(JAR) port=6000 \
		clients=3 \
		ops=10000 \
		conflict_percentage=100

clean:
	mvn clean
