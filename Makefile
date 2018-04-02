JAR=target/vcd-java-client-0.1.jar

test:
	mvn test

rel:
	mvn clean compile assembly:single
	mv target/vcdjavaclient-0.1-jar-with-dependencies.jar $(JAR)

run: rel $(JAR)
	java -jar $(JAR) \
		-clients=3 \
		-ops=10000 \
		-conflicts=true \
		-zk=127.0.0.1:2181

clean:
	mvn clean
