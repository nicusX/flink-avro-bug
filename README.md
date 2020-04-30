# Possible bug with Apache Fink and AVRO decimal fields

This repo contains the code to reproduce a bug with Apache Flink 1.10.0
when consuming AVRO SpecificRecords containing a decimal (logical type) field

The [test](src/test/java/example/TestDeepCopy.java) demonstrate the error when copying a SpecificRecord using Flink `AvroSerializer`.

The AVRO object is generated from [AVRO IDL](src/main/resources/avro/sample.avdl).

The repository also contains a sample Flink application where the problem arise.
The application consumes events from a Kafka source and uses the deserializer supporting Confluent Schema Registry.
