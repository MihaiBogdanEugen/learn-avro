# Learning Avro
## 0. Prerequisites
- Oracle JDK 8u144 with JAVA_HOME environment variable set up
## 1. Compile
```shell
./mvnw compile
```
## 2. Run tests
```shell
./mvnw test
```
- Check **testSpecific** to see how serializing and deserializing with code generation works
- Check **testGeneric** to see how serializing and deserializing without code generation works
## 3. Run app
```shell
./mvnw exec:java
```
- Check how RPC works

