# Learning Avro
## 0. Prerequisites
- Oracle JDK 8u131
- Apache Maven 3.5.0
## 1. Compile
```shell
mvn compile
```
## 2. Run tests
```shell
mvn test
```
- Check **testSpecific** to see how serializing and deserializing with code generation works
- Check **testGeneric** to see how serializing and deserializing without code generation works
## 3. Run app
```shell
mvn exec:java
```
- Check how RPC works

