package de.mls.mbe;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertNull;

class AppTest {

    @Test
    void testSpecific() {

        User expectedUser1 = new User();
        expectedUser1.setName("Alyssa");
        expectedUser1.setFavoriteNumber(256);

        User expectedUser2 = new User("Ben", 7, "red");

        User expectedUser3 = User.newBuilder()
                .setName("Charlie")
                .setFavoriteColor("blue")
                .setFavoriteNumber(null)
                .build();

        String fileName = "users.avro";

        App.serializeToFile(fileName, expectedUser1, expectedUser2, expectedUser3);

        List<User> users = App.deserializeFromFile(fileName);

        User actualUser1 = users.stream()
                .filter(x -> x.getName().toString().equals(expectedUser1.getName().toString()))
                .findFirst()
                .orElse(null);

        assertEquals(expectedUser1, actualUser1);

        User actualUser2 = users.stream()
                .filter(x -> x.getName().toString().equals(expectedUser2.getName().toString()))
                .findFirst()
                .orElse(null);

        assertEquals(expectedUser2, actualUser2);

        User actualUser3 = users.stream()
                .filter(x -> x.getName().toString().equals(expectedUser3.getName().toString()))
                .findFirst()
                .orElse(null);

        assertEquals(expectedUser3, actualUser3);
    }

    @Test
    void testGeneric() throws IOException {

        String schemaFileName = "person.avsc";
        String serializationFileName = "people.avro";

        Schema schema = new Schema.Parser().parse(new File(schemaFileName));

        GenericRecord expectedPerson1 = new GenericData.Record(schema);
        expectedPerson1.put("first_name", "Bogdan");
        expectedPerson1.put("last_name", "Mihai");

        GenericRecord expectedPerson2 = new GenericData.Record(schema);
        expectedPerson2.put("first_name", "Sorin");
        expectedPerson2.put("last_name", "Mihai");
        expectedPerson2.put("age", 28);

        App.serializeToFile(serializationFileName, schema, expectedPerson1, expectedPerson2);

        List<GenericRecord> records = App.deserializeFromFile(serializationFileName, schema);

        GenericRecord actualPerson1 = records.stream()
                .filter(x -> x.get("first_name").toString().equals(expectedPerson1.get("first_name").toString()))
                .findFirst()
                .orElse(null);

        assertEquals(expectedPerson1, actualPerson1);

        GenericRecord actualPerson2 = records.stream()
                .filter(x -> x.get("first_name").toString().equals(expectedPerson2.get("first_name").toString()))
                .findFirst()
                .orElse(null);

        assertEquals(expectedPerson2, actualPerson2);
    }

    private static void assertEquals(GenericRecord expected, GenericRecord actual) {

        assertEqualsAsStrings(expected.get("first_name").toString(), actual.get("first_name"));
        assertEqualsAsStrings(expected.get("last_name"), actual.get("last_name"));
        assertEqualsAsStrings(expected.get("age"), actual.get("age"));
    }

    private static void assertEqualsAsStrings(Object expected, Object actual) {

        if (expected == null) {
            assertNull(actual);
            return;
        }

        if (actual == null) {
            assertNull(expected);
            return;
        }

        Assertions.assertEquals(expected.toString(), actual.toString());
    }

    private static void assertEquals(User expected, User actual) {

        if (expected == null) {
            assertNull(actual);
            return;
        }

        if (actual == null) {
            assertNull(expected);
            return;
        }

        if (expected.getName() == null) {
            assertNull(actual.getName());
        }

        if (actual.getName() == null) {
            assertNull(expected.getName());
        }

        assertEquals(expected.getName(), actual.getName());
        assertEquals(expected.getFavoriteColor(), actual.getFavoriteColor());
        assertEquals(expected.getFavoriteNumber(), actual.getFavoriteNumber());
    }

    private static void assertEquals(CharSequence expected, CharSequence actual) {

        assertEqualsAsStrings(expected, actual);
    }

    private static void assertEquals(Integer expected, Integer actual) {

        if (expected == null) {
            assertNull(actual);
            return;
        }

        if (actual == null) {
            assertNull(expected);
            return;
        }

        Assertions.assertEquals(expected, actual);
    }
}
