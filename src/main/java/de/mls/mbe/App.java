package de.mls.mbe;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.ipc.NettyServer;
import org.apache.avro.ipc.NettyTransceiver;
import org.apache.avro.ipc.Server;
import org.apache.avro.ipc.specific.SpecificRequestor;
import org.apache.avro.ipc.specific.SpecificResponder;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

public class App {

    public static class MailImpl implements Mail {

        public String send(Message message) {

            return "Sending message to '" + message.getTo() + "' from '" + message.getFrom() + "' with body '" + message.getBody() + "'";
        }
    }

    public static final int ServerTcpPort = 1224;

    public static void main(String[] args) throws IOException {

        if (args.length != 3) {
            System.out.println("Usage: <to> <from> <body>");
            System.exit(1);
        }

        Message message = new Message();
        message.setTo(args[0]);
        message.setFrom(args[1]);
        message.setBody(args[2]);

        Server server = new NettyServer(new SpecificResponder(Mail.class, new MailImpl()), new InetSocketAddress(ServerTcpPort));

        NettyTransceiver client = new NettyTransceiver(new InetSocketAddress(ServerTcpPort));

        Mail proxy = SpecificRequestor.getClient(Mail.class, client);

        CharSequence result = proxy.send(message);

        System.out.println("Result is: " + result);

        client.close();
        server.close();
    }

    public static List<User> deserializeFromFile(String fileName) {

        File file = new File(fileName);
        if (!file.exists()) {
            return null;
        }

        List<User> result = new ArrayList<>();

        DatumReader<User> datumReader = new SpecificDatumReader<>(User.class);
        try (DataFileReader<User> dataFileReader = new DataFileReader<>(file, datumReader)) {

            while (dataFileReader.hasNext()) {
                result.add(dataFileReader.next());
            }

        } catch (IOException error) {
            System.err.println(error.getMessage());
            return null;
        }

        return result;
    }

    public static List<GenericRecord> deserializeFromFile(String fileName, Schema schema) {

        File file = new File(fileName);
        if (!file.exists()) {
            return null;
        }

        List<GenericRecord> result = new ArrayList<>();

        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
        try (DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(file, datumReader)) {

            while (dataFileReader.hasNext()) {
                result.add(dataFileReader.next());
            }

        } catch (IOException error) {
            System.err.println(error.getMessage());
            return null;
        }

        return result;
    }

    public static void serializeToFile(String fileName, Schema schema, GenericRecord... records) {

        if (records.length == 0) {
            return;
        }

        File file = new File(fileName);
        if (file.exists()) {
            file.delete();
        }

        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);

        try (DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter)) {

            dataFileWriter.create(schema, file);
            for (GenericRecord record : records) {
                dataFileWriter.append(record);
            }
        } catch (IOException error) {
            System.err.println(error.getMessage());
        }
    }

    public static void serializeToFile(String fileName, User... users) {

        if (users.length == 0) {
            return;
        }

        File file = new File(fileName);
        if (file.exists()) {
            file.delete();
        }

        DatumWriter<User> datumWriter = new SpecificDatumWriter<>(User.class);

        try (DataFileWriter<User> dataFileWriter = new DataFileWriter<>(datumWriter)) {

            dataFileWriter.create(users[0].getSchema(), file);
            for (User user : users) {
                dataFileWriter.append(user);
            }
        } catch (IOException error) {
            System.err.println(error.getMessage());
        }
    }
}
