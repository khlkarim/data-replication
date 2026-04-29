package local.dev.replication;

import com.rabbitmq.client.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.util.Base64;
import java.util.List;
import java.util.Scanner;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.charset.StandardCharsets;

public class Replica {
    private static final String DATA_DIR = "data";
    private static final String READ_EXCHANGE_NAME = "read_exchange";
    private static final String REPLY_EXCHANGE_NAME = "reply_exchange";
    private static final String WRITE_EXCHANGE_NAME = "write_exchange";

    public static void main(String[] argv) throws Exception {
        Scanner scanner = new Scanner(System.in);

        String line = "";
        while (line.trim().length() == 0) {
            System.out.print("Replica ID: ");
            line = scanner.nextLine();
        }

        scanner.close();
        String replicaId = line;
        Files.createDirectories(Paths.get(DATA_DIR));
        String filePath = DATA_DIR + "/replica_" + replicaId + ".txt";

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.exchangeDeclare(WRITE_EXCHANGE_NAME, "fanout");
        channel.exchangeDeclare(REPLY_EXCHANGE_NAME, "direct");
        channel.exchangeDeclare(READ_EXCHANGE_NAME, "fanout");

        String writeQueueName = channel.queueDeclare().getQueue();
        channel.queueBind(writeQueueName, WRITE_EXCHANGE_NAME, "");

        String readQueueName = channel.queueDeclare().getQueue();
        channel.queueBind(readQueueName, READ_EXCHANGE_NAME, "");

        System.out.println("Replica " + replicaId + " started. File: " + filePath);
        System.out.println("Waiting for write and read requests.");

        DeliverCallback writeCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            System.out.println("Received write request: " + message);

            try (PrintWriter out = new PrintWriter(new FileWriter(filePath, true))) {
                out.println(message);
                System.out.println("Wrote to " + filePath);
            } catch (IOException e) {
                e.printStackTrace();
            }
        };

        channel.basicConsume(writeQueueName, true, writeCallback, consumerTag -> {
        });

        DeliverCallback readCallback = (consumerTag, delivery) -> {
            String body = new String(delivery.getBody(), StandardCharsets.UTF_8);

            try {
                Message message = decodeMessage(body);

                if (message.getAction() == Message.Action.READ_LAST) {
                    System.out.println("Received a read last request from: " + message.getReplyTo());

                    String lastLine = "";
                    try {
                        File file = new File(filePath);

                        if (file.exists()) {
                            List<String> lines = Files.readAllLines(Paths.get(filePath));
                            if (!lines.isEmpty()) {
                                lastLine = lines.get(lines.size() - 1);
                            }
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                    System.out.println("Replying with: " + lastLine);

                    Message reply = new Message(message.getAction(), replicaId, lastLine);
                    channel.basicPublish(REPLY_EXCHANGE_NAME, message.getReplyTo(), null,
                            encodeMessage(reply).getBytes(StandardCharsets.UTF_8));

                } else if (message.getAction() == Message.Action.READ_ALL) {
                    System.out.println("Received a read all request from: " + message.getReplyTo());

                    try {
                        File file = new File(filePath);

                        if (file.exists()) {
                            List<String> lines = Files.readAllLines(Paths.get(filePath));
                            if (!lines.isEmpty()) {
                                for (String l : lines) {
                                    System.out.println("Replying with: " + l);

                                    Message reply = new Message(message.getAction(), replicaId, l);
                                    channel.basicPublish(REPLY_EXCHANGE_NAME, message.getReplyTo(), null,
                                            encodeMessage(reply).getBytes(StandardCharsets.UTF_8));
                                }
                            }
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            } catch (Exception e) {
                System.out.println("Failed to parse message");
            }

        };

        channel.basicConsume(readQueueName, true, readCallback, consumerTag -> {
        });
    }

    public static Message decodeMessage(String s) throws IOException, ClassNotFoundException {
        byte[] data = Base64.getDecoder().decode(s);
        ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(data));

        Message m = (Message) ois.readObject();

        ois.close();
        return m;
    }

    private static String encodeMessage(Message m) throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(m);
        oos.close();
        return Base64.getEncoder().encodeToString(baos.toByteArray());
    }
}
