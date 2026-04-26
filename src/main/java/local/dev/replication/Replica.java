package local.dev.replication;

import com.rabbitmq.client.*;

import java.io.File;
import java.util.List;
import java.util.Scanner;
import java.io.FileWriter;
import java.io.IOException;
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
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            System.out.println("Received read request: " + message);

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
            channel.basicPublish(REPLY_EXCHANGE_NAME, message, null, lastLine.getBytes(StandardCharsets.UTF_8));
        };

        channel.basicConsume(readQueueName, true, readCallback, consumerTag -> {
        });
    }
}
