package local.dev.replication;

import java.util.Scanner;
import java.nio.charset.StandardCharsets;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class ClientWriter {
    private static final String EXCHANGE_NAME = "write_exchange";

    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.exchangeDeclare(EXCHANGE_NAME, "fanout");

        Scanner scanner = new Scanner(System.in);
        System.out.println("Type lines to send to replicas (type 'exit' to quit):");

        while (true) {
            String line = scanner.nextLine();
            if (line.trim().equalsIgnoreCase("exit")) {
                break;
            }

            channel.basicPublish(EXCHANGE_NAME, "", null, line.getBytes(StandardCharsets.UTF_8));
            System.out.println("Sent: " + line);
        }

        scanner.close();
    }
}
