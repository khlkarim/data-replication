package local.dev.replication;

import com.rabbitmq.client.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.LinkedList;
import java.util.Scanner;

public class ClientReader {
    private static LinkedList<String> lines = new LinkedList<>();
    private static final String READ_EXCHANGE_NAME = "read_exchange";
    private static final String REPLY_EXCHANGE_NAME = "reply_exchange";

    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.exchangeDeclare(READ_EXCHANGE_NAME, "fanout");
        channel.exchangeDeclare(REPLY_EXCHANGE_NAME, "direct");

        String replyQueueName = channel.queueDeclare().getQueue();
        channel.queueBind(replyQueueName, REPLY_EXCHANGE_NAME, replyQueueName);

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String body = new String(delivery.getBody(), StandardCharsets.UTF_8);
            try {
                Message message = decodeMessage(body);

                if (message.getAction() == Message.Action.READ_LAST) {
                    System.out.println(
                            "Received reply from replica " + message.getReplyTo() + ": " + message.getContent());
                } else if (message.getAction() == Message.Action.READ_ALL) {
                    lines.push(message.getContent());

                    int nb = 0;
                    for (String line : lines) {
                        if (line.trim().equals(message.getContent().trim())) {
                            nb++;
                        }
                    }

                    if (nb == 2) {
                        System.out.println("Line that appears in the majority of replicas: " + message.getContent());
                    }
                }

            } catch (Exception e) {
                System.out.println("Failed to parse message");
            }

        };

        System.out.println("Listening on queue: " + replyQueueName);
        channel.basicConsume(replyQueueName, false, deliverCallback, consumerTag -> {
        });

        System.out.println("Sending read request...");
        Scanner scanner = new Scanner(System.in);

        System.out.println("Available request types:");
        System.out.println("[0] Read Last");
        System.out.println("[1] Read All");
        System.out.print("Pick which request type to send (default: 0): ");
        String choix = scanner.nextLine();

        Message message = null;
        if (choix.trim().equals("1")) {
            message = new Message(Message.Action.READ_ALL, replyQueueName, "");
        } else {
            message = new Message(Message.Action.READ_LAST, replyQueueName, "");
        }

        if (message != null) {
            String encodedMessage = encodeMessage(message);
            channel.basicPublish(READ_EXCHANGE_NAME, "", null, encodedMessage.getBytes(StandardCharsets.UTF_8));
        }

        scanner.close();
    }

    private static String encodeMessage(Message m) throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(m);
        oos.close();
        return Base64.getEncoder().encodeToString(baos.toByteArray());
    }

    public static Message decodeMessage(String s) throws IOException, ClassNotFoundException {
        byte[] data = Base64.getDecoder().decode(s);
        ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(data));

        Message m = (Message) ois.readObject();

        ois.close();
        return m;
    }
}
