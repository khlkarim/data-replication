package local.dev.replication;

import com.rabbitmq.client.*;
import java.nio.charset.StandardCharsets;

public class ClientReader {
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
            System.out.println("Received reply from replica: " + body);
        };

        System.out.println("Listening on queue: " + replyQueueName);
        channel.basicConsume(replyQueueName, false, deliverCallback, consumerTag -> {
        });

        System.out.println("Sending read request...");
        channel.basicPublish(READ_EXCHANGE_NAME, "", null, replyQueueName.getBytes(StandardCharsets.UTF_8));
    }
}
