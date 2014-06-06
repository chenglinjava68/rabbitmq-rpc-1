package com.ekino.technoshare.rabbitmq;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.AMQP.BasicProperties;

import java.util.UUID;

public class RPCClient {

    private static final String RPC_QUEUE_NAME = "rpc_queue";

    public static void main(String[] argv) throws Exception {
        // Connection + Channel
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        // Create the reply queue
        String replyQueueName = channel.queueDeclare().getQueue();
        QueueingConsumer consumer = new QueueingConsumer(channel);
        channel.basicConsume(replyQueueName, true, consumer);

        // Create the properties
        String corrId = UUID.randomUUID().toString();
        BasicProperties props = new BasicProperties
                .Builder()
                .correlationId(corrId) // TO-EXPLAIN correlationId
                .replyTo(replyQueueName) // TO-EXPLAIN replyTo
                .build();

        // Make the request
        System.out.println(" [x] Requesting fib(30)");
        channel.basicPublish("", RPC_QUEUE_NAME, props, argv[0].getBytes());  // TO-EXPLAIN Default Exchange

        // Handle the response by consuming the reply queue
        while (true) {
            QueueingConsumer.Delivery delivery = consumer.nextDelivery();
            if (delivery.getProperties().getCorrelationId().equals(corrId)) {
                String response = new String(delivery.getBody(), "UTF-8");
                System.out.println(" [.] Got '" + response + "'");
                break;
            }
        }

        channel.close();
        connection.close();
    }

}
