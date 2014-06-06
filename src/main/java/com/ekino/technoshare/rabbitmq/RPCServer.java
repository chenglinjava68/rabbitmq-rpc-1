package com.ekino.technoshare.rabbitmq;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.AMQP.BasicProperties;

public class RPCServer {

    private static final String RPC_QUEUE_NAME = "rpc_queue";

    private static int fib(int n) {
        if (n == 0) return 0;
        if (n == 1) return 1;
        return fib(n - 1) + fib(n - 2);
    }

    public static void main(String[] argv) throws Exception {
        // Connection + Channel
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        // Declare queue
        channel.queueDeclare(RPC_QUEUE_NAME, false, false, false, null);
        channel.basicQos(1);

        // Register a consumer
        QueueingConsumer consumer = new QueueingConsumer(channel);
        channel.basicConsume(RPC_QUEUE_NAME, false, consumer);
        System.out.println(" [x] Awaiting RPC requests");

        // Main loop
        while (true) {
            // Wait for a message
            QueueingConsumer.Delivery delivery = consumer.nextDelivery();

            // Get the correlation id and forward it
            BasicProperties props = delivery.getProperties();
            BasicProperties replyProps = new BasicProperties
                    .Builder()
                    .correlationId(props.getCorrelationId()) // TO-EXPLAIN correlationId
                    .build();

            String response = null;
            try {
                // Do the work
                String message = new String(delivery.getBody(), "UTF-8");
                int n = Integer.parseInt(message);
                System.out.println(" [.] fib(" + message + ")");
                response = "" + fib(n);
            } catch (Exception e) {
                System.out.println(" [.] " + e.toString());
                response = "";
            } finally {
                // Send the response and ack the message
                channel.basicPublish("", props.getReplyTo(), replyProps, response.getBytes("UTF-8")); // TO-EXPLAIN replyTo
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            }
        }

    }
}
