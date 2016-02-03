<?php

namespace Queue;

use Queue\Exception\ConnectionException;

/**
 * AMQPClient class is middleware for interactions with AMQP broker
 *
 * @author Sergey Kutikin <s.kutikin@gmail.com>
 * @package Queue
 */
class AMQPClient
{
    const DEFAULT_PREFETCH_COUNT = 3;

    const MESSAGE_ACK = 1;
    const MESSAGE_NACK = 2;
    const MESSAGE_REQUEUE = 3;

    /**
     * @var \AMQPConnection A connection to AMQP broker
     */
    private $connection = null;

    /**
     * @var \AMQPChannel A channel to AMQP broker
     */
    private $channel = null;

    /**
     * Basic AMQPDriver constructor, establish new connection to AMQP broker (not opening).
     *
     * @param array $config Config with credentials for AMQP broker
     * @param int $readTimeout Timeout for read op in seconds
     * @param int $writeTimeout Timeout for write op in seconds
     * @param int $connectTimeout Timeout for connection in seconds
     */
    public function __construct(array $config, $readTimeout = 0, $writeTimeout = 5, $connectTimeout = 5)
    {
        $this->connection = new \AMQPConnection(
            [
                'host'  => $config['host'],
                'port'  => $config['port'],
                'vhost' => $config['vhost'],
                'login' => $config['user'],
                'password' => $config['pass'],
                'read_timeout'  => $readTimeout,
                'write_timeout' => $writeTimeout,
                'connect_timeout' => $connectTimeout
            ]
        );
    }

    /**
     * Initialize if not yet initialized and return channel to AMQP broker
     *
     * @return \AMQPChannel
     *
     * @throws ConnectionException
     */
    private function getChannel()
    {
        if ($this->channel && $this->channel->isConnected()) {
            return $this->channel;
        }

        try {
            $this->channel = null;

            if ($this->connection->isConnected()) {
                $this->connection->disconnect();
            }

            $this->connection->connect();

            $channel = new \AMQPChannel($this->connection);
            $channel->setPrefetchCount(self::DEFAULT_PREFETCH_COUNT);
        } catch (\AMQPException $e) {
            throw new ConnectionException(
                sprintf(
                    'AMQP connection error (%s:%s): %s',
                    $this->connection->getHost(),
                    $this->connection->getPort(),
                    $e->getMessage()
                )
            );
        }

        return $this->channel = $channel;
    }

    /**
     * Destroy channel and clearly close connection on destruction
     */
    public function __destruct()
    {
        $this->channel = null;

        // Close the connection clearly on destruct
        if ($this->connection->isConnected()) {
            $this->connection->disconnect();
        }
    }

    /**
     * Declare new exchange in AMQP broker
     *
     * @param string $name Exchange name
     * @param string $type Exchange type, see AMQP_EX_TYPE_*
     * @param bool $durable Is exchange durable
     *
     * @return bool Success flag
     *
     * @throws ConnectionException
     * @throws \AMQPException
     */
    public function declareExchange($name, $type = AMQP_EX_TYPE_DIRECT, $durable = true)
    {
        try {
            $exchange = new \AMQPExchange($this->getChannel());
            $exchange->setName($name);
            $exchange->setType($type);
            $exchange->setFlags($durable ? AMQP_DURABLE : AMQP_NOPARAM);

            return $exchange->declareExchange();

        } catch (\AMQPConnectionException $e) {
            $this->channel = null;
            throw new ConnectionException($e->getMessage(), $e->getCode(), $e);
        } catch (\AMQPException $e) {
            $this->channel = null;
            throw $e;
        }
    }

    /**
     * Declare new queue in AMQP broker
     *
     * @param string $name Queue name
     * @param bool $durable Is queue durable
     * @param bool $exclusive Is queue exclusive
     *
     * @return string New queue name
     *
     * @throws ConnectionException
     * @throws \AMQPException
     */
    public function declareQueue($name, $durable = true, $exclusive = false)
    {
        try {
            $queue = new \AMQPQueue($this->getChannel());

            if ($name !== null) {
                $queue->setName((string) $name);
            }

            $flags = AMQP_NOPARAM;
            $flags |= $durable ? AMQP_DURABLE : AMQP_NOPARAM;
            $flags |= $exclusive ? AMQP_EXCLUSIVE : AMQP_NOPARAM;

            $queue->setFlags($flags);
            $queue->declareQueue();

            return $queue->getName();
        } catch (\AMQPConnectionException $e) {
            $this->channel = null;
            throw new ConnectionException($e->getMessage(), $e->getCode(), $e);
        } catch (\AMQPException $e) {
            $this->channel = null;
            throw $e;
        }
    }

    /**
     * Sends payload to the provided exchange (optionally using routing key)
     *
     * @param string $message Payload to send to AMQP broker
     * @param string $exchangeName Exchange name to publish payload to
     * @param string|null $routingKey Optional routing key
     *
     * @throws \AMQPException
     */
    public function send($message, $exchangeName, $routingKey = null)
    {
        try {
            $exchange = new \AMQPExchange($this->getChannel());
            $exchange->setName($exchangeName);

            $exchange->publish($message, $routingKey);
        } catch (\AMQPException $e) {
            $this->channel = null;
            throw $e;
        }
    }

    /**
     * Consumes next message from queue and passes its payload to callback when it arrives
     *
     * @param string $queueName Queue name to consume message from
     * @param callable $callback Callback to pass payload to
     *
     * @throws \AMQPException
     */
    public function consume($queueName, callable $callback)
    {
        try {
            $queue = new \AMQPQueue($this->getChannel());
            $queue->setName($queueName);
            $queue->consume(function(\AMQPEnvelope $envelope, \AMQPQueue $queue) use ($callback) {
                switch($callback($envelope->getBody())) {
                    case self::MESSAGE_ACK:
                        $queue->ack($envelope->getDeliveryTag());

                        break;
                    case self::MESSAGE_NACK:
                        $queue->nack($envelope->getDeliveryTag());

                        break;
                    case self::MESSAGE_REQUEUE:
                        $queue->nack($envelope->getDeliveryTag(), AMQP_REQUEUE);

                        break;
                }
            });
        } catch (\AMQPException $e) {
            $this->channel = null;
            throw $e;
        }
    }

    /**
     * Gets next message from queue and returnes it or false if queue is emptied
     *
     * @param string $queueName Queue name to consume message from
     *
     * @return AMQPClosure Closure, returns false when queue emptied
     *
     * @throws \AMQPException
     */
    public function get($queueName)
    {
        try {
            $queue = new \AMQPQueue($this->getChannel());
            $queue->setName($queueName);

            $envelope = $queue->get();

            if ($envelope) {
                return new AMQPClosure($envelope->getBody(), $envelope, $queue);
            } else {
                return false;
            }
        } catch (\AMQPException $e) {
            $this->channel = null;
            throw $e;
        }
    }

    /**
     * Binds queues to exchange by name
     *
     * @param string $exchangeName A name of exchange
     * @param string[] $queueNames An array of queues names
     *
     * @throws \AMQPException
     */
    public function bind($exchangeName, array $queueNames)
    {

        try {
            foreach ($queueNames as $queueName) {
                $queue = new \AMQPQueue($this->getChannel());
                $queue->setName($queueName);

                $queue->bind($exchangeName, $queueName);
            }
        } catch (\AMQPException $e) {
            $this->channel = null;
            throw $e;
        }
    }
}
