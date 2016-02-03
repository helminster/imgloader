<?php

namespace Queue;

/**
 * AMQPClosure for post processing and informing AMQP broker of result of this processing
 *
 * @author Sergey Kutikin <s.kutikin@gmail.com>
 * @package Queue
 */
class AMQPClosure
{
    /**
     * @var \AMQPEnvelope Envelope object of closure
     */
    private $envelope;

    /**
     * @var \AMQPQueue Queue object of closure
     */
    private $queue;

    /**
     * @var bool Is closure processed already
     */
    private $processed;

    /**
     * @var string Payload of message of this closure
     */
    private $message;

    /**
     * Basic AMQPClosure constructor.
     *
     * @param string $message A payload from message of this closure
     * @param \AMQPEnvelope $envelope Envelope object of closure
     * @param \AMQPQueue $queue Queue object of closure
     */
    public function __construct($message, \AMQPEnvelope $envelope, \AMQPQueue $queue)
    {
        $this->envelope = $envelope;
        $this->queue = $queue;
        $this->message = $message;
    }

    /**
     * Returns payload of message of this closure
     *
     * @return string Payload of message
     */
    public function get()
    {
        return $this->message;
    }

    /**
     * ACK queue about processed envelope
     *
     * @return bool Success flag
     */
    public function ack()
    {
        if ($this->processed) {
            return false;
        }

        $this->queue->ack($this->envelope->getDeliveryTag());
        return $this->processed = true;
    }

    /**
     * NACK queue about processed envelope
     *
     * @return bool Success flag
     */
    public function nack()
    {
        if ($this->processed) {
            return false;
        }

        $this->queue->nack($this->envelope->getDeliveryTag());
        return $this->processed = true;
    }

    /**
     * Requeue message in queue
     *
     * @return bool Success flag
     */
    public function requeue()
    {
        if ($this->processed) {
            return false;
        }

        $this->queue->nack($this->envelope->getDeliveryTag(), AMQP_REQUEUE);
        return $this->processed = true;
    }
}
