<?php

namespace Downloader;

use Queue\AMQPClient;
use Queue\AMQPClosure;
use React\Stream\Stream;
use React\EventLoop;

/**
 * Worker is downloader main class, runs streams and starts daemon in daemon mode
 *
 * @author Sergey Kutikin <s.kutikin@gmail.com>
 * @package Downloader
 */
class Worker
{
    /**
     * @var AMQPClient $queueClient Client to AMQP broker
     */
    private $queueClient = null;

    /**
     * @var int A counter of processed URLs
     */
    private $counter = 0;

    /**
     * @var float A tick interval for daemon mode
     */
    private $tick = 1;

    /**
     * @var int Limit on concurrent downloads
     */
    private $limit = 3;

    /**
     * @var array Currently downloading files
     */
    private $files = [];

    public function __construct(AMQPClient $queueClient)
    {
        $this->queueClient = $queueClient;
    }

    /**
     * On destruction unlink all partial files
     */
    public function __destruct()
    {
        foreach (array_keys($this->files) as $file) {
            @unlink($file);
        }
    }

    /**
     * Receives <limit> messages from queue and processes them
     *
     * @param EventLoop\LoopInterface $loop A loop, worker is run on
     * @param int $limit Maximum parallel downloads
     *
     * @throws \AMQPException
     * @throws \Exception
     */
    public function run(EventLoop\LoopInterface $loop, $limit = 0)
    {
        while (($this->counter < $this->limit || $limit == 0) &&
            $closure = $this->queueClient->get(\Bot::QUEUE_NAME_SCHEDULER)) {
            $this->counter++;
            $this->processURL($closure, $loop);
        }

        // If loop is already run (in case of daemon), does nothing
        $loop->run();
    }

    /**
     * Starts downloader in daemon mode, awaiting for future tasks
     * @todo Locks
     *
     * @param EventLoop\LoopInterface $loop A loop, worker is run on
     */
    public function runDaemon(EventLoop\LoopInterface $loop)
    {
        $loop->addPeriodicTimer($this->tick, function ($timer) use ($loop) {
            $this->run($loop, $this->limit);
        });

        $loop->run();
    }

    /**
     * Processing one URL, opens pair of read-write streams and piping them
     *
     * @param AMQPClosure $closure A closure of message with another URL
     * @param EventLoop\LoopInterface $loop A loop, worker is run on
     *
     * @return bool Success flag
     *
     * @throws \Exception
     */
    private function processURL(AMQPClosure $closure, EventLoop\LoopInterface $loop)
    {
        $readStream = @fopen($closure->get(), 'r');

        if (!$readStream) {
            return $this->error($closure);
        }

        $tmpdir = \Bot::config('tmpdir');

        if (!file_exists($tmpdir)) {
            if (!mkdir($tmpdir, 0777, true)) {
                throw new \Exception('Cannot create downloading dirname ' . $tmpdir);
            }
        }

        $fWritePath = $tmpdir . "/" . md5($closure->get());
        $writeStream = fopen($fWritePath, 'w');

        if (!$writeStream) {
            throw new \Exception('Cannot open downloading file ' . $fWritePath);
        }

        $this->files[$fWritePath] = true;

        $read = new Stream($readStream, $loop);
        $write = new Stream($writeStream, $loop);

        $write->on('end', function () use ($closure, $fWritePath) {
            unset($this->files[$fWritePath]);

            $this->success($closure);
        });

        $read->on('error', function () use ($closure, $fWritePath) {
            unset($this->files[$fWritePath]);

            $this->error($closure);
        });

        $read->pipe($write);
    }

    /**
     * Sending URL to failed queue and ACKing schedule queue about processed item
     *
     * @param AMQPClosure $closure A closure of message with URL
     *
     * @return bool Success flag
     *
     * @throws \AMQPException
     */
    private function error(AMQPClosure $closure)
    {
        $this->counter--;
        $this->queueClient->send($closure->get(), \Bot::EXCHANGE_NAME, \Bot::QUEUE_NAME_FAILED);

        return $closure->ack();
    }

    /**
     * Sending URL to done queue and ACKing schedule queue about processed item
     *
     * @param AMQPClosure $closure A closure of message with URL
     *
     * @return bool Success flag
     *
     * @throws \AMQPException
     */
    private function success(AMQPClosure $closure)
    {
        $this->counter--;
        $this->queueClient->send($closure->get(), \Bot::EXCHANGE_NAME, \Bot::QUEUE_NAME_DOWNLOADED);

        return $closure->ack();
    }
}
