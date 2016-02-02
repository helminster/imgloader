<?php

namespace Downloader;

use Queue\AMQPClient;
use Queue\AMQPClosure;
use React\Stream\Stream;
use React\EventLoop;

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
     * @var int Limit on concurrent downloads
     */
    private $limit = 3;

    /**
     * @var array Currently downloading files
     */
    private $files = [];

    /**
     * @var bool Flag of failed file transfer
     */
    private $lastError;

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

    public function run(EventLoop\Timer\Timer $timer)
    {
        $this->counter = 0;

        while ($this->counter < $this->limit &&
                $closure = $this->queueClient->get(\Bot::QUEUE_NAME_SCHEDULER)) {
            $this->counter++;
            $this->proceedDownload($closure, $timer->getLoop());
        }

        if ($this->counter == 0) {
            exit();
        }
    }

    private function proceedDownload(AMQPClosure $closure, EventLoop\LoopInterface $loop)
    {
        set_error_handler([$this, 'errorUrlHandler']);
        $readStream = @fopen($closure->get(), 'r');
        restore_error_handler();

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

    private function error(AMQPClosure $closure)
    {
        $this->queueClient->send($closure->get(), \Bot::EXCHANGE_NAME, \Bot::QUEUE_NAME_FAILED);
        return $closure->ack();
    }

    private function success(AMQPClosure $closure)
    {
        $this->queueClient->send($closure->get(), \Bot::EXCHANGE_NAME, \Bot::QUEUE_NAME_DOWNLOADED);
        return $closure->ack();
    }

    /**
     * Error handler for read errors while downloading
     *
     * @param int $errno
     * @param string $errstr
     * @param string $errfile
     * @param int $errline
     * @param string $errcontext
     *
     * @return bool
     */
    public function errorUrlHandler($errno, $errstr, $errfile, $errline, $errcontext)
    {
        $this->lastError = true;

        return false;
    }
}
