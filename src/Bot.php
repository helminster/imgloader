<?php

use Queue\AMQPClient;
use Scheduler\Parser;
use Downloader\Worker;
use React\EventLoop;

/**
 * Bot is main class, starting appropriate tasks dependant on args
 *
 * @author Sergey Kutikin <s.kutikin@gmail.com>
 */
class Bot
{
    /**
     * Delimiter for config keys
     */
    const CONFIG_DELIMITER = '/';

    /**
     * Task type, schedules URLs from input file for download
     */
    const BOT_TYPE_SCHEDULE = 1;
    /**
     * Task type, downloads all current URLs from schedule and exits
     */
    const BOT_TYPE_DOWNLOAD = 2;
    /**
     * Task type, downloads URLs as they arrive to schedule, daemon mode
     */
    const BOT_TYPE_DOWNLOAD_DAEMON = 3;

    const EXCHANGE_NAME = 'image_download';
    const QUEUE_NAME_SCHEDULER = 'download';
    const QUEUE_NAME_DOWNLOADED = 'done';
    const QUEUE_NAME_FAILED = 'failed';

    /**
     * @var array A dictionary connecting commands to BOT_TYPE_* types
     */
    public static $typeDictionary = [
        'schedule' => Bot::BOT_TYPE_SCHEDULE,
        'download' => Bot::BOT_TYPE_DOWNLOAD,
        'daemon' => Bot::BOT_TYPE_DOWNLOAD_DAEMON
    ];

    /**
     * @var AMQPClient An interface for communication with RabbitMQ
     */
    private $queueClient;

    /**
     * @var int Type of the application run task, see Bot::BOT_TYPE_*
     */
    private $type = null;

    /**
     * Basic Bot constructor, initializes bot and starts appropriate task.
     *
     * @param int $type Type of task to perform, see BOT_TYPE_*
     * @param string $file Path to input file for schedule task
     */
    public function __construct($type = self::BOT_TYPE_SCHEDULE, $file = 'images.txt')
    {
        $this->queueClient = new AMQPClient(self::config('queue'));
        $this->initAMQPBroker();

        $this->type = $type;

        switch ($this->type) {
            case self::BOT_TYPE_SCHEDULE:
                $this->schedule($file);

                break;
            case self::BOT_TYPE_DOWNLOAD:
            case self::BOT_TYPE_DOWNLOAD_DAEMON:
                $this->download($this->type == self::BOT_TYPE_DOWNLOAD_DAEMON);

                break;
            default:
                throw new InvalidArgumentException(sprintf('This should not happen, wrong task type: %s', $this->type));
        }
    }

    /**
     * Get config params according to path. If path is null -- return whole config
     *
     * @param string|null $path
     * @return mixed|null
     */
    public static function config($path = null)
    {
        static $config = null;

        if (null == $config) {
            $config = include ROOT_DIR . '/etc/config.php';
        }

        if (null == $path) {
            return $config;
        }

        $keys = explode(self::CONFIG_DELIMITER, $path);
        $conf = $config;

        foreach ($keys as $key) {
            if (!isset($conf[$key])) {
                return null;
            }

            $conf = $conf[$key];
        }

        return $conf;

    }

    /**
     * Schedules downloads and puts malformed URLs to failed queue
     *
     * @param string $filename Path to file with image URLs
     *
     * @throws AMQPException
     * @throws \InvalidArgumentException
     */
    private function schedule($filename)
    {
        $parser = new Parser($filename);
        $urls = $parser->retrieveUrls();

        foreach ($urls as $url) {
            $this->queueClient->send($url['url'], self::EXCHANGE_NAME, $url['malformed'] ? 'failed' : 'download');
        }
    }

    /**
     * Downloads URLs scheduled by scheduler, can run in daemon mode
     *
     * @param bool $isDaemon Daemon mode on/off
     */
    private function download($isDaemon = false)
    {
        $loop = EventLoop\Factory::create();
        $worker = new Worker($this->queueClient);

        if (!$isDaemon) {
            $worker->run($loop);
        } else {
            $worker->runDaemon($loop);
        }
    }

    /**
     * Declares and binds exchange and all needed queues.
     *
     * @throws AMQPException
     * @throws \Queue\Exception\ConnectionException
     */
    private function initAMQPBroker()
    {
        $this->queueClient->declareExchange(self::EXCHANGE_NAME);

        $schedulerQueue = $this->queueClient->declareQueue(self::QUEUE_NAME_SCHEDULER);
        $downloadQueue = $this->queueClient->declareQueue(self::QUEUE_NAME_DOWNLOADED);
        $failQueue = $this->queueClient->declareQueue(self::QUEUE_NAME_FAILED);

        $this->queueClient->bind(self::EXCHANGE_NAME, [$schedulerQueue, $downloadQueue, $failQueue]);
    }
}
