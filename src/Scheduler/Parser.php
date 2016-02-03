<?php

namespace Scheduler;

/**
 * Parser for extracting URLs from input file
 *
 * @author Sergey Kutikin <s.kutikin@gmail.com>
 * @package Scheduler
 */
class Parser
{
    const ALLOWED_SCHEMES = [
        'http',
        'https'
    ];

    /**
     * @var null|resource A pointer to file with image urls
     */
    private $filed = null;

    /**
     * Basic Parser constructor, opens pointer to filename passed.
     *
     * @param string $filename Path to file for parsing
     *
     * @throws \InvalidArgumentException
     */
    public function __construct($filename)
    {
        if (!file_exists($filename)) {
            throw new \InvalidArgumentException(sprintf('File does not exist: %s', $filename));
        }

        $this->filed = fopen($filename, 'r');
    }

    /**
     * Clean up on destruction
     */
    public function __destruct()
    {
        fclose($this->filed);
    }

    /**
     * Returns array of urls extracted from file passed to the class on construction.
     * Format of array is [ ['url' => <url>, 'malformed' => <boolean flag>], ... ]
     *
     * @return array An array of parsed urls
     */
    public function retrieveUrls()
    {
        $result = [];

        while ($url = trim(fgets($this->filed))) {
            $failed = false;

            if (!$this->isURLValid($url) || !$this->isSchemeValid($url)) {
                $failed = true;
            }

            $result[] = [
                'url' => $url,
                'malformed' => $failed
            ];
        }

        return $result;
    }

    /**
     * Validates URL by PHP embedded means
     *
     * @param string $url An URL to test
     * @return bool Result of check
     */
    private function isURLValid($url)
    {
        return !!filter_var($url, FILTER_VALIDATE_URL, FILTER_FLAG_SCHEME_REQUIRED | FILTER_FLAG_HOST_REQUIRED);
    }

    /**
     * Checks if scheme of URL is in allowed list
     *
     * @param string $url An URL to check
     * @return bool Result of check
     */
    private function isSchemeValid($url)
    {
        return in_array(parse_url($url, PHP_URL_SCHEME), self::ALLOWED_SCHEMES);
    }
}
