#!/usr/bin/env php
<?php

require __DIR__ . '/src/bootstrap.php';

$displayHelp = false;
$typeDictionary = [
    'schedule' => Bot::BOT_TYPE_SCHEDULE,
    'download' => Bot::BOT_TYPE_DOWNLOAD,
    'daemon' => Bot::BOT_TYPE_DOWNLOAD_DAEMON
];

if ($argc < 2) {
    $displayHelp = true;
}

$file = array_shift($argv);
$type = array_shift($argv);
$input = array_shift($argv);

if (!in_array($type, array_keys($typeDictionary))) {
    $displayHelp = true;
}

if ($displayHelp) {
    echo sprintf(
        "usage: %s <command> [<file>]\n\n" .
        "Commands list:\n" .
        "  %s\n",
        $file,
        implode("\n  ", array_keys($typeDictionary))
    );
    exit(1);
} else {
    new Bot($typeDictionary[$type], $input);
    exit(0);
}