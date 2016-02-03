#!/usr/bin/env php
<?php

require __DIR__ . '/src/bootstrap.php';

$displayHelp = false;

if ($argc < 2) {
    $displayHelp = true;
}

$file = array_shift($argv);
$type = array_shift($argv);
$input = array_shift($argv);

if (!in_array($type, array_keys(Bot::$typeDictionary))) {
    $displayHelp = true;
}

if ($displayHelp) {
    echo sprintf(
        "usage: %s <command> [<file>]\n\nCommands list:\n  %s\n",
        $file,
        implode("\n  ", array_keys(Bot::$typeDictionary))
    );

    exit(1);
} else {
    new Bot(Bot::$typeDictionary[$type], $input);

    exit(0);
}