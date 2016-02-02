<?php

class Bootstrap
{
    /**
     * Application initialization
     */
    public static function init()
    {
        define('ROOT_DIR', dirname(__DIR__));

        require ROOT_DIR . '/vendor/autoload.php';
    }
}

Bootstrap::init();
