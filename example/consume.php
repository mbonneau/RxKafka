<?php

use React\EventLoop\Factory;
use Rx\Scheduler;
use Voryx\RxKafka\Consumer;

require __DIR__ . '/../vendor/autoload.php';

$loop = Factory::create();

Scheduler::setDefaultFactory(function () use ($loop) {
    return new Scheduler\EventLoopScheduler($loop);
});

$topic = new Consumer('some.test.topic', 'localhost');

$topic->subscribe(function ($x) {
    echo $x . "\n";
});

$loop->run();
