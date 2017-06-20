<?php

use RdKafka\Message;
use React\EventLoop\Factory;
use Rx\Scheduler;
use Voryx\RxKafka\Consumer;

require __DIR__ . '/../vendor/autoload.php';

$loop = Factory::create();

Scheduler::setDefaultFactory(function () use ($loop) {
    return new Scheduler\EventLoopScheduler($loop);
});

$topic = new Consumer('some.test.topic', 'localhost');

$topic
    ->takeWhile(function (Message $x) {
        return $x->payload !== 'last';
    })
    ->subscribe(function (Message $x) {
        echo $x->payload . "\n";
    });

$loop->run();
