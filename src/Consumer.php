<?php

namespace Voryx\RxKafka;

use RdKafka\Conf;
use Rx\Disposable\CallbackDisposable;
use Rx\Disposable\CompositeDisposable;
use Rx\DisposableInterface;
use Rx\Observable;
use Rx\ObserverInterface;
use Rx\Scheduler;
use Rx\SchedulerInterface;

class Consumer extends Observable
{
    /** @var Conf */
    private $conf;

    /** @var string */
    private $brokers;

    /** @var string */
    private $topic;

    /** @var SchedulerInterface */
    private $scheduler;

    /** @var int */
    private $offset;

    /**
     * Consumer constructor.
     * @param string $topic
     * @param string $brokers
     * @param int $offset
     * @param Conf|null $conf
     * @param SchedulerInterface|null $scheduler
     */
    public function __construct($topic, $brokers, $offset = RD_KAFKA_OFFSET_END, Conf $conf = null, SchedulerInterface $scheduler = null)
    {
        $this->conf      = $conf;
        $this->brokers   = $brokers;
        $this->topic     = $topic;
        $this->offset    = $offset;
        $this->scheduler = $scheduler ?? Scheduler::getAsync();
    }

    protected function _subscribe(ObserverInterface $observer): DisposableInterface
    {
        $rk = new \RdKafka\Consumer();
        $rk->addBrokers($this->brokers);

        $topic = $rk->newTopic($this->topic);

        $topic->consumeStart(0, $this->offset);

        $scheduleDisp =  $this->scheduler->schedulePeriodic(
            function () use ($topic, $observer) {
                $n = 30; //Number of messages in one tick.  This is the longest that it will block at one time
                $i = 0;
                while ($i <= $n && $msg = $topic->consume(0, 0)) {
                    if (!$msg || $msg->err !== RD_KAFKA_RESP_ERR_NO_ERROR) {
                        if ($msg) {
                            // there seem to be a lot of non-error errors
//                    $obs->onError(new Exception($msg->err));
                        }
                        break;
                    }
                    $i++;
                    $observer->onNext($msg);
                }
            },
            0,
            100
        );

        $cleanup = new CallbackDisposable(function () use ($rk, $topic) {
            $topic->consumeStop(0);
        });

        return new CompositeDisposable([$scheduleDisp, $cleanup]);
    }
}