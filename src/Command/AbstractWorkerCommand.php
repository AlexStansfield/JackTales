<?php

namespace JackTales\Command;

use Pheanstalk\Job;
use Pheanstalk\PheanstalkInterface;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Process\Exception\RuntimeException;

/**
 * Class AbstractWorkerCommand
 *
 * @package JackTales\Command
 */
abstract class AbstractWorkerCommand extends Command
{
    const ACTION_RELEASE = 1;
    const ACTION_BURY = 2;
    const ACTION_DELETE = 3;

    /**
     * @var InputInterface
     */
    protected $input;

    /**
     * @var OutputInterface
     */
    protected $output;

    /**
     * @var PheanstalkInterface
     */
    protected $pheanstalk;

    /**
     * @var bool
     */
    protected $terminated = false;

    /**
     * @var int
     */
    protected $ttl = 3600;

    /**
     * @var string
     */
    protected $tube;

    /**
     * @param PheanstalkInterface $pheanstalk
     * @param string|null $name
     */
    public function __construct($name = null, PheanstalkInterface $pheanstalk = null)
    {
        parent::__construct($name);

        if (null !== $pheanstalk) {
            $this->setPheanstalk($pheanstalk);
        }
    }

    /**
     * Set the Pheanstalk
     *
     * @param PheanstalkInterface $pheanstalk
     * @return $this
     */
    public function setPheanstalk(PheanstalkInterface $pheanstalk)
    {
        $this->pheanstalk = $pheanstalk;

        return $this;
    }

    /**
     * Set the tube to listen to
     *
     * @param string $tube
     * @return $this
     */
    public function setTube($tube)
    {
        $this->tube = $tube;

        return $this;
    }

    /**
     * Set the worker Time to Live
     *
     * @param int $ttl
     * @return $this
     */
    public function setTtl($ttl)
    {
        $this->ttl = $ttl;

        return $this;
    }

    /**
     * Execute the command
     *
     * @param InputInterface $input
     * @param OutputInterface $output
     * @return void
     */
    protected function execute(InputInterface $input, OutputInterface $output)
    {
        // Check Pheanstalk has been set
        if (null === $this->pheanstalk) {
            throw new RuntimeException('Pheanstalk service not found, did you set it?');
        }

        $this->input = $input;
        $this->output = $output;

        // Required for signal catching
        declare(ticks = 1);

        // Catch the TERM and INT signal
        pcntl_signal(SIGTERM, [$this, 'terminate']);
        pcntl_signal(SIGINT, [$this, 'terminate']);

        // Let the folks at home know what we're doing
        $this->output->writeln('<info>Watching Queue</info>');

        // Watch for jobs
        $this->watchForJobs();
    }

    /**
     * Check the job data is valid
     *
     * @param Job $job
     * @return bool
     */
    abstract protected function isValid(Job $job);

    /**
     * Get the message to display when we start
     *
     * @param Job $job
     * @return string
     */
    abstract protected function getStartMessage(Job $job);

    /**
     * Process the job
     *
     * @param Job $job
     * @return int
     */
    abstract protected function processJob(Job $job);

    /**
     * Watch for jobs on the given tube
     *
     * @return void
     */
    public function watchForJobs()
    {
        // Time the worker will retire
        $retireTime = time() + $this->ttl;

        if (null !== $this->tube) {
            $this->pheanstalk->watchOnly($this->tube);
        }

        // Watch the Queue
        while (!$this->isTerminated()) {
            $job = $this->pheanstalk->reserve(5);

            if ($job) {
                // Let everyone know we just grabbed a job off the queue
                $this->output->writeln('<comment>Found Job ID: ' . $job->getId() . '</comment>');

                // Check the data is valid for us to process a job
                if (!$this->isValid($job)) {
                    $this->output->writeln('<comment>Invalid Job, skipping.</comment>');
                    $outcome = self::ACTION_BURY;
                } else {
                    // Output to let anyone watching know that we're starting a worker
                    $this->output->writeln('<comment>' . $this->getStartMessage($job) . '</comment>');

                    try {
                        // Process the job
                        $outcome = $this->processJob($job);
                    } catch (\Exception $e) {
                        // Output error
                        $this->output->writeln('<error>Fatal Error: ' . $e->getMessage() . '</error>');
                        // Bury the job
                        $this->pheanstalk->bury($job);
                        // Break out of while loop
                        break;
                    }

                    // Let the folks know we've completed it
                    $this->output->writeln('<comment>Job Processed.</comment>');
                }

                switch ($outcome) {
                    case self::ACTION_DELETE:
                        // Remove the job from the queue
                        $this->pheanstalk->delete($job);
                        break;
                    case self::ACTION_BURY:
                        // Remove the job from the queue
                        $this->pheanstalk->bury($job);
                        break;
                    case self::ACTION_RELEASE:
                        // Remove the job from the queue
                        $this->pheanstalk->release($job);
                        break;
                }

                $this->output->writeln('<info>Waiting for next job...</info>');
            }

            // Check if it's time to retire the worker
            if ((0 !== $this->ttl) && (time() > ($retireTime))) {
                $this->retire();
            }
        }

        $this->output->writeln('<info>Exiting.</info>');
    }

    /**
     * Terminate the worker
     *
     * @return $this
     */
    public function terminate()
    {
        $this->output->writeln('<info>Caught Signal. Graceful Exit.</info>');
        $this->terminated = true;

        return $this;
    }

    /**
     * Retire Worker
     *
     * @return $this
     */
    public function retire()
    {
        $this->output->writeln('<info>Worker reached old age, retiring.</info>');
        $this->terminated = true;

        return $this;
    }

    /**
     * Has the worker been given order to terminate?
     *
     * @return bool
     */
    public function isTerminated()
    {
        return $this->terminated;
    }
}
