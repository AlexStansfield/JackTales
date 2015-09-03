<?php

namespace JackTales\Command;

use Pheanstalk\Job;
use Pheanstalk\PheanstalkInterface;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;

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
     * @var string
     */
    protected $tube;

    /**
     * @param PheanstalkInterface $pheanstalk
     * @param string|null $name
     */
    public function __construct(PheanstalkInterface $pheanstalk, $name = null)
    {
        parent::__construct($name);

        $this->pheanstalk = $pheanstalk;
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
        $this->input = $input;
        $this->output = $output;

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
        if (null !== $this->tube) {
            $this->pheanstalk->watchOnly($this->tube);
        }

        // Watch the Queue
        while ($job = $this->pheanstalk->reserve()) {
            // Let everyone know we just grabbed a job off the queue
            $this->output->writeln('<comment>Found Job ID: ' . $job->getId() . '</comment>');

            // Check the data is valid for us to process a job
            if (!$this->isValid($job)) {
                $this->output->writeln('<comment>Invalid Job, skipping.</comment>');
                $outcome = self::ACTION_BURY;
            } else {
                // Output to let anyone watching know that we're starting a worker
                $this->output->writeln('<comment>' . $this->getStartMessage($job) . '</comment>');

                // Process the Job
                $outcome = $this->processJob($job, $this->output);

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
    }
}