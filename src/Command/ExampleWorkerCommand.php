<?php

namespace JackTales\Command;

use Pheanstalk\Job;

/**
 * Class ExampleWorkerCommand
 *
 * @package JackTales\Command
 */
class ExampleWorkerCommand extends AbstractWorkerCommand
{
    protected function configure()
    {
        $this
            ->setName('jacktales:example')
            ->setDescription('Output Messages from Queue');
    }

    /**
     * @param Job $job
     * @return bool
     */
    protected function isValid(Job $job)
    {
        $data = json_decode($job->getData(), true);

        $valid = is_array($data) && array_key_exists('message', $data);

        return $valid;
    }

    /**
     * @param Job $job
     * @return string
     */
    protected function getStartMessage(Job $job)
    {
        return 'Starting Send Message Job';
    }

    /**
     * @param Job $job
     * @return int
     * @throws \Exception
     */
    protected function processJob(Job $job)
    {
        $data = json_decode($job->getData(), true);

        // Throw exception
        if ('error' === $data['message']) {
            throw new \Exception('Example error thrown from worker');
        }

        $this->output->writeln('<comment>' . $data['message'] . '</comment>');

        return self::ACTION_DELETE;
    }
}
