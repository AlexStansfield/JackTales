<?php

namespace JackTales\Command;

use Pheanstalk\PheanstalkInterface;
use Symfony\Component\DependencyInjection\ContainerAwareInterface;
use Symfony\Component\DependencyInjection\ContainerAwareTrait;
use Symfony\Component\DependencyInjection\ContainerInterface;

abstract class ContainerAwareWorkerCommand extends AbstractWorkerCommand implements ContainerAwareInterface
{
    use ContainerAwareTrait;

    /**
     * ContainerAwareWorkerCommand constructor.
     * @param PheanstalkInterface $pheanstalk
     * @param string $tube
     */
    public function __construct(PheanstalkInterface $pheanstalk, $tube)
    {
        parent::__construct(null, $pheanstalk);
        $this->setTube($tube);
    }

    /**
     * @return ContainerInterface
     */
    public function getContainer()
    {
        return $this->container;
    }
}
