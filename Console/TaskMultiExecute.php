<?php

namespace App\Console\Commands;

use Illuminate\Console\Command;
use Illuminate\Support\Facades\Log;
use ReflectionClass;

/**
 * Class TaskMultiExecute
 * Runs Prediction and Import task in multi-thread mode
 * @package App\Console\Commands
 *
 * */


class TaskMultiExecute extends Command
{
    const DEFAULT_LIMIT = 100;

    protected $signature = 'multi:task {task} {--customer_id=} {--force} {--type=} {--d|debug}';
    protected $description = 'Executes multi tasks';
    protected $debug = false;

    const PARAMS = [
        'process' => ['debug' => 'option'],
        'prediction' => ['debug' => 'option', 'customer_id' => 'option', 'force' => 'option', 'type' => 'option'],
    ];

    public function handle()
    {
        $task = $this->argument('task');
        $this->debug = $this->option('debug');

        $file = dirname(__FILE__) . '/Multi/' . strtolower($task) . '.php';
        if (!file_exists($file)) {
            Log::error('Multitask: NOT FOUND "' . $task . '"');
            die('ERROR: Multitask "' . $task . '" not found' . PHP_EOL);
        }

        require $file;

        Log::info('Multitask: "' . $task . '"');

        $params = [];
        foreach (self::PARAMS[$task] as $param => $method) {
            $params[$param] = $this->$method($param);
        }

        $taskClass = __NAMESPACE__ . '\\Multi\\' . $task;
        $tasker = $this->instantiate($taskClass, $params);
        $tasker->run();
    }

    protected function instantiate($name, $args = array())
    {
        if (empty($args)) {
            return new $name();
        } else {
            $ref = new ReflectionClass($name);
            return $ref->newInstanceArgs($args);
        }
    }

    public function debug($string)
    {
        if (!$this->debug) {
            return;
        }

        $this->info($string);

        $this->getHelp();
    }

}
