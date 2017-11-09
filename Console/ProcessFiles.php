<?php

namespace App\Console\Commands;

use App\Models\DataChannel;
use App\Models\FileSetting;
use App\Models\UploadedFile;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\Log;
use Wilgucki\Csv\Facades\Reader;
use App\Models\DataItem;
use App\Models\ImportFile;


class ProcessFiles extends Command
{
    protected $signature = 'process:files {--d|debug}';
    protected $description = 'Cron task. processes uploaded files';
    protected $debug = false;

    public function handle()
    {
        $this->debug = $this->option('debug');
        Log::info('process:files START');
        $this->debug('process:files START');

        /* Implementation */
        $files = UploadedFile::where('status', UploadedFile::STATUS_NEW)->get();

        foreach ($files as $file) {
            $importer = new ImportFile($file);
            $importer->import();
        }

        Log::info('process:files FINISH');
        $this->debug('process:files FINISH');
    }

    protected function debug($string)
    {
        if (!$this->debug) {
            return;
        }

        $this->info($string);
    }
}