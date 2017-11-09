<?php

namespace App\Models;

use Illuminate\Database\Eloquent\Model;
use DatePeriod;
use DateTime;
use DateInterval;
use Illuminate\Support\Facades\DB;
use App\Libs\Uuids;


class TaskMaster extends Model
{

    use Uuids;

    const TYPE_IMPORT = 1;
    const TYPE_UPDATE = 2;
    const TYPE_DELETE = 3;
    const TYPE_FUTURE_PREDICTION = 4;
    const TYPE_HISTORICAL_PREDICTION = 5;

    const PRIORITY_HIGH = 1;
    const PRIORITY_MEDIUM = 2;
    const PRIORITY_LOW = 3;

    const STATUS_NEW = 1;
    const STATUS_SPLIT = 2;
    const STATUS_PROGRESS = 3;
    const STATUS_FAIL = 4;
    const STATUS_CANCELED = 5;

    const KEEP_CANCELED_TIME = 60;

    public $incrementing = false;
    protected $fillable = [
        'customer_id',
        'connector_id',
        'date_from',
        'date_to',
        'type',
        'priority',
        'status',
    ];

    public static function getType($id = null)
    {
        static $list = null;
        if ($list === null) {
            $list = [
                self::TYPE_IMPORT => 'Import',
                self::TYPE_UPDATE => 'Update',
                self::TYPE_DELETE => 'Delete',
                self::TYPE_FUTURE_PREDICTION => 'Future',
                self::TYPE_HISTORICAL_PREDICTION => 'Historical',
            ];
        }

        if (empty($id)) {
            return $list;
        }

        if (!isset($list[$id])) {
            throw new Exception('Type "' . $id . '" not exists');
        }

        return $list[$id];
    }

    public static function getStatus($id = null)
    {
        static $list = null;
        if ($list === null) {
            $list = [
                self::STATUS_NEW => 'New',
                self::STATUS_SPLIT => 'Prepared',
                self::STATUS_PROGRESS => 'Processing',
                self::STATUS_FAIL => 'Fail',
                self::STATUS_CANCELED => 'Canceled',
            ];
        }

        if (empty($id)) {
            return $list;
        }

        if (!isset($list[$id])) {
            throw new Exception('Status "' . $id . '" not exists');
        }

        return $list[$id];
    }

    public static function getPriority($id = null)
    {
        static $list = null;
        if ($list === null) {
            $list = [
                self::PRIORITY_HIGH => 'High',
                self::PRIORITY_MEDIUM => 'Medium',
                self::PRIORITY_LOW => 'Low',
            ];
        }

        if (empty($id)) {
            return $list;
        }

        if (!isset($list[$id])) {
            throw new Exception('Priority "' . $id . '" not exists');
        }

        return $list[$id];
    }

    public function split()
    {
        $period = $this->getPeriod();

        $existing_tasks = DB::table('task_children')
            ->select('task_children.date', 'task_masters.priority', 'task_children.id')
            ->whereBetween('date', [$this->date_from, $this->date_to])
            ->where('task_masters.customer_id', $this->customer_id)
            ->where('task_masters.connector_id', $this->connector_id)
            ->join('task_masters', 'task_masters.id', '=', 'task_children.master_id')
            ->get();
        $data = [];
        foreach ($existing_tasks as $task) {
            $data[$task->date] = [
                'id' => $task->id,
                'priority' => $task->priority
            ];
        }

        $to_delete = [];
        $to_create = [];
        foreach ($period as $date) {
            $date = $date->format('Y-m-d');
            if (array_key_exists($date, $data)) {
                if ($data[$date]['priority'] <= $this->priority) {
                    continue;
                }
                $to_delete[] = $data[$date]['id'];
                $to_create[] = $date;
            } else {
                $to_create[] = $date;
            }
        }


        DB::beginTransaction();
        if (!empty($to_create)) {
            foreach ($to_create as $date) {
                TaskChild::create([
                    'master_id' => $this->id,
                    'date' => $date,
                    'status' => TaskChild::STATUS_NEW
                ]);
            }
        }

        if (!empty($to_delete)) {
            TaskChild::whereIn('id', $to_delete)->delete();
        }
        DB::commit();

        $this->status = self::STATUS_SPLIT;
        $this->save();
    }

    public function splitPrediction()
    {
        DataItem::select('channel_id', 'rule_id')
            ->where('customer_id', $this->customer_id)
            ->where('status', DataItem::STATUS_ACTIVE)
            ->where('predicted', 0)
            ->distinct()
            ->get()->map(function ($unit) {
                PredictionChild::create([
                    'channel_id' => $unit['channel_id'],
                    'rule_id' => $unit['rule_id'],
                    'status' => PredictionChild::STATUS_NEW,
                    'master_id' => $this->id,
                ]);
            });

        $this->status = self::STATUS_SPLIT;
        $this->save();
    }

    public function progress()
    {
        if ($this->is_prediction()) {
            $total_count = DataItem::select('rule_id')
                ->where('customer_id', $this->customer_id)
                ->where('status', DataItem::STATUS_ACTIVE)
                ->where('predicted', 0)
                ->distinct()
                ->count();
        } else {
            $total_count = iterator_count($this->getPeriod());
        }

        $items_count = $this->child_tasks()->count();

        return $total_count > 0 ? 100 - (int)($items_count / $total_count * 100) : 0;
    }

    /* RELATIONS */

    public function child_tasks()
    {
        if ($this->is_prediction())
            return $this->hasMany(PredictionChild::class, 'master_id');
        return $this->hasMany(TaskChild::class, 'master_id');
    }

    private function getPeriod()
    {
        $start = new DateTime($this->date_from);
        $interval = new DateInterval('P1D');
        $finish = new DateTime($this->date_to);
        return new DatePeriod($start, $interval, $finish->modify('+ 1 day'));
    }

    public function customer()
    {
        return $this->belongsTo(Customer::class, 'customer_id');
    }

    public function connector()
    {
        return $this->belongsTo(Connector::class, 'connector_id');
    }

    public function is_prediction()
    {
        return ($this->type == self::TYPE_HISTORICAL_PREDICTION || $this->type == self::TYPE_FUTURE_PREDICTION);
    }
}
