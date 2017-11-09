<?php

namespace App\Models;

use DateTime;
use DB;
use Illuminate\Support\Facades\Log;
use PDO;
use DatePeriod;
use DateInterval;

class DataDispatcher
{
    private $days_left = 0;

    private $customer;

    public function __construct(Customer $customer)
    {
        $this->customer = $customer;
    }

    public function getChannelsSources($by_channel = false)
    {
        $id = $this->customer->id;
        static $result = [];
        if (!isset($result[$id])) {
            $response = DataChannel::select('data_channels.id as channel_id', 'data_channel_rules.id as source_id', 'data_channel_rules.label as source')
                ->join('data_channel_rules', 'data_channels.id', '=', 'data_channel_rules.channel_id')
                ->where('customer_id', $id)
                ->whereIn('data_channels.id', DataChannel::getCustomerChannels($this->customer))
                ->get();

            foreach ($response as $row) {
                $params = $by_channel ? [$id, $row->channel_id, $row->source_id] : [$id, $row->source_id];
                if (!array_has($result, implode('.', $params))) {
                    array_set($result, implode('.', $params), $row->source);
                }
            }
        }
        return !empty($result[$id]) ? $result[$id] : [];
    }

    public function getConnectors()
    {
        $id = $this->customer->id;
        static $connectors = [];
        if (!isset($connectors[$id])) {
            $connectors[$id] = [];
            foreach ($this->customer->configurations as $connection) {
                $connectors[$id][] = $connection->connector_id;
            }
        }
        return $connectors[$id];
    }

    private function metricBlank($metric)
    {
        return DataLabel::isCustomerConversion($this->customer, $metric) ? ['value' => 0, 'cpr' => 0] : 0;
    }

    public function setForecastPeriod($period, DateTime $from = null)
    {
        $per = $this->getCurrentPeriodDates($period, $from);
        $date = new DateTime($from);
        $this->days_left = $per['end']->diff($date)->days;
    }

    public function getPeriodStartDate($period)
    {
        $date = new DateTime();
        return $period == "year-end" ? $date->modify('Jan 1') : $date->modify('first day of this month');
    }

    public function getCurrentPeriodDates($period, DateTime $from = null)
    {
        $date = new DateTime($from);
        $curr_day = $date->format('j');
        $start_date = $period == "year-end"
            ? $date->modify('Jan 1') : $date->modify('first day of this month');
        $end_date = clone $start_date;

        $count = 1;
        $units = 'month';

        switch ($period) {
            case "3-months":
                $count = ($curr_day > 15 ? 4 : 3);
                break;
            case "6-months":
                $count = ($curr_day > 15 ? 7 : 6);
                break;
            case "12-months":
                $count += ($curr_day > 15 ? 13 : 12);
                break;
            case "year-end":
                $units = 'year';
                break;
        }

        $end_date->modify('+ ' . $count . ' ' . $units)->modify('- 1 day');

        return [
            'start' => $start_date,
            'end' => $end_date,
        ];
    }

    public function getPreviousPeriodDates($period, DateTime $from = null)
    {
        $per = $this->getCurrentPeriodDates($period, $from);
        $days = $per['end']->diff($per['start'])->days;
        $days++;
        return [
            'start' => $per['start']->modify('- ' . $days . ' days'),
            'end' => $per['end']->modify('- ' . $days . ' days'),
        ];
    }

    public function getQueryDates($period, DateTime $from = null)
    {
        $predicted = $this->getCurrentPeriodDates($period);
        $historical = $this->getPreviousPeriodDates($period);
        if ($from) {
            $historical['start'] = $from;
        }

        $historical['end'] = (new DateTime($this->customer->last_sync));
        $predicted['start'] = (new DateTime($this->customer->last_sync))->modify('+ 1 day');

        return [
            'historical' => $historical,
            'predicted' => $predicted,
        ];
    }

    public function getDaysLeft()
    {
        return $this->days_left;
    }

    public function makeItemsGroups(DateTime $from, DateTime $to)
    {
        $diff = $from->diff($to);


        $days = $diff->days;
        $weeks = intval($days / 7);

        if ($days <= 30) { //group by days
            $grouping = 'days';
            $format = 'd M Y';
        } elseif ($weeks <= 15) { //group by weeks
            $grouping = 'weeks';
            $format = 'W \w\e\e\k Y';
        } else { //leave grouping by month
            $grouping = 'months';
            $format = 'M Y';
        }

        $daterange = new DatePeriod($from, new DateInterval('P1D'), $to);

        $dates = [];
        foreach ($daterange as $date) {
            array_push($dates, $date->format($format));
        }
        $dates = array_values(array_unique($dates));

        return [$grouping, $format, $dates];
    }

    private function metrics4data()
    {
        $data = [];
        foreach (DataLabel::getSingleMetrics() as $metric) {
            $data[$metric] = 0;
        }

        foreach (DataLabel::getCustomerConversions($this->customer) as $metric) {
            $data[$metric] = 0;
        }
        return $data;
    }

    private function get7DayAverage($requested_channel = null)
    {
        $date_diff = 7;

        $date_to = (new DateTime($this->customer->last_sync));
        $date_from = clone $date_to;
        $date_from->modify("-" . ($date_diff - 1) . " days");

        DB::setFetchMode(PDO::FETCH_ASSOC);
        $query = DB::table('data_items')->
        select('item_value as value', 'name as metric', 'channel_id as channel', 'rule_id as source')
            ->leftJoin('data_labels', function ($leftJoin) {
                $leftJoin->on('data_labels.id', '=', 'data_items.label_id');
            })
            ->where('data_items.customer_id', '=', $this->customer->id)
            ->where('status', DataItem::STATUS_ACTIVE)
            ->where('predicted', 0)
            ->whereBetween('item_date', [$date_from->format('Y-m-d'), $date_to->format('Y-m-d')])
            ->whereIn('connector_id', $this->getConnectors())
            ->whereIn('label_id', array_keys(DataLabel::getCustomerMetrics($this->customer)));

        if ($requested_channel) {
            $query->where('channel_id', $requested_channel);
        } else {
            $query->whereIn('channel_id', DataChannel::getCustomerChannels($this->customer));
        }

        $items = $query->get();

        $data = [];
        $metrics4data = $this->metrics4data();
        foreach ($items as $itemIdx => $item) {
            $channel = (int)$item['channel'];
            $source = (int)$item['source'];
            $metric = $item['metric'];

            if (!isset($data[$channel])) {
                $data[$channel] = [
                    'sources' => [],
                    'metrics' => $metrics4data
                ];
            }

            if (!isset($data[$channel]['sources'][$source])) {
                $data[$channel]['sources'][$source] = $metrics4data;
            }

            $data[$channel]['sources'][$source][$metric] += $item['value'];
            $data[$channel]['metrics'][$metric] += $item['value'];
        }


        foreach ($data as $channel => $channel_data) {
            $channel_prepared = false;
            foreach ($channel_data['sources'] as $source => $metrics) {

                foreach (DataLabel::getCustomerConversions($this->customer) as $metric) {
                    if (!$channel_prepared) {
                        $channel_cpr = $data[$channel]['metrics'][$metric] != 0
                            ? $data[$channel]['metrics']['Cost'] / $data[$channel]['metrics'][$metric] : 0;

                        $channel_conv = $data[$channel]['metrics']['Sessions'] != 0
                            ? $data[$channel]['metrics'][$metric] / $data[$channel]['metrics']['Sessions'] : 0;

                        $data[$channel]['metrics'][$metric] = [
                            'value' => $data[$channel]['metrics'][$metric] / $date_diff,
                            'cpr' => $channel_cpr,
                            'conv' => $channel_conv
                        ];
                    }

                    $source_cpr = $data[$channel]['sources'][$source][$metric] != 0
                        ? $data[$channel]['sources'][$source]['Cost'] / $data[$channel]['sources'][$source][$metric] : 0;

                    $source_conv = $data[$channel]['sources'][$source]['Sessions'] != 0
                        ? $data[$channel]['sources'][$source][$metric] / $data[$channel]['sources'][$source]['Sessions'] : 0;


                    $data[$channel]['sources'][$source][$metric] = [
                        'value' => $data[$channel]['sources'][$source][$metric] / $date_diff,
                        'cpr' => $source_cpr,
                        'conv' => $source_conv
                    ];
                }

                foreach (DataLabel::getSingleMetrics() as $metric) {
                    $data[$channel]['sources'][$source][$metric] /= $date_diff;
                    if (!$channel_prepared) {
                        $data[$channel]['metrics'][$metric] /= $date_diff;
                    }
                }
                $channel_prepared = true;
            }
        }
        return $data;
    }

    public function getPredictedData(DateTime $date_from, DateTime $date_to, $budget = null, $requested_channel = null)
    {
        DB::setFetchMode(PDO::FETCH_ASSOC);
        $query = DB::table('data_items')->
        select('item_value as value', 'name as metric', 'channel_id as channel', 'rule_id as source')
            ->leftJoin('data_labels', function ($leftJoin) {
                $leftJoin->on('data_labels.id', '=', 'data_items.label_id');
            })
            ->where('data_items.customer_id', '=', $this->customer->id)
            ->where('status', DataItem::STATUS_ACTIVE)
            ->where('predicted', 1)
            ->whereBetween('item_date', [$date_from->format('Y-m-d'), $date_to->format('Y-m-d')])
            ->whereIn('connector_id', $this->getConnectors())
            ->whereIn('label_id', array_keys(DataLabel::getCustomerMetrics($this->customer)));

        if ($requested_channel) {
            $query->where('channel_id', $requested_channel);
        } else {
            $query->whereIn('channel_id', DataChannel::getCustomerChannels($this->customer));
        }
        $items = $query->get();

        $data = [];
        $metrics4data = $this->metrics4data();

        // summ items
        foreach ($items as $item) {
            $channel = (int)$item['channel'];
            $source = (int)$item['source'];
            $metric = $item['metric'];

            if (!isset($data[$channel])) {
                $data[$channel] = [
                    'sources' => [],
                    'metrics' => $metrics4data
                ];
            }

            if (!isset($data[$channel]['sources'][$source])) {
                $data[$channel]['sources'][$source] = $metrics4data;
            }

            $data[$channel]['sources'][$source][$metric] += $item['value'];
            $data[$channel]['metrics'][$metric] += $item['value'];
        }

        $avg_data = $this->get7DayAverage($requested_channel);

        if ($budget) {
            foreach ($data as $channel => $channel_data) {
                if (!$budget[$channel]) {
                    continue;
                }

                $new_channel_budget = (float)$budget[$channel];
                $old_channel_budget = $avg_data[$channel]['metrics']['Cost'] * 30;

                $channel_coef = $old_channel_budget != 0 ? $new_channel_budget / $old_channel_budget : 0;

                $channel_calculated = false;
                foreach ($channel_data['sources'] as $source => $metrics) {

                    foreach ($metrics as $metric => $metric_data) {
                        if (!$channel_calculated) {
                            $data[$channel]['metrics'][$metric] *= $channel_coef;
                        }
                        $data[$channel]['sources'][$source][$metric] *= $channel_coef;
                    }
                    $channel_calculated = true;
                }
            }
        }

        // calculate cpr and budget
        foreach ($data as $channel => $channel_data) {

            $channel_prepared = false;
            $channel_budget = isset($budget[$channel]) ? (float)$budget[$channel] : $avg_data[$channel]['metrics']['Cost'] * 30;
            $channel_cost = $data[$channel]['metrics']['Cost'];
            $data[$channel]['metrics']['Budget'] = $channel_budget;

            foreach ($channel_data['sources'] as $source => $metrics) {

                $source_cost = $data[$channel]['sources'][$source]['Cost'];
                if ($channel_cost != 0) {
                    $source_weight = $source_cost / $channel_cost;
                    $source_budget = $channel_budget * $source_weight;
                } else {
                    $source_budget = 0;
                }

                $data[$channel]['sources'][$source]['Budget'] = $source_budget;

                foreach (DataLabel::getCustomerConversions($this->customer) as $metric) {
                    if (!$channel_prepared) {
                        $channel_cpr = $data[$channel]['metrics'][$metric] != 0
                            ? $data[$channel]['metrics']['Cost'] / $data[$channel]['metrics'][$metric] : 0;

                        $channel_conv = $data[$channel]['metrics']['Clicks'] != 0
                            ? $data[$channel]['metrics'][$metric] / $data[$channel]['metrics']['Clicks'] * 100 : 0;

                        $data[$channel]['metrics'][$metric] = [
                            'value' => $data[$channel]['metrics'][$metric],
                            'cpr' => $channel_cpr,
                            'conv' => $channel_conv
                        ];
                    }

                    $source_cpr = $data[$channel]['sources'][$source][$metric] != 0
                        ? $data[$channel]['sources'][$source]['Cost'] / $data[$channel]['sources'][$source][$metric] : 0;

                    $source_conv = $data[$channel]['sources'][$source]['Clicks'] * 100 != 0
                        ? $data[$channel]['sources'][$source][$metric] / $data[$channel]['sources'][$source]['Clicks'] * 100 : 0;

                    $data[$channel]['sources'][$source][$metric] = [
                        'value' => $data[$channel]['sources'][$source][$metric],
                        'cpr' => $source_cpr,
                        'conv' => $source_conv
                    ];
                }
                $channel_prepared = true;
            }
        }
        return $data;
    }

    public function getForecastData($period, $metric)
    {
        $channels = DataChannel::getCustomerChannels($this->customer);
        $data = ['prediction' => 0, 'diff' => 0, 'confidence' => 0];
        if (!empty($channels)) {

            $current_period = $this->getCurrentPeriodDates($period);
            $previous_period = $this->getPreviousPeriodDates($period);

            $prev_prediction = 0;
            $prev_value = 0;

            $items = DataItem::select('item_value as value', 'predicted')
                ->where('customer_id', $this->customer->id)
                ->where('status', DataItem::STATUS_ACTIVE)
                ->whereIn('predicted', [0, 1])
                ->whereBetween('item_date', [
                    $previous_period['start']->format('Y-m-d'),
                    $previous_period['end']->format('Y-m-d')
                ])->whereIn('connector_id', $this->getConnectors())
                ->whereIn('channel_id', $channels)
                ->where('label_id', $metric)
                ->get()->toArray();

            foreach ($items as $item) {
                if ($item['predicted'] == 1) {
                    $prev_prediction += $item['value'];
                } else {
                    $prev_value += $item['value'];
                }
            }
            unset($items);


            if ($prev_value != 0) {
                $dates = $this->getQueryDates($period, $current_period['start']);
                $items = DataItem::select('item_value as value', 'predicted')
                    ->where('customer_id', $this->customer->id)
                    ->where('status', DataItem::STATUS_ACTIVE)
                    ->where(function ($query) use ($dates) {
                        $query->where(function ($query) use ($dates) {
                            $query->where('predicted', 1);
                            $query->whereBetween('item_date', [
                                $dates['predicted']['start']->format('Y-m-d'),
                                $dates['predicted']['end']->format('Y-m-d')
                            ]);
                        });
                        $query->orWhere(function ($query) use ($dates) {
                            $query->where('predicted', 0);
                            $query->whereBetween('item_date', [
                                $dates['historical']['start']->format('Y-m-d'),
                                $dates['historical']['end']->format('Y-m-d')
                            ]);
                        });
                    })
                    ->whereIn('connector_id', $this->getConnectors())
                    ->whereIn('channel_id', $channels)
                    ->where('label_id', $metric)
                    ->get()->toArray();

                foreach ($items as $item) {
                    $data['prediction'] += $item['value'];
                }

                $data['diff'] = round(($data['prediction'] - $prev_value) / $prev_value * 100, 0);
                $data['confidence'] = max(0, 100 - round(abs($prev_prediction - $prev_value) / $prev_value * 100, 0));
            }
        }
        return $data;
    }

    public function getChartData($label, $period, DateTime $from)
    {
        $dates = $this->getQueryDates($period, $from);

        list($groupType, $groupDateFormat, $groups) = $this->makeItemsGroups($dates['historical']['start'], $dates['predicted']['end']);

        if ($groupType !== 'days') {
            $dates['predicted']['start'] = (new DateTime())->modify('first day of this month');
        }

        $items = DataItem::select('id', 'item_date as date', 'item_value as value', 'predicted', 'channel_id as channel')
            ->where('customer_id', $this->customer->id)
            ->where('status', DataItem::STATUS_ACTIVE)
            ->where(function ($query) use ($dates) {
                $query->where(function ($query) use ($dates) {
                    $query->where('predicted', 1);
                    $query->whereBetween('item_date', [
                        $dates['predicted']['start']->format('Y-m-d'),
                        $dates['predicted']['end']->format('Y-m-d')
                    ]);
                });
                $query->orWhere(function ($query) use ($dates) {
                    $query->where('predicted', 0);
                    $query->whereBetween('item_date', [
                        $dates['historical']['start']->format('Y-m-d'),
                        $dates['historical']['end']->format('Y-m-d')
                    ]);
                });
            })
            ->whereIn('connector_id', $this->getConnectors())
            ->whereIn('channel_id', DataChannel::getCustomerChannels($this->customer))
            ->where('label_id', $label)
            ->get()->toArray();

        $indexes = array_flip($groups);
        $data = array_fill_keys($groups, []);
        foreach ($items as $item) {
            $item_date = (new DateTime($item['date']))->format($groupDateFormat);

            // отсекаются только те, которые предсказанные и меньше текущей даты
            if ($item['predicted'] == 1 && $item['date'] < $this->customer->last_sync) {
                continue;
            }

            if (!isset($data[$item_date][$item['channel']])) {
                $data[$item_date][$item['channel']] = [
                    'channel' => $item['channel'],
                    'date' => $indexes[$item_date],
                    'value' => 0,
                    'predicted' => 0
                ];
            }

            $valueField = $item['predicted'] == 0 ? 'value' : 'predicted';
            $data[$item_date][$item['channel']][$valueField] += $item['value'];
        }

        $result = [];
        foreach ($data as $items) {
            foreach ($items as $item) {
                $result[] = $item;
            }
        };

        return [
            'data' => $result,
            'groups' => $groups,
            'grouping' => $groupType,
        ];
    }

    public function getTimelineData($period, DateTime $from)
    {
        $dates = $this->getQueryDates($period, $from);
        list($groupType, $groupDateFormat, $groups) = $this->makeItemsGroups($dates['historical']['start'], $dates['predicted']['end']);

        $items = Timeline::select('id', 'date', 'description')
            ->where('customer_id', $this->customer->id)
            ->whereBetween('date', [
                $dates['historical']['start']->format('Y-m-d'),
                $dates['predicted']['end']->format('Y-m-d')
            ])->get();

        $groups = ['data' => $groups, 'format' => $groupType];
        $data = [];

        foreach ($items as $item) {
            $item->date = (new DateTime($item->date))->format($groupDateFormat);
            $data[] = $item;
        }

        usort($data, function ($a, $b) {
            if ($a->date == $b->date) {
                return $a->id < $b->id ? -1 : 1;
            }
            return $a->date < $b->date ? 1 : -1;
        });

        return ['groups' => $groups, 'items' => $data];
    }

    public function getHistoricalData(DateTime $date_from, DateTime $date_to, $requested_channel = false)
    {
        DB::setFetchMode(PDO::FETCH_ASSOC);
        $query = DB::table('data_items')->
        select('item_value as value', 'name as metric', 'channel_id as channel', 'rule_id as source')
            ->leftJoin('data_labels', function ($leftJoin) {
                $leftJoin->on('data_labels.id', '=', 'data_items.label_id');
            })
            ->where('data_items.customer_id', '=', $this->customer->id)
            ->where('status', DataItem::STATUS_ACTIVE)
            ->where('predicted', 0)
            ->whereBetween('item_date', [$date_from->format('Y-m-d'), $date_to->format('Y-m-d')])
            ->whereIn('connector_id', $this->getConnectors())
            ->whereIn('label_id', array_keys(DataLabel::getCustomerMetrics($this->customer)));

        if ($requested_channel) {
            $query->where('channel_id', $requested_channel);
        } else {
            $query->whereIn('channel_id', DataChannel::getCustomerChannels($this->customer));
        }

        $items = $query->get();

        $data = [];
        $metrics4data = $this->metrics4data();
        foreach ($items as $itemIdx => $item) {
            $channel = (int)$item['channel'];
            $source = (int)$item['source'];
            $metric = $item['metric'];

            if (!isset($data[$channel])) {
                $data[$channel] = [
                    'sources' => [],
                    'metrics' => $metrics4data
                ];
            }

            if (!isset($data[$channel]['sources'][$source])) {
                $data[$channel]['sources'][$source] = $metrics4data;
            }

            $data[$channel]['sources'][$source][$metric] += $item['value'];
            $data[$channel]['metrics'][$metric] += $item['value'];
        }

        foreach ($data as $channel => $channel_data) {
            $channel_prepared = false;
            foreach ($channel_data['sources'] as $source => $metrics) {
                foreach (DataLabel::getCustomerConversions($this->customer) as $metric) {
                    if (!$channel_prepared) {
                        $channel_cpr = $data[$channel]['metrics'][$metric] != 0
                            ? $data[$channel]['metrics']['Cost'] / $data[$channel]['metrics'][$metric] : 0;

                        $channel_conv = $data[$channel]['metrics']['Clicks'] != 0
                            ? $data[$channel]['metrics'][$metric] / $data[$channel]['metrics']['Clicks'] * 100 : 0;

                        $data[$channel]['metrics'][$metric] = [
                            'value' => $data[$channel]['metrics'][$metric],
                            'cpr' => $channel_cpr,
                            'conv' => $channel_conv
                        ];
                    }

                    $source_cpr = $data[$channel]['sources'][$source][$metric] != 0
                        ? $data[$channel]['sources'][$source]['Cost'] / $data[$channel]['sources'][$source][$metric] : 0;

                    $source_conv = $data[$channel]['sources'][$source]['Clicks'] != 0
                        ? $data[$channel]['sources'][$source][$metric] / $data[$channel]['sources'][$source]['Clicks'] * 100 : 0;


                    $data[$channel]['sources'][$source][$metric] = [
                        'value' => $data[$channel]['sources'][$source][$metric],
                        'cpr' => $source_cpr,
                        'conv' => $source_conv
                    ];
                }

                $channel_prepared = true;
            }
        }

        return $data;
    }

    public function getDataSummary(array $predicted_data, array $historical_data)
    {
        $data = [];
        foreach ($predicted_data as $channel => $channel_data) {
            $channel_calculated = false;
            $data[$channel] = [
                'sources' => [],
                'metrics' => $this->metrics4data(),
            ];

            foreach ($channel_data['sources'] as $source => $metrics) {
                $data[$channel]['sources'][$source] = $this->metrics4data();
                foreach ($metrics as $metric => $predicted) {
                    $predicted_channel = $predicted_data[$channel]['metrics'][$metric];
                    $historical_channel = $historical = $this->metricBlank($metric);
                    if (isset($historical_data[$channel]) && isset($historical_data[$channel]['metrics'][$metric])) {
                        $historical_channel = $historical_data[$channel]['metrics'][$metric];
                        if (isset($historical_data[$channel]['sources'][$source])) {
                            $historical = $historical_data[$channel]['sources'][$source][$metric];
                        }
                    }
                    if (DataLabel::isCustomerConversion($this->customer, $metric)) {
                        if (!is_array($data[$channel]['sources'][$source][$metric])) {
                            $data[$channel]['sources'][$source][$metric] = ['value' => 0, 'cpr' => 0];
                        }

                        if (!$channel_calculated) {
                            if (!is_array($data[$channel]['metrics'][$metric])) {
                                $data[$channel]['metrics'][$metric] = ['value' => 0, 'cpr' => 0];
                            }

                            $data[$channel]['metrics'][$metric]['value'] += $predicted_channel['value'];
                            $data[$channel]['metrics'][$metric]['value'] += $historical_channel['value'];

                            if ($data[$channel]['metrics'][$metric]['value'] != 0) {
                                $channel_cpr = $data[$channel]['metrics']['Cost'] / $data[$channel]['metrics'][$metric]['value'];
                                $data[$channel]['metrics'][$metric]['cpr'] = $channel_cpr;
                            }
                        }


                        $data[$channel]['sources'][$source][$metric]['value'] += $predicted['value'];
                        $data[$channel]['sources'][$source][$metric]['value'] += $historical['value'];

                        if ($data[$channel]['sources'][$source][$metric]['value'] != 0) {
                            $source_cpr = $data[$channel]['sources'][$source]['Cost'] / $data[$channel]['sources'][$source][$metric]['value'];
                            $data[$channel]['sources'][$source][$metric]['cpr'] = $source_cpr;
                        }

                    } else {
                        if (!$channel_calculated) {
                            if (!isset($data[$channel]['metrics'][$metric])) {
                                $data[$channel]['metrics'][$metric] = 0;
                            }

                            $data[$channel]['metrics'][$metric] += $predicted_channel;
                            $data[$channel]['metrics'][$metric] += $historical_channel;
                        }

                        // for those metrics which are calculated (like Budget)
                        if (!isset($data[$channel]['sources'][$source][$metric])) {
                            $data[$channel]['sources'][$source][$metric] = 0;
                        }

                        $data[$channel]['sources'][$source][$metric] += $predicted;
                        $data[$channel]['sources'][$source][$metric] += $historical;
                    }
                }
                $channel_calculated = true;
            }
        }
        return $data;
    }
}
