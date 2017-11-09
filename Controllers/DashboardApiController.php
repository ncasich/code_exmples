<?php

namespace App\Http\Controllers;

use App\Models\Customer;
use App\Models\DataChannel;
use App\Models\DataLabel;
use DateTime;
use Illuminate\Http\Request;
use DB;
use App\Models\DataDispatcher;
use Illuminate\Support\Facades\Auth;


class DashboardApiController extends Controller
{
    /**
     * @var Customer
     */
    private $customer;

    public function __construct(Customer $customer)
    {
        if (!Auth::user()->can('viewCustomer', $customer)) {
            abort(403);
        }

        $this->customer = $customer;
    }

    public function getMetrics()
    {
        return DataLabel::getCustomerMetrics($this->customer);
    }

    public function getChannels()
    {
        return DataChannel::getCustomerChannels($this->customer, true);
    }

    public function getChannelsSources()
    {
        $dispatcher = new DataDispatcher($this->customer);
        return $dispatcher->getChannelsSources();
    }

    public function getResultsMetrics()
    {
        $results = [];
        foreach ($this->customer->conversionSettings as $resultSet) {
            foreach ($resultSet->results as $result) {
                if ($result->metric !== null) {
                    $results[$result->metric] = $resultSet->name;
                }
            }
        }

        return $results;
    }

    public function getTimelineData(Request $request)
    {
        $this->validate($request, [
            'adjust' => 'required',
            'dateFrom' => 'required|date_format:m/d/Y',
        ]);

        $period = $request->input('adjust');
        $date_from = new DateTime($request->input('dateFrom'));
        $dispatcher = new DataDispatcher($this->customer);
        return $dispatcher->getTimelineData($period, $date_from);
    }

    public function getChartData(Request $request)
    {
        $this->validate($request, [
            'adjust' => 'required',
            'dateFrom' => 'required|date_format:m/d/Y',
            'metric' => 'required',
        ]);

        $date_from = new DateTime($request->input('dateFrom'));
        $period = $request->input('adjust');
        $metric = $request->input('metric');
        $dispatcher = new DataDispatcher($this->customer);
        return $dispatcher->getChartData($metric, $period, $date_from);
    }

    public function getPerformanceData(Request $request)
    {
        $this->validate($request, [
            'dateFrom' => 'required|date_format:m/d/Y',
        ]);

        $date_from = new DateTime($request->input('dateFrom'));
        $date_to = new DateTime($this->customer->last_sync);
        $dispatcher = new DataDispatcher($this->customer);
        return $dispatcher->getHistoricalData($date_from, $date_to);
    }

    public function getForecastData(Request $request)
    {
        $this->validate($request, [
            'adjust' => 'required',
            'metric' => 'required',
        ]);

        $period = $request->input('adjust');
        $metric = $request->input('metric');
        $dispatcher = new DataDispatcher($this->customer);
        return $dispatcher->getForecastData($period, $metric);
    }

    public function getAdjustData(Request $request)
    {
        $this->validate($request, [
            'adjust' => 'required',
        ]);

        $dispatcher = new DataDispatcher($this->customer);
        $period = $request->input('adjust');
        $channel = $request->input('channel', false);
        $budget = $channel ? [$channel => $request->input('value', 0)] : [];

        $current_period = $dispatcher->getCurrentPeriodDates($period);
        $dates = $dispatcher->getQueryDates($period, $current_period['start']);

        $predicted = $dates['predicted'];
        $predicted_data = $dispatcher->getPredictedData($predicted['start'], $predicted['end'], $budget, $channel);

        $historical = $dates['historical'];
        $historical_data = $dispatcher->getHistoricalData($historical['start'], $historical['end'], $channel);
        $data = $dispatcher->getDataSummary($predicted_data, $historical_data);

        $response = [];
        foreach ($data as $channel => $channel_data) {
            $response[$channel] = [];
            foreach ($channel_data['metrics'] as $metric => $value) {
                $response[$channel][$metric] = $value;
            }
        }

        $current_period = $dispatcher->getCurrentPeriodDates($period);

        return [
            'data' => $response,
            'date_from' => $current_period['start']->format('M d Y'),
            'date_to' => $current_period['end']->format('M d Y'),
        ];
    }
}
