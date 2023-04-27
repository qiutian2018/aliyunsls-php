<?php

/**
 * 阿里云日志服务查询
 * Class SlsQuery
 */
class SlsQuery
{

    // 日志项目名
    public static function logProject()
    {
        return 'web-logger';
    }

    // 阿里云web-logger项目storage列表
    public static function webLoggerStorageList()
    {
        return ['push', 'logger', 'tr_logger'];
    }

    // 最近一年的月份
    public static function monthList()
    {
        $list = [];
        for ($i = 0; $i < 12; $i++) {
            $time = strtotime('-' . $i . ' month');
            $list[] = date('Y-m', $time);
        }
        return $list;
    }

    // 阿里云账户配置
    public static function getAliConfig()
    {
        // todo::you aliyun ak config
        $endpoint = '';
        $accessKeyId = '';
        $accessKey = '';
        return [$endpoint, $accessKeyId, $accessKey, ""];
    }

    // 从阿里云日志服务查数据,并入库
    public static function queryFromAliSls($end, $interval = 86400)
    {
        return ['success' => false, 'msg' => 'invalid opt', 'data' => []];

        /*
        list($endpoint, $accessKeyId, $accessKey) = self::getAliConfig();
        $project = 'all-web-log';
        $logstore = 'all-web-logs-store';
        $token = "";

        $client = new \Aliyun_Log_Client($endpoint, $accessKeyId, $accessKey, $token);
        $query = "* | SELECT http_host, COUNT(*) as pv,COUNT(DISTINCT remote_user) as uv GROUP BY http_host LIMIT 50";
        $req = new \Aliyun_Log_Models_GetLogsRequest($project, $logstore, $end - $interval, $end, '', $query, 0, 0, false, true);
        try {
            $rep = $client->getLogs($req);
            $today = date('Ymd');
            $suc = 0;
            foreach ($rep->getLogs() as $item) {
                $content = $item->getContents();
                $data = [
                    'http_host' => $content['http_host'],
                    'pv' => $content['pv'],
                    'uv' => $content['uv'],
                    'day' => $today,
                    'storage' => $logstore,
                ];
                $exist = self::existHostDayData($logstore, $data['http_host'], $today);
                if ($exist) {
                    Db::name('host_day')->where('id', '=', $exist['id'])->update($data);
                } else {
                    $id = Db::name('host_day')->insertGetId($data);
                    $id and $suc++;
                }
            }
            return ['success' => true, 'msg' => '', 'data' => ['suc' => $suc]];
        } catch (\Aliyun_Log_Exception $e) {
            return ['success' => false, 'msg' => $e->getMessage(), 'data' => []];
        }
        */
    }

    // 从库里查host每月的分组数据
    public static function hostDayListDataByMonth($month, $storage)
    {
        return Db::name('host_day')->field('pv,uv,http_host,day')
            ->where('storage', '=', $storage)
            ->where('day', 'like', $month . '%')
            ->where('day', '<>', date('Ymd'))
            ->order('day', 'desc')
            ->select()->toArray();
    }

    // 当天的host数据是否存在
    public static function existHostDayData($storage, $host, $day)
    {
        return Db::name('host_day')
            ->where('storage', '=', $storage)
            ->where('http_host', '=', $host)
            ->where('day', '=', $day)
            ->find();
    }

    // 从阿里云日志服务里面查host统计数据
    public static function queryWebLoggerFromAliSls($end, $interval = 86400, $logstore = 'push', $day = '', $syncDb = true, $update = false)
    {
        list($endpoint, $accessKeyId, $accessKey, $token) = self::getAliConfig();
        $project = self::logProject();
        $today = $day ? $day : date('Ymd', $end);
        $suc = 0;
        $tmp = [];

        $client = new \Aliyun_Log_Client($endpoint, $accessKeyId, $accessKey, $token);
        $query = "* | SELECT count(*) as pv,COUNT(__source__) as uv,url GROUP BY url LIMIT 1000";
        $req = new \Aliyun_Log_Models_GetLogsRequest($project, $logstore, $end - $interval, $end, '', $query, 0, 0, false, true);
        try {
            $rep = $client->getLogs($req);
            foreach ($rep->getLogs() as $item) {
                $content = $item->getContents();
                $data = [
                    'http_host' => $content['url'],
                    'pv' => $content['pv'],
                    'uv' => $content['uv'],
                    'day' => $today,
                    'storage' => $logstore,
                ];
                if ($syncDb) {
//                    $id = self::hostWriteDb($data, $update);
//                    $id and $suc++;
                } else {
                    $tmp[] = $data;
                }
            }
            return ['success' => true, 'msg' => '', 'data' => ['suc' => $suc, 'tmp' => $tmp]];
        } catch (\Aliyun_Log_Exception $e) {
            return ['success' => false, 'msg' => $e->getMessage(), 'data' => ['suc' => $suc, 'tmp' => $tmp]];
        }
    }

    // 当天的item数据是否存在
    public static function existItemDayData($storage, $itemId, $day)
    {
        return Db::name('item_day')
            ->where('storage', '=', $storage)
            ->where('item_id', '=', $itemId)
            ->where('day', '=', $day)
            ->find();
    }

    // 从阿里云日志服务里查item统计
    public static function queryWebLoggerItemFromAliSls($end, $interval = 86400, $logstore = 'push', $day = '', $syncDb = true, $update = false)
    {
        list($endpoint, $accessKeyId, $accessKey, $token) = self::getAliConfig();
        $project = self::logProject();
        $today = $day ? $day : date('Ymd', $end);
        $suc = 0;
        $tmp = [];

        $client = new Aliyun_Log_Client($endpoint, $accessKeyId, $accessKey, $token);
        $query = "* | SELECT count(*) as pv,COUNT(__source__) as uv,item_id GROUP BY item_id LIMIT 100000";
        $req = new Aliyun_Log_Models_GetLogsRequest($project, $logstore, $end - $interval, $end, '', $query, 0, 0, false, true);
        try {
            $rep = $client->getLogs($req);
            foreach ($rep->getLogs() as $item) {
                $content = $item->getContents();
                $data = [
                    'item_id' => $content['item_id'],
                    'pv' => $content['pv'],
                    'uv' => $content['uv'],
                    'day' => $today,
                    'storage' => $logstore,
                ];
                if ($syncDb) {
                    // $id = self::itemWriteDb($data, $update);
                    // $id and $suc++;
                } else {
                    $tmp[] = $data;
                }
            }
            return ['success' => true, 'msg' => '', 'data' => ['suc' => $suc, 'tmp' => $tmp]];
        } catch (Aliyun_Log_Exception $e) {
            return ['success' => false, 'msg' => $e->getMessage(), 'data' => ['suc' => $suc, 'tmp' => $tmp]];
        }
    }

    // 从库里查item每月的分组数据
    public static function itemDayListDataByMonth($month, $storage)
    {
        return Db::name('item_day')->field('pv,uv,item_id,day')
            ->where('storage', '=', $storage)
            ->where('day', 'like', $month . '%')
            ->where('day', '<>', date('Ymd'))
            ->order('day', 'desc')
            ->select()->toArray();
    }

    // host每日数据写库
    public static function hostWriteDb($data, $update)
    {
        $exist = self::existHostDayData($data['storage'], $data['http_host'], $data['day']);
        if (!$exist) {
            return Db::name('host_day')->insertGetId($data);
        } else {
            $update and Db::name('host_day')->where('id', '=', $exist['id'])->update($data);
            return 0;
        }
    }

    // item每日数据写库
    public static function itemWriteDb($data, $update)
    {
        $exist = self::existItemDayData($data['storage'], $data['item_id'], $data['day']);
        if (!$exist) {
            return Db::name('item_day')->insertGetId($data);
        } else {
            $update and Db::name('item_day')->where('id', '=', $exist['id'])->update($data);
            return 0;
        }
    }

}