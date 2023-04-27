<?php
require_once '../vendor/autoload.php';
require_once 'SlsQuery.php';

$res = SlsQuery::queryFromAliSls(time());
var_dump($res);