# canal监听binlog后的输出结果格式


- DELETE操作
```
{
  "data": [
    {
      "id": "13",
      "rule_name": "r1",
      "rule_code": "code-new",
      "rule_type": "0",
      "rule_status": "0",
      "eventCountQuerySqls": "sql1",
      "eventSeqQuerySql": "sql2",
      "create_time": "2021-04-05 23:33:11",
      "modify_time": "2021-04-05 23:33:14"
    }
  ],
  "database": "realtimedw",
  "es": 1617637022000,
  "id": 5,
  "isDdl": false,
  "mysqlType": {
    "id": "int",
    "rule_name": "varchar(255)",
    "rule_code": "varchar(4096)",
    "rule_type": "varchar(255)",
    "rule_status": "varchar(255)",
    "eventCountQuerySqls": "varchar(4096)",
    "eventSeqQuerySql": "varchar(4096)",
    "create_time": "datetime",
    "modify_time": "datetime"
  },
  "old": null,
  "pkNames": [
    "id"
  ],
  "sql": "",
  "sqlType": {
    "id": 4,
    "rule_name": 12,
    "rule_code": 12,
    "rule_type": 12,
    "rule_status": 12,
    "eventCountQuerySqls": 12,
    "eventSeqQuerySql": 12,
    "create_time": 93,
    "modify_time": 93
  },
  "table": "canal_rule",
  "ts": 1617637022998,
  "type": "DELETE"
}
```


- INSERT操作

```
{
  "data": [
    {
      "id": "2",
      "rule_name": "rule2",
      "rule_code": "code2"
    }
  ],
  "database": "realtimedw",
  "es": 1617699081000,
  "id": 3,
  "isDdl": false,
  "mysqlType": {
    "id": "int",
    "rule_name": "varchar(255)",
    "rule_code": "varchar(255)"
  },
  "old": null,
  "pkNames": [
    "id"
  ],
  "sql": "",
  "sqlType": {
    "id": 4,
    "rule_name": 12,
    "rule_code": 12
  },
  "table": "test_drools",
  "ts": 1617699082093,
  "type": "INSERT"
}


```