# persistent_offer
offer持久化服务
[![codecov](https://codecov.io/gh/cloudadrd/persistent_offer/branch/master/graph/badge.svg?token=a1oJCu387u)](https://codecov.io/gh/cloudadrd/persistent_offer)


## 数据库
### 数据库信息
5.7

### 表结构

```sql
Create Table: CREATE TABLE `offer_persistent_201701171947` (
`docid` char(255) NOT NULL COMMENT '主键dnfid',
`insertDate` char(255) NOT NULL COMMENT '插入记录时的时间',
`adid` char(255) NOT NULL COMMENT 'offer id',
`app_pkg_name` char(255) DEFAULT NULL COMMENT 'app包名',
`channel` char(255) NOT NULL COMMENT '渠道',
`final_url` char(255) DEFAULT NULL COMMENT '最终的app商店链接',
`content` json DEFAULT NULL,
PRIMARY KEY (`docid`),
KEY `idx_adid` (`adid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8
```

```
+--------------+-----------+------+-----+---------+-------+
| Field        | Type      | Null | Key | Default | Extra |
+--------------+-----------+------+-----+---------+-------+
| docid        | char(255) | NO   | PRI | NULL    |       |
| insertDate   | char(255) | NO   |     | NULL    |       |
| adid         | char(255) | NO   | MUL | NULL    |       |
| app_pkg_name | char(255) | YES  |     | NULL    |       |
| channel      | char(255) | NO   |     | NULL    |       |
| final_url    | char(255) | YES  |     | NULL    |       |
| content      | json      | YES  |     | NULL    |       |
+--------------+-----------+------+-----+---------+-------+
```

### 接口

请求查询

|key|必须|说明|
|:-:|:-:|:---:|
|time|否|给定时间点精确到分钟如：201702131450|
|offerid|是，但是跟docid互斥|给定需要查询的offerid，可能重复如ym_123和iym_123|
|docid|是,跟offerid互斥|给定精准查询的docid如：ym_1234|

#### 示例：

http://54.255.167.180:10080/persistent/search?time=201701170312&offerid=1234

```
{
    "message":"offer is valid",
    "status":true,
    "snapshots":[
        {
            "record_time":"201702131450",
            "offer":{
                "active":true,
                "dnf":"( channel in { irs,any } and country in { US,DEBUG } and platform in { Android } and version in { 4.0.3,any } )",
                "docid":"irs_1071403",
                "name":"",
                "attr":{
                    "ad_expire_time":1000,
                    "adid":"1071403",
                    "app_category":[
                        "tool",
                        "BOOKS_AND_REFERENCE"
                    ],
                    "app_download":{
                        "app_pkg_name":"com.dailydevotionapp",
                        "download":"100000+",
                        "rate":4.6,
                        "review":0,
                        "size":"20M",
                        "title":"My Daily Devotion Bible App",
                        "tracking_link":"https://click.apprevolve.com/static/5ceaeaa7be82487aadde0d8c3cbebcd8/109591/1071403/0edbed61cea300b7?timestamp=1486968060&bundleId=com.dailydevotionapp&strategyId=4"
                    },
                    "channel":"irs",
                    "click_callback":"",
                    "clk_tks":[
                        
                    ],
                    "clk_url":"",
                    "countries":[
                        "US"
                    ],
                    "final_url":"https://play.google.com/store/apps/details?id=com.dailydevotionapp",
                    "landing_type":0,
                    "payout":0.66,
                    "platform":"Android",
                    "product_category":"googleplaydownload",
                    "third_party_clk_tks":[
                        
                    ],
                    "third_party_imp_tks":[
                        "https://imp.apprevolve.com/static/impression/5ceaeaa7be82487aadde0d8c3cbebcd8/109591/1071403/0edbed61cea300b7?timestamp=1486968060&bundleId=com.dailydevotionapp&strategyId=4"
                    ]
                }
            }
        }
    ]
}
```

http://54.255.167.180:10080/persistent/search?time=201701171234&docid=ym_12345

查询一个offer在数据库中的插入时间
http://54.255.167.180:10080/persistent/search?docid=2644357

```json
{
"message": "offer is valid",
"status": true,
"snapshots": [
{"record_time": "201702131449"},
{"record_time": "201702131456"},
{"record_time": "201702131500"}]
}
```
