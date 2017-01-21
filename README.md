# persistent_offer
offer持久化服务

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

http://54.255.167.180:10080/persistent/search?time=201701170312&offerid=1234

http://54.255.167.180:10080/persistent/search?time=201701171234&docid=ym_12345
