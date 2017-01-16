# persistent_offer
offer持久化服务

## 数据库
### 数据库信息
5.7

### 表结构

```sql
create table offer_persistent_201701121105 (
docid char(255) not null comment '主键dnfid',
insertDate char(255) not null comment '插入记录时的时间',
active char(255) not null comment 'offer是否有效',
ad_expire_time int default 1000 comment 'ad的有效时间',
adid char(255) not null comment 'offer id',
app_category varchar(1000) comment 'app分类逗号分隔',
app_pkg_name char(255) comment 'app包名',
description text comment 'app的描述',
download char(255) comment 'app的下载量',
rate float comment 'app评分',
review int comment '评论数',
app_size char(255) comment 'app安装包大小',
title varchar(1000) comment 'app的title',
channel char(255) not null comment '渠道',
click_callback varchar(1000),
clk_tks varchar(1000),
countries varchar(1000) comment 'app投放的国家',
creatives json comment 'app的创意',
final_url varchar(1000) comment '最终的app商店链接',
icons json comment 'app的icon',
landing_type int,
payout float not null comment 'offer的单价',
platform char(255) not null comment 'app投放的平台',
product_category char(255) default 'googleplaydownload',
render_imgs json comment 'render 图片',
third_party_clk_tks text,
third_party_imp_tks text,
dnf varchar(1000) not null comment 'dnf的查询条件',
name char(255),
PRIMARY key(docid),
)ENGINE=InnoDB default CHARSET=utf8;
```

```
+---------------------+----------------+------+-----+--------------------+-------+
| Field               | Type           | Null | Key | Default            | Extra |
+---------------------+----------------+------+-----+--------------------+-------+
| docid               | char(255)      | NO   | PRI | NULL               |       |
| insertDate          | char(255)      | NO   |     | NULL               |       |
| active              | char(255)      | NO   |     | NULL               |       |
| ad_expire_time      | int(11)        | YES  |     | 1000               |       |
| adid                | char(255)      | NO   | MUL | NULL               |       |
| app_category        | varchar(1000)  | YES  |     | NULL               |       |
| app_pkg_name        | char(255)      | YES  |     | NULL               |       |
| description         | text           | YES  |     | NULL               |       |
| download            | char(255)      | YES  |     | NULL               |       |
| rate                | float          | YES  |     | NULL               |       |
| review              | int(11)        | YES  |     | NULL               |       |
| app_size            | char(255)      | YES  |     | NULL               |       |
| title               | varchar(1000)  | YES  |     | NULL               |       |
| channel             | char(255)      | NO   |     | NULL               |       |
| click_callback      | varchar(1000)  | YES  |     | NULL               |       |
| clk_tks             | varchar(1000)  | YES  |     | NULL               |       |
| countries           | varchar(1000)  | YES  |     | NULL               |       |
| creatives           | json           | YES  |     | NULL               |       |
| final_url           | varchar(1000)  | YES  |     | NULL               |       |
| icons               | json           | YES  |     | NULL               |       |
| landing_type        | int(11)        | YES  |     | NULL               |       |
| payout              | float          | NO   |     | NULL               |       |
| platform            | char(255)      | NO   | MUL | NULL               |       |
| product_category    | char(255)      | YES  |     | googleplaydownload |       |
| render_imgs         | json           | YES  |     | NULL               |       |
| third_party_clk_tks | text           | YES  |     | NULL               |       |
| third_party_imp_tks | text           | YES  |     | NULL               |       |
| dnf                 | varchar(1000)  | NO   |     | NULL               |       |
| name                | char(255)      | YES  |     | NULL               |       |
+---------------------+----------------+------+-----+--------------------+-------+
```
