## 项目开发流程
1. 提出需求
    * 提出打的需求方向，需要多个部门领导人参与讨论，项目可行性研究。
2. 需求分析
    * 和各部门确定（或者甲方确定）需求模块，最直接的是研究竞品。
3. 技术选型
    * 确定用哪些技术来进行开发，需要和多个研发组确定。
4. 预研工作
    * 可行性分析，需要搭建测试环境
5. 数据对接
    * 采集、清洗、抽取、存取
6. 数据分析
    * 将需求转换为指标
7. 存储
8. 展示

```敏捷开发```

## logstash
* logstash采用JRuby语言实现。
* logstash 官网
    * https://www.elastic.co/guide/en/logstash/2.4/index.html
* logstash 和 flume的区别：
    * 监控文件： tail -f  ; 监控目录： spooldir
    * flume ：  source  channel  sink
    * logstash：input   filter   output
    * flume 优点： 
        * 能实现高可用，保证数据的安全性
        * 能够通过事务控制的方式保证数据的一致性
        * 可用于多类型数据的采集
    * logstash 优点：
        * 相比较flume，小巧，易于安装【解压】
        * filter过滤器，能够进行数据的清洗，减少网络IO
        * 可以与es无缝对接，不必担心版本冲突问题。
        * 可以进行断点续传，flume要实现必须自己开发进行偏移量的管理。
        * 主要是用于日志数据的采集
### logstash 安装        
* https://www.elastic.co/guide/en/logstash/current/index.html
首先下载logstash，上传到服务器

* logstash是用JRuby语言开发的，所以要安装JDK

* 解压：
`tar -zxvf logstash-2.3.1.tar.gz -C /bigdata/`

* 启动：
    * bin/logstash -e 'input { stdin {} } output { stdout{} }'
    * bin/logstash -e 'input { stdin {} } output { stdout{codec => rubydebug} }'
    * bin/logstash -e 'input { stdin {} } output { elasticsearch {hosts => ["node01:9200"]} stdout{} }'
    * bin/logstash -e 'input { stdin {} } output { elasticsearch {hosts => ["node01:9200", "node02:9200"]} stdout{} }'
    * bin/logstash -e 'input { stdin {} } output { kafka { topic_id => "test1" bootstrap_servers => "192.168.88.81:9092,192.168.88.82:9092,192.168.88.83:9092"} stdout{codec => rubydebug} }'

---


以配置的形式:

`vi logstash-kafka.conf`

```
input {
  file {
    path => "/root/data/test.log"
    discover_interval => 5
    start_position => "beginning"
  }
}

output {
    kafka {
	  topic_id => "test1"
	  codec => plain {
        format => "%{message}"
		charset => "UTF-8"
      }
	  bootstrap_servers => "node01:9092,node02:9092,node03:9092"
    }
}
```

**启动logstash**

bin/logstash -f logstash-kafka.conf


`vi logstash-es.conf`


```
input {
	file {
		type => "gamelog"
		path => "/log/*/*.log"
		discover_interval => 10
		start_position => "beginning" 
	}
}
output {
    elasticsearch {
		index => "gamelog-%{+YYYY.MM.dd}"
        hosts => ["node01:9200", "node02:9200", "node03:9200"]
    }
}
```
**启动logstash**

```
bin/logstash -f logstash.conf


bin/logstash -e '
input { stdin {} }
filter {
  grok {
    match => { "message" => "%{IP:client} %{WORD:method} %{URIPATHPARAM:request} %{NUMBER:bytes} %{NUMBER:duration}" }
  }
} 
output { stdout{codec => rubydebug} 
}'
```