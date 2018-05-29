README


***
|Autor|ays|
|---|---|
|E-mail|bhza8987510@gmail.com
***

# 介绍
    该项目为空间分析课程的实现代码
    主要是爬取百度地图上的部分小区的位置与轮廓以及公交线路的站点信息
    
# 环境依赖:
    python2.7 (with ArcGIS)
    numpy
    百度地图ak授权码

# 文件简介
    Crawler.py 包含部分百度地图API的接口封装，便于爬取
    convert2shape.py 将数据转换成ArcGIS支持的SHP文件格式的接口封装
    temp.db 使用sqlite3构建的数据库，爬取的数据均存在此文件之中。具体表结构在Crawler.py之中
    buslines.txt 武汉市公交线路名称

