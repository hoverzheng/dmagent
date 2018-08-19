dmagent
=======

该软件是一款memcached轻客户端代理软件。是在开源软件magent上修改完成的。主要有以下特性：
* 可以作为客户端的memcached轻量级代理使用，也可以作为memcached的代理服务器。
* 可以安装在使用的客户端一端，客户端程序只需要在本地和该代理软件进行连接，发送命令，取回结果即可。
* memcached端集群对客户端完全透明。
* 实现了memcached的高可用性，高命中率，高伸缩性等需求。
* 使用c语言和事件驱动libevent实现。

主要实现以下功能：

* 重写了一致性hash算法，可以在配置文件中添加权重，key会按照权重比例进行分配。
* 若memcached集群中某台机器宕机，会自动踢掉该机器，这一切对客户端都是透明的。
* 被分配到宕机机器的key，会按权重比例分配到还在正常运行的那些机器上,而被分配到其他机器的key，不受影响。
* 当宕机的机器又能正常服务时，会自动把该机器添加到集群ip地址环中。
* 使用了tc_malloc作为内存管理模块，减少内存碎片，提高了软件性能。

## 使用架构
[dmagent]----->memcached1
       |
       -------->memcached2
       |
       -------->memcached3
       ...
       

## 使用说明
### 编译
$ make
$ ./dmagent

## 通过memcached的客户就可以连接使用
