#使用说明
#### **16302010033 孔庆灿**
-----
1、 首先把要导入的数据库文件和初始化数据表的SQL语句文本文件放到dataTransport.jar的同级目录下

2、 通过java -jar dataTransport.jar 命令运行

3、 根据提示选择导入Mysql的数据来源

    -1、从csv文件读入；
    -2、从SQlite数据库导入
    -3: 退出
    
4、 选择完成后输入要导入文件的名称（包含后缀）

*导入之后如果有主键冲突重复不能导入的情况会打印到控制台*
