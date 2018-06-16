##MIT 6.832 实验课

课程信息可以查询 [这里](https://github.com/MIT-DB-Class/course-info-2017)

### Lab1

实现simpledb用来访问底层数据存储的主要模块

#### Exercise 1

实现Tuple和TupleDesc。Tuple对应到数据库概念里是一张表的一行, 每张表都有一个描述文件,
指明每个Tuple中每个Field的类型和值

#### Exercise 2

实现Catalog,。catalog中文是类目的意思。在这里一个数据库有唯一一个Catalog属性,每个Catalog包含里
数据库中所有的表。

#### Exercise 3
实现BufferPool和HeapFile。
* BufferPool会对数据库的Page进行缓存,一个数据库只有唯一一个BufferPool属性
* HeapFile对象实现了对磁盘文件的读写,每个HeapFile对象包含了若干个Page对象,以及每个Page对象的头文件。

### Exercise 4

实现 HeapPage

HeapPage中isSlotUsed() 我们根据slot的index, 在header字节数组中取到对应的byte,然后
在这个byte中找到对应的bit, 判断这个bit是否被设置。需要注意的是判断时从低位的字节算起

### Exercise 5
实现 HeapFile

一个HeapFile由多个HeapPage组成
* readPage()方法是通过九三Page的偏移量,读取HeapFile的底层文件来实现的
* iterator()方法需要自己实现一个HeapFileIterator, 通过分别遍历Page和Page中的Tuple来判断是有下一个元素'