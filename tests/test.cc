#include "leveldb/db.h"
#include "leveldb/write_batch.h"
#include "leveldb/comparator.h"
#include <string>
#include <iostream>
using namespace std;
using namespace leveldb;

// 编译命令示例：g++ -o test test.cc ../build/libleveldb.a -I../include -pthread -lsnappy
int main(){
    // 声明一个指向 DB 对象的指针，DB 对象代表一个数据库实例
    DB* db;

    // 打开数据库
    // 创建一个 Options 对象，用于配置数据库打开时的选项
    Options options;
    // 设置如果数据库不存在则创建它
    options.create_if_missing=true;
    // 定义数据库的名称（即存储数据的目录名）
    string name="testdb";
    // 调用 DB::Open 方法打开（或创建）数据库
    // options: 打开选项
    // name: 数据库名称
    // &db: 用于接收打开的数据库对象的指针
    Status status=DB::Open(options,name,&db);
    // 打印打开数据库的状态信息
    cout<<status.ToString()<<endl;

    // 写入数据 (Put操作)
    // 创建一个 WriteOptions 对象，用于配置写入操作的选项
    WriteOptions woptions;
    // 调用 db->Put 方法向数据库中写入一个键值对
    // woptions: 写入选项
    // "name": 键 (key)
    // "owenliang": 值 (value)
    status=db->Put(woptions,"name","owenliang");

    // SST 文件合并 (Compaction)
    // 调用 db->CompactRange 方法可以手动触发一个范围的 compaction
    // nullptr, nullptr 表示对整个数据库进行 compaction
    //db->CompactRange(nullptr, nullptr); 

    // 检查比较器类型 (示例代码，当前未启用)
    // if(options.comparator==BytewiseComparator()){
    //     cout<<"BytewiseComparator"<<endl;
    // }

    // 读取数据 (Get操作)
    // 创建一个 ReadOptions 对象，用于配置读取操作的选项
    ReadOptions roptions;
    // 声明一个字符串变量用于存储读取到的值
    string value;
    // 调用 db->Get 方法从数据库中读取指定键的值
    // roptions: 读取选项
    // "name": 要读取的键
    // &value: 用于接收读取到的值的指针
    status=db->Get(roptions,"name",&value);
    // 打印读取操作的状态信息和读取到的值
    cout<<status.ToString()<<","<<value<<endl;

    // 批量写入 (WriteBatch)
    // 创建一个 WriteBatch 对象，用于将多个写操作原子性地应用到数据库
    WriteBatch batch;
    //向 WriteBatch 中添加一个 Put 操作
    batch.Put("a","1");
    //向 WriteBatch 中添加另一个 Put 操作
    batch.Put("b","2");
    // 调用 db->Write 方法将 WriteBatch 中的所有操作应用到数据库
    status=db->Write(woptions,&batch);

    // 删除数据 (Delete操作)
    // 调用 db->Delete 方法从数据库中删除指定的键值对
    db->Delete(woptions,"name");

    // 迭代器 (Iterator)
    // 创建一个新的迭代器，用于遍历数据库中的键值对
    Iterator *iter=db->NewIterator(roptions);
    // 将迭代器定位到数据库中的第一个键值对
    iter->SeekToFirst();
    // 循环遍历所有有效的键值对
    while(iter->Valid()){
        // 获取当前迭代器指向的键
        Slice key=iter->key();
        // 获取当前迭代器指向的值
        Slice value=iter->value();
        // 打印键和值
        cout<<key.ToString()<<"="<<value.ToString()<<endl;
        // 将迭代器移动到下一个键值对
        iter->Next();
    }
    // 释放迭代器占用的资源
    delete iter;
    // 关闭并释放数据库对象占用的资源
    delete db;
    // 程序正常退出
    return 0;
}