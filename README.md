一个轻量化的database组件，基于 sqlite ，本意是拓展闪卡数据库的灵活性。

希望能实现以下目标:

* [ ] 属性属于对象
* [ ] 允许跨表属性共享
* [ ] 支持多种不同类型的属性（富文本、数字、多选、单选、checkbox）
* [ ] 每个对象拥有多个属性
* [ ] 数据表对应多个对象，同一个对象可以对应不同的数据表
* [ ] 数据表是一个视图，他确定了包含的对象、列
* [ ] 数据表有对应的多个数据表视图，数据表视图确定了显示的列、排序、筛选


### 查询语法

基础的查询语法不完整的借鉴了mongo（完整的我也实现不了）：
```
{...
    "attributeId|connect":{
        "op":"value"
        }
...
}
```
不同点：
使用$eq表示相等
每个attributeId

### 排序语法
mongo的排序不足以满足多维表格的要求，所以我自己造了一套
```
[
    {
        "field":attributeClassID,
        ""
    }
]
```