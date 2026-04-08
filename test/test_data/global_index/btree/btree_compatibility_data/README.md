# BTree 兼容性测试数据

## 文件说明

### 数据文件
- `btree_test_int_<count>.csv` - 整数类型测试数据（CSV格式）
- `btree_test_int_<count>.bin` - 整数类型测试数据（二进制格式）
- `btree_test_varchar_<count>.csv` - 字符串类型测试数据（CSV格式）
- `btree_test_varchar_<count>.bin` - 字符串类型测试数据（二进制格式）

### 数据格式
CSV文件格式：
```
row_id,key,is_null
0,123,false
1,NULL,true
2,456,false
```

### 测试场景
1. **小规模数据**：50、100条记录
2. **中等规模数据**：500、1000条记录  
3. **大规模数据**：5000条记录
4. **边界条件**：空值、重复键、边界值

### 使用说明
这些数据可用于验证 C++ 版本的 BTree 索引实现与 Java 版本的兼容性。
