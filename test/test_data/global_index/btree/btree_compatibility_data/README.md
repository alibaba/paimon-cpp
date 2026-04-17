# BTree Compatibility Test Data

## File Description

### Data Files
- `btree_test_int_<count>.csv` - Integer type test data (CSV format)
- `btree_test_int_<count>.bin` - Integer type test data (binary format)
- `btree_test_varchar_<count>.csv` - String type test data (CSV format)
- `btree_test_varchar_<count>.bin` - String type test data (binary format)

### Data Format
CSV file format:
```
row_id,key,is_null
0,123,false
1,NULL,true
2,456,false
```

### Test Scenarios
1. **Small-scale data**: 50, 100 records
2. **Medium-scale data**: 500, 1000 records
3. **Large-scale data**: 5000 records
4. **Edge cases**: null values, duplicate keys, boundary values

### Usage
This data can be used to verify the compatibility between the C++ BTree index implementation and the Java version.