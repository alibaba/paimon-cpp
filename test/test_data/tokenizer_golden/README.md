# Tokenizer 黄金样本

供 `paimon-tantivy-tokenizer-test` 比对 cppjieba vs jieba-rs 的分词输出。

## 文件

- `golden_synthetic.txt` — 手写边界 case（混合中英文、数字、标点、emoji、空白、超长词…）
- `golden_corpus.txt` — 公开语料短句摘录（通用知识、无版权敏感）

## 使用

测试代码（见 `src/paimon/global_index/tantivy/tantivy_tokenizer_test.cpp`）：
1. 逐行读取
2. 每行用 cppjieba `JiebaTokenizer::CutWithMode` + `Normalize` 得到 token 序列 A
3. 每行用 jieba-rs FFI `paimon_tantivy_tokenizer_tokenize` 得到 token 序列 B
4. 比对 A 和 B：如果完全相同则本行 pass；否则记入 diff 报告
5. 通过条件：diff 率 ≤ 1%（见 plan Stage 3 验收标准）

## 扩充

后续补充业务 query log 时，新增文件 `golden_business.txt` 放在同目录，测试代码自动扫描 `golden_*.txt`。
