# Tokenizer golden samples

Used by `paimon-tantivy-tokenizer-test` to compare cppjieba vs jieba-rs
tokenization output.

## Files

- `golden_synthetic.txt` — hand-written edge cases (mixed Chinese/English,
  digits, punctuation, emoji, whitespace, very long words, ...)
- `golden_corpus.txt` — short excerpts from public corpora (general knowledge,
  no copyright concerns)

## Usage

The test code (see `src/paimon/global_index/tantivy/tantivy_tokenizer_test.cpp`):
1. reads the files line by line
2. tokenizes each line with cppjieba `JiebaTokenizer::CutWithMode` + `Normalize`
   to get token sequence A
3. tokenizes each line with the jieba-rs FFI `paimon_tantivy_tokenizer_tokenize`
   to get token sequence B
4. compares A and B: the line passes if they are identical, otherwise it is
   recorded in the diff report
5. (historical) the original acceptance bar was a diff rate <= 1%; the test is
   now advisory only and logs diffs without failing

## Extending

To add business query logs later, drop a new `golden_business.txt` in this
directory; the test scans `golden_*.txt` automatically.
