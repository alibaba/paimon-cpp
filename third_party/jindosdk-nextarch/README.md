# JindoSDK NextArch C++

The `jindosdk-nextarch` C++ interface is derived from an internal repository maintained by Alibaba Jindo Team, which also develops and contributes to the open-source project [alibabacloud-jindodata](https://github.com/aliyun/alibabacloud-jindodata/).

## License

[Apache-2.0](http://www.apache.org/licenses/LICENSE-2.0)

Copyright 2024-present Alibaba Cloud.

## Usage

### requirement

g++: support c++17

cmake: > 3.17.0

jindo_c_sdk: You can download from [alibabacloud-jindodata](https://github.com/aliyun/alibabacloud-jindodata/)

### build

```bash
# use your compiler
export CC=${CC:-"/usr/local/bin/gcc"}
export CXX=${CXX:-"/usr/local/bin/g++"}
mkdir -p build
cd build
cmake .. -DJINDOSDK_ROOT=<jindo_c_sdk_root>
make -j3
```
