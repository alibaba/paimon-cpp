/*
 * Copyright 2024-present Alibaba Cloud.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "JdoFileSystem.hpp"

#include <cstdlib>
#include <iostream>
#include <memory>
#include <vector>
#include <cstdlib>
#include <pwd.h>
#include <unistd.h>

#include "JdoStatus.hpp"
#include "JdoFileInfo.hpp"
#include "JdoListResult.hpp"
#include "JdoContentSummary.hpp"

#include "jdo_common.h"
#include "jdo_api.h"
#include "jdo_defines.h"
#include "jdo_error.h"
#include "jdo_options.h"
#include "jdo_file_status.h"
#include "jdo_list_dir_result.h"
#include "jdo_content_summary.h"
#include "jdo_file_buffers.h"
#include "jdo_longs.h"

#define START_CALL(fs) \
    JdoHandleCtx_t ctx = jdo_createHandleCtx1(fs);

#define START_CALL2(fs, stream) \
    JdoHandleCtx_t ctx = jdo_createHandleCtx2(fs, stream);

#define END_CALL() \
    auto errorCode = jdo_getHandleCtxErrorCode(ctx); \
    std::string errorMsg;                            \
    const char* msg = jdo_getHandleCtxErrorMsg(ctx); \
    if (msg != nullptr) {                            \
        errorMsg.assign(msg);                        \
    }                                                \
    jdo_freeHandleCtx(ctx);

static void covertToFileInfo(JdoFileStatus_t fileStatus,
                             JdoFileInfo* info) {
    if (!fileStatus) return;
    auto path = jdo_getFileStatusPath(fileStatus);
    if (path) {
        info->path = path;
    }
    auto user = jdo_getFileStatusUser(fileStatus);
    if (user) {
        info->user = user;
    }
    auto group = jdo_getFileStatusGroup(fileStatus);
    if (group) {
        info->group = group;
    }
    info->type = jdo_getFileStatusType(fileStatus);
    info->perm = jdo_getFileStatusPerm(fileStatus);
    info->length = jdo_getFileStatusSize(fileStatus);
    info->mtime = jdo_getFileStatusMtime(fileStatus);
    info->atime = jdo_getFileStatusAtime(fileStatus);
    if (info->isDir() && info->path.back() != '/') {
        info->path.push_back('/');
    }
}

class JindoOperation : public JdoTask {
public:
    JindoOperation(JdoStore_t store, JdoOperationCall_t call)
            : JdoTask(), _store(store), _stream(nullptr), _call(call) {}

    JindoOperation(JdoStore_t store, JdoIOContext_t stream, JdoOperationCall_t call)
            : JdoTask(), _store(store), _stream(stream), _call(call) {}

    ~JindoOperation() override {
        if (_call) {
            jdo_freeOperationCall(_call);
            _call = nullptr;
        }
    }

    JdoStatus perform() override {
        START_CALL2(_store, _stream)
        jdo_perform(ctx, _call);
        END_CALL();
        if (errorCode != 0) {
            return JdoStatus::InternalError(errorCode, "failed to perform, errmsg: ", errorMsg);
        }
        return JdoStatus::OK();
    }

    JdoStatus wait() override {
        START_CALL2(_store, _stream)
        jdo_wait(ctx, _call);
        END_CALL();
        if (errorCode != 0) {
            return JdoStatus::InternalError(errorCode, "failed to wait, errmsg: ", errorMsg);
        }
        return JdoStatus::OK();
    }

    JdoStatus cancel() override {
        START_CALL2(_store, _stream)
        jdo_cancel(ctx, _call);
        END_CALL();
        if (errorCode != 0) {
            return JdoStatus::InternalError(errorCode, "failed to cancel, errmsg: ", errorMsg);
        }
        return JdoStatus::OK();
    }

private:
    JdoStore_t _store = nullptr;
    JdoIOContext_t _stream = nullptr;
    JdoOperationCall_t _call = nullptr;
};

struct ReadArgs {
    JdoStore_t store = nullptr;
    JdoIOContext_t reader = nullptr;
    std::string name;
    size_t n = 0;
    std::string_view *result;
    char *scratch;
    std::function<void(JdoStatus)> callback;
};

static void readCallback(JdoHandleCtx_t ctx, int64_t num_read, void *userdata) {
    ReadArgs *args = (ReadArgs *) userdata;
    auto store = args->store;
    auto reader = args->reader;
    auto name = args->name;
    auto n = args->n;
    auto result = args->result;
    auto scratch = args->scratch;
    auto total_read = num_read;
    std::function<void(JdoStatus)> callback = args->callback;
    delete args;

    END_CALL()
    if (errorCode != 0) {
        auto status = JdoStatus::InternalError(errorCode, "failed to read ", name,
                                          " size ", n - total_read, " readed ", num_read,
                                          ", errmsg: ", errorMsg);
        if (callback) {
            callback(status);
        }
        return;
    }

    JdoStatus status;
    if (total_read >= 0) {
        while (n - total_read > 0) {
            START_CALL2(store, reader)
            num_read = jdo_read(ctx, scratch + total_read, n - total_read, nullptr);
            END_CALL()
            if (errorCode != 0) {
                status = JdoStatus::InternalError(errorCode, "failed to read ", name,
                                                  " size ", n - total_read, " readed ", num_read,
                                                  ", errmsg: ", errorMsg);
                if (callback) {
                    callback(status);
                }
                return;
            }
            if (num_read < 0) {
                // isEOF
                break;
            }
            total_read += num_read;
        }
    }

    if ((total_read < 0 || total_read < n) && num_read != -1) {
        // if there was an error before satisfying the current read, this logic declares it an error
        // and does not try to return any of the bytes read. Don't think it matters, so the code
        // is just being conservative.
        status = JdoStatus::IOError("Failed to read, ", name,
                                    ", expect read ", n, " bytes"
                                    ", but ", total_read, " bytes readed"
                                    ", last read ", num_read, " bytes");
        if (callback) {
            callback(status);
        }
        return;
    }

    if (total_read < 0 || total_read < n) {
        // This is not an error per se. The RandomAccessFile interface expects
        // that Read returns OutOfRange if fewer bytes were read than requested.
        status = JdoStatus::IOError("EOF reached, ", result->size(),
                                    " bytes were read out of ", n,
                                    " bytes requested.");
        if (callback) {
            callback(status);
        }
        return;
    }

    if (callback) {
        callback(JdoStatus::OK());
    }
}

struct PreadArgs {
    JdoStore_t store = nullptr;
    JdoIOContext_t reader = nullptr;
    std::string name;
    int64_t offset = 0;
    size_t n = 0;
    std::string_view *result;
    char *scratch;
    std::function<void(JdoStatus)> callback;
};

static void preadCallback(JdoHandleCtx_t ctx, int64_t num_read, void *userdata) {
    PreadArgs *args = (PreadArgs *) userdata;
    auto store = args->store;
    auto reader = args->reader;
    auto name = args->name;
    auto offset = args->offset;
    auto n = args->n;
    auto result = args->result;
    auto scratch = args->scratch;
    auto total_read = num_read;
    std::function<void(JdoStatus)> callback = args->callback;
    delete args;

    END_CALL()
    if (errorCode != 0) {
        auto status = JdoStatus::InternalError(errorCode, "failed to read ", name, " offset ", offset + total_read,
                                    " size ", n - total_read, " readed ", num_read,
                                    ", errmsg: ", errorMsg);
        if (callback) {
            callback(status);
        }
        return;
    }

    JdoStatus status;
    if (total_read >= 0) {
        while (n - total_read > 0) {
            START_CALL2(store, reader)
            num_read = jdo_pread(ctx, scratch + total_read, n - total_read, offset + total_read, nullptr);
            END_CALL()
            if (errorCode != 0) {
                status = JdoStatus::InternalError(errorCode, "failed to read ", name, " offset ", offset + total_read,
                                            " size ", n - total_read, " readed ", num_read,
                                            ", errmsg: ", errorMsg);
                if (callback) {
                    callback(status);
                }
                return;
            }
            if (num_read < 0) {
                // isEOF
                break;
            }
            total_read += num_read;
        }


        // Set the results.
        *result = std::string_view(scratch, total_read);
    }

    if ((total_read < 0 || total_read < n) && num_read != -1) {
        // if there was an error before satisfying the current read, this logic declares it an error
        // and does not try to return any of the bytes read. Don't think it matters, so the code
        // is just being conservative.
        status = JdoStatus::IOError("Failed to read, ", name,
                                    ", expect read ", n, " bytes"
                                    ", but ", total_read, " bytes readed"
                                    ", last read ", num_read, " bytes");
        if (callback) {
            callback(status);
        }
        return;
    }

    if (total_read < 0 || total_read < n) {
        // This is not an error per se. The RandomAccessFile interface expects
        // that Read returns OutOfRange if fewer bytes were read than requested.
        status = JdoStatus::IOError("EOF reached, ", result->size(),
                                    " bytes were read out of ", n,
                                    " bytes requested.");
        if (callback) {
            callback(status);
        }
        return;
    }

    if (callback) {
        callback(JdoStatus::OK());
    }
}

struct PreadvArgs {
    JdoStore_t store = nullptr;
    JdoIOContext_t reader = nullptr;
    std::string name;
    JdoLongs_t lengths;
    JdoLongs_t offsets;
    JdoFileBuffers_t buffers;
    std::function<void(JdoStatus)> callback;
};

static void preadvCallback(JdoHandleCtx_t ctx, int64_t ret, void *userdata) {
    PreadvArgs *args = (PreadvArgs *) userdata;
    auto store = args->store;
    auto reader = args->reader;
    auto name = args->name;
    auto offsets = args->offsets;
    auto lengths = args->lengths;
    auto buffers = args->buffers;
    auto rangeCnt = jdo_getFileBuffersSize(buffers);
    std::function<void(JdoStatus)> callback = args->callback;
    delete args;

    END_CALL()
    jdo_freeFileBuffers(buffers);
    jdo_freeLongs(lengths);
    jdo_freeLongs(offsets);

    JdoStatus status;
    if (errorCode != 0) {
        status = JdoStatus::InternalError(errorCode, "failed to preadv ", name,
                                  " ranges ", std::to_string(rangeCnt),
                                  ", errmsg: ", errorMsg);
    }

    if (callback) {
        callback(status);
    }
}

struct Int64Args {
    JdoStore_t store = nullptr;
    JdoIOContext_t reader = nullptr;
    JdoIOContext_t writer = nullptr;
    std::function<void(JdoStatus, int64_t)> callback;
};

static void tellCallback(JdoHandleCtx_t ctx, int64_t result, void *userdata) {
    Int64Args* args = (Int64Args*) userdata;
    auto store = args->store;
    auto reader = args->reader;
    std::function<void(JdoStatus, int64_t)> callback = args->callback;
    delete args;

    END_CALL()

    JdoStatus status;
    if (errorCode != 0) {
        status = JdoStatus::InternalError(errorCode, "file tell failed: ", errorMsg);
    }

    if (callback) {
        callback(status, result);
    }
}

static void seekCallback(JdoHandleCtx_t ctx, int64_t result, void *userdata) {
    Int64Args* args = (Int64Args*) userdata;
    auto store = args->store;
    auto reader = args->reader;
    std::function<void(JdoStatus, int64_t)> callback = args->callback;
    delete args;

    END_CALL()

    JdoStatus status;
    if (errorCode != 0) {
        status = JdoStatus::InternalError(errorCode, "file seek failed: ", errorMsg);
    }

    if (callback) {
        callback(status, result);
    }
}

static void getFileLengthCallback(JdoHandleCtx_t ctx, int64_t result, void *userdata) {
    Int64Args* args = (Int64Args*) userdata;
    auto store = args->store;
    auto reader = args->reader;
    std::function<void(JdoStatus, int64_t)> callback = args->callback;
    delete args;

    END_CALL()

    JdoStatus status;
    if (errorCode != 0) {
        status = JdoStatus::InternalError(errorCode, "get file length failed: ", errorMsg);
    }

    if (callback) {
        callback(status, result);
    }
}

struct CloseReaderArgs {
    JdoStore_t store = nullptr;
    JdoIOContext_t reader = nullptr;
    std::string name;
    std::atomic<bool>* isClosed = nullptr;
    std::function<void(JdoStatus)> callback;
};

static void closeReaderCallback(JdoHandleCtx_t ctx, bool result, void* userdata) {
    CloseReaderArgs* args = (CloseReaderArgs*) userdata;
    auto store = args->store;
    auto reader = args->reader;
    auto name = args->name;
    std::atomic<bool>* isClosed = args->isClosed;
    std::function<void(JdoStatus)> callback = args->callback;
    delete args;

    END_CALL()

    *isClosed = true;

    JdoStatus status;
    if (errorCode != 0) {
        status = JdoStatus::InternalError(errorCode, "close file failed: ", errorMsg);
    }

    if (callback) {
        callback(status);
    }
}

class JindoReader : public JdoReader {
public:
    JindoReader(const std::string &fname,
                JdoStore_t store,
                JdoIOContext_t reader)
            : _name(fname),
              _store(store),
              _reader(reader),
              _is_closed(false) {
    }

    ~JindoReader() override {
        if (_reader) {
            if (!_is_closed) {
                JdoHandleCtx_t ctx = jdo_createHandleCtx2(_store, _reader);
                jdo_close(ctx, nullptr);
                jdo_freeHandleCtx(ctx);
            }
            jdo_freeIOContext(_reader);
            _reader = nullptr;
        }
    }

    bool closed() const override {
        return _is_closed;
    }

    JdoStatus name(std::string_view *result) const override {
        *result = _name;
        return JdoStatus::OK();
    }

    JdoStatus read(size_t n, std::string_view *result, char *scratch) override {
        std::lock_guard<std::mutex> lock(_lock);
        size_t total_read = 0;
        int64_t num_read = 0;
        while (n - total_read > 0) {
            START_CALL2(_store, _reader)
            num_read = jdo_read(ctx, scratch + total_read, n - total_read, nullptr);
            END_CALL()
            if (errorCode != 0) {
                return JdoStatus::InternalError(errorCode, "failed to read ", _name, " size ", n - total_read, " readed ", num_read,
                                          ", errmsg: ", errorMsg);
            }
            if (num_read < 0) {
                // isEOF
                break;
            }
            total_read += num_read;
        }
        // Set the results.
        *result = std::string_view(scratch, total_read);

        if (total_read < n && num_read != -1) {
            // if there was an error before satisfying the current read, this logic declares it an error
            // and does not try to return any of the bytes read. Don't think it matters, so the code
            // is just being conservative.
            return JdoStatus::IOError("Failed to read, ", _name,
                                      ", expect read ", n, " bytes"
                                      ", but ", total_read, " bytes readed"
                                       ", last read ", num_read, " bytes");
        }

        if (total_read < n) {
            // This is not an error per se. The RandomAccessFile interface expects
            // that Read returns OutOfRange if fewer bytes were read than requested.
            return JdoStatus::IOError("EOF reached, ", result->size(),
                                      " bytes were read out of ", n,
                                      " bytes requested.");
        }

        return JdoStatus::OK();
    }

    JdoTaskPtr readAsync(size_t n, std::string_view *result, char *scratch, std::function<void(JdoStatus)> callback) override {
        size_t total_read = 0;
        int64_t num_read = 0;
        ReadArgs *args = new ReadArgs();
        args->store = _store;
        args->reader = _reader;
        args->name = _name;
        args->n = n;
        args->result = result;
        args->scratch = scratch;
        args->callback = callback;
        START_CALL2(_store, _reader)
        auto options = jdo_createOptions();
        jdo_setInt64Callback(options, readCallback);
        jdo_setCallbackContext(options, args);
        auto operationCall = jdo_readAsync(ctx, scratch + total_read, n - total_read, options);
        END_CALL()
        jdo_freeOptions(options);

        return std::make_shared<JindoOperation>(_store, _reader, operationCall);
    }

    JdoStatus pread(int64_t offset, size_t n, std::string_view *result, char *scratch) override {
        size_t total_read = 0;
        int64_t num_read = 0;
        while (n - total_read > 0) {
            START_CALL2(_store, _reader)
            num_read = jdo_pread(ctx, scratch + total_read, n - total_read, offset + total_read, nullptr);
            END_CALL()
            if (errorCode != 0) {
                return JdoStatus::InternalError(errorCode, "failed to read ", _name, " offset ", offset + total_read,
                                          " size ", n - total_read, " readed ", num_read,
                                          ", errmsg: ", errorMsg);
            }
            if (num_read < 0) {
                // isEOF
                break;
            }
            total_read += num_read;
        }
        // Set the results.
        *result = std::string_view(scratch, total_read);

        if (total_read < n && num_read != -1) {
            // if there was an error before satisfying the current read, this logic declares it an error
            // and does not try to return any of the bytes read. Don't think it matters, so the code
            // is just being conservative.
            return JdoStatus::IOError("Failed to read, ", _name,
                                      ", expect read ", n, " bytes"
                                      ", but ", total_read, " bytes readed"
                                       ", last read ", num_read, " bytes");
        }

        if (total_read < n) {
            // This is not an error per se. The RandomAccessFile interface expects
            // that Read returns OutOfRange if fewer bytes were read than requested.
            return JdoStatus::IOError("EOF reached, ", result->size(),
                                      " bytes were read out of ", n,
                                      " bytes requested.");
        }

        return JdoStatus::OK();
    }

    JdoTaskPtr preadAsync(int64_t offset, size_t n, std::string_view *result, char *scratch,
                              std::function<void(JdoStatus)> callback) override {
        size_t total_read = 0;
        int64_t num_read = 0;
        PreadArgs *args = new PreadArgs();
        args->store = _store;
        args->reader = _reader;
        args->name = _name;
        args->offset = offset;
        args->n = n;
        args->result = result;
        args->scratch = scratch;
        args->callback = callback;
        START_CALL2(_store, _reader)
        auto options = jdo_createOptions();
        jdo_setInt64Callback(options, preadCallback);
        jdo_setCallbackContext(options, args);
        auto operationCall = jdo_preadAsync(ctx, scratch + total_read, n - total_read, offset + total_read, options);
        END_CALL()
        jdo_freeOptions(options);

        return std::make_shared<JindoOperation>(_store, _reader, operationCall);
    }

    JdoStatus preadv(std::vector<int64_t>& offsets, std::vector<size_t>& lengths, std::vector<char*>& scratches) override {
        START_CALL2(_store, _reader)
        JdoLongs_t offs = jdo_createLongs();
        for (auto& offset : offsets) {
            jdo_appendLong(offs, offset);
        }
        JdoLongs_t lens = jdo_createLongs();
        for (auto& len: lengths) {
            jdo_appendLong(lens, len);
        }
        JdoFileBuffers_t fileBuffers = jdo_createFileBuffers();
        for (auto& scratch : scratches) {
            jdo_appendFileBuffer(fileBuffers, scratch);
        }
        jdo_preadv(ctx, fileBuffers, lens, offs, nullptr);
        END_CALL()
        jdo_freeFileBuffers(fileBuffers);
        jdo_freeLongs(lens);
        jdo_freeLongs(offs);
        if (errorCode != 0) {
            return JdoStatus::InternalError(errorCode, "failed to preadv ", _name,
                                      " ranges ", scratches.size(),
                                      ", errmsg: ", errorMsg);
        }
        return JdoStatus::OK();
    }

    JdoTaskPtr preadvAsync(std::vector<int64_t>& offsets, std::vector<size_t>& lengths, std::vector<char*>& scratches,
                          std::function<void(JdoStatus)> callback) override {
        JdoLongs_t offs = jdo_createLongs();
        for (auto& offset : offsets) {
            jdo_appendLong(offs, offset);
        }
        JdoLongs_t lens = jdo_createLongs();
        for (auto& len: lengths) {
            jdo_appendLong(lens, len);
        }
        JdoFileBuffers_t fileBuffers = jdo_createFileBuffers();
        for (auto& scratch : scratches) {
            jdo_appendFileBuffer(fileBuffers, scratch);
        }
        PreadvArgs *args = new PreadvArgs();
        args->store = _store;
        args->reader = _reader;
        args->name = _name;
        args->offsets = offs;
        args->lengths = lens;
        args->buffers = fileBuffers;
        args->callback = callback;
        START_CALL2(_store, _reader)
        auto options = jdo_createOptions();
        jdo_setInt64Callback(options, preadvCallback);
        jdo_setCallbackContext(options, args);
        auto operationCall = jdo_preadvAsync(ctx, fileBuffers, lens, offs, options);
        END_CALL()
        jdo_freeOptions(options);

        return std::make_shared<JindoOperation>(_store, _reader, operationCall);
    }

    JdoStatus tell(int64_t& offset) override {
        START_CALL2(_store, _reader)
        offset = jdo_tell(ctx, nullptr);
        END_CALL()

        if (errorCode != 0) {
            return JdoStatus::InternalError(errorCode, "tell file failed: ", errorMsg);
        }

        return JdoStatus::OK();
    }

    JdoTaskPtr tellAsync(std::function<void(JdoStatus, int64_t)> callback) override {
        Int64Args *args = new Int64Args();
        args->store = _store;
        args->reader = _reader;
        args->callback = callback;
        START_CALL2(_store, _reader)
        auto options = jdo_createOptions();
        jdo_setInt64Callback(options, tellCallback);
        jdo_setCallbackContext(options, args);
        auto operationCall = jdo_tellAsync(ctx,options);
        END_CALL()
        jdo_freeOptions(options);

        return std::make_shared<JindoOperation>(_store, _reader, operationCall);
    }


    JdoStatus seek(int64_t offset) override {
        START_CALL2(_store, _reader)
        jdo_seek(ctx, offset, nullptr);
        END_CALL()

        if (errorCode != 0) {
            return JdoStatus::InternalError(errorCode, "seek file failed: ", errorMsg);
        }

        return JdoStatus::OK();
    }

    JdoTaskPtr seekAsync(int64_t offset, std::function<void(JdoStatus, int64_t)> callback) override {
        Int64Args *args = new Int64Args();
        args->store = _store;
        args->reader = _reader;
        args->callback = callback;
        START_CALL2(_store, _reader)
        auto options = jdo_createOptions();
        jdo_setInt64Callback(options, seekCallback);
        jdo_setCallbackContext(options, args);
        auto operationCall = jdo_seekAsync(ctx, offset, options);
        END_CALL()
        jdo_freeOptions(options);

        return std::make_shared<JindoOperation>(_store, _reader, operationCall);
    }

    JdoStatus getFileLength(int64_t& length) override {
        START_CALL2(_store, _reader)
        length = jdo_getFileLength(ctx, nullptr);
        END_CALL()

        if (errorCode != 0) {
            return JdoStatus::InternalError(errorCode, "tell file failed: ", errorMsg);
        }

        return JdoStatus::OK();
    }

    JdoTaskPtr getFileLengthAsync(std::function<void(JdoStatus, int64_t)> callback) override {
        Int64Args *args = new Int64Args();
        args->store = _store;
        args->reader = _reader;
        args->callback = callback;
        START_CALL2(_store, _reader)
        auto options = jdo_createOptions();
        jdo_setInt64Callback(options, getFileLengthCallback);
        jdo_setCallbackContext(options, args);
        auto operationCall = jdo_getFileLengthAsync(ctx,options);
        END_CALL()
        jdo_freeOptions(options);

        return std::make_shared<JindoOperation>(_store, _reader, operationCall);
    }

    JdoStatus close() override {
        START_CALL2(_store, _reader)
        jdo_close(ctx, nullptr);
        END_CALL()

        if (errorCode != 0) {
            return JdoStatus::InternalError(errorCode, "close file failed: ", errorMsg);
        }

        _is_closed = true;
        return JdoStatus::OK();
    }

    JdoTaskPtr closeAsync(std::function<void(JdoStatus)> callback) override {
        CloseReaderArgs* args = new CloseReaderArgs();
        args->store = _store;
        args->reader = _reader;
        args->name = _name;
        args->isClosed = &_is_closed;
        args->callback = callback;

        START_CALL2(_store, _reader)
        auto options = jdo_createOptions();
        jdo_setBoolCallback(options, closeReaderCallback);
        jdo_setCallbackContext(options, args);
        auto operationCall = jdo_closeAsync(ctx, options);
        END_CALL()
        jdo_freeOptions(options);

        return std::make_shared<JindoOperation>(_store, _reader, operationCall);
    }

private:
    std::mutex  _lock;
    std::string _name;
    JdoStore_t  _store;
    JdoIOContext_t _reader = nullptr;
    std::atomic<bool> _is_closed = false;
};

struct WriteArgs {
    JdoStore_t store = nullptr;
    JdoIOContext_t writer = nullptr;
    std::string name;
    std::function<void(JdoStatus)> callback;
};

static void writeCallback(JdoHandleCtx_t ctx, int64_t result, void* userdata) {
    WriteArgs* args = (WriteArgs*) userdata;
    auto store = args->store;
    auto writer = args->writer;
    auto name = args->name;
    std::function<void(JdoStatus)> callback = args->callback;
    delete args;

    END_CALL()

    JdoStatus status;
    if (errorCode != 0) {
        status = JdoStatus::InternalError(errorCode, "write file failed: ", errorMsg);
    }

    if (callback) {
        callback(status);
    }
}

struct FlushArgs {
    JdoStore_t store = nullptr;
    JdoIOContext_t writer = nullptr;
    std::string name;
    std::function<void(JdoStatus)> callback;
};

static void flushCallback(JdoHandleCtx_t ctx, bool result, void* userdata) {
    FlushArgs* args = (FlushArgs*) userdata;
    auto store = args->store;
    auto writer = args->writer;
    auto name = args->name;
    std::function<void(JdoStatus)> callback = args->callback;
    delete args;

    END_CALL()

    JdoStatus status;
    if (errorCode != 0) {
        status = JdoStatus::InternalError(errorCode, "flush file failed: ", errorMsg);
    }

    if (callback) {
        callback(status);
    }
}

struct CloseWriterArgs {
    JdoStore_t store = nullptr;
    JdoIOContext_t writer = nullptr;
    std::string name;
    std::atomic<bool>* isClosed = nullptr;
    std::function<void(JdoStatus)> callback;
};

static void closeWriterCallback(JdoHandleCtx_t ctx, bool result, void* userdata) {
    CloseWriterArgs* args = (CloseWriterArgs*) userdata;
    auto store = args->store;
    auto writer = args->writer;
    auto name = args->name;
    std::atomic<bool>* isClosed = args->isClosed;
    std::function<void(JdoStatus)> callback = args->callback;
    delete args;

    END_CALL()

    JdoStatus status;
    if (errorCode != 0) {
        status = JdoStatus::InternalError(errorCode, "close file failed: ", errorMsg);
    }

    *isClosed = true;

    if (callback) {
        callback(status);
    }
}

class JindoWriter : public JdoWriter {
public:
    JindoWriter(const std::string& fname,
                JdoStore_t store,
                JdoIOContext_t writer)
            : _name(fname),
              _store(store),
              _writer(writer),
              _is_closed(false) {
    }

    ~JindoWriter() override {
        if (_writer) {
            if (!_is_closed) {
                JdoHandleCtx_t ctx = jdo_createHandleCtx2(_store, _writer);
                jdo_close(ctx, nullptr);
                jdo_freeHandleCtx(ctx);
            }
            jdo_freeIOContext(_writer);
            _writer = nullptr;
        }
    }

    bool closed() const override {
        return _is_closed;
    }

    JdoStatus name(std::string_view* result) const override {
        *result = _name;
        return JdoStatus::OK();
    }

    JdoStatus write(std::string_view data) override {
        JDO_RETURN_IF_ERROR(_CheckClosed());

        START_CALL2(_store, _writer)
        jdo_write(ctx, data.data(), data.size(), nullptr);
        END_CALL()

        if (errorCode != 0) {
            return JdoStatus::InternalError(errorCode, "write file failed: ", errorMsg);
        }

        return JdoStatus::OK();
    }

    JdoTaskPtr writeAsync(std::string_view data, std::function<void(JdoStatus)> callback) override {
        WriteArgs* args = new WriteArgs();
        args->store = _store;
        args->writer = _writer;
        args->name = _name;
        args->callback = callback;

        START_CALL2(_store, _writer)
        auto options = jdo_createOptions();
        jdo_setInt64Callback(options, writeCallback);
        jdo_setCallbackContext(options, args);
        auto operationCall = jdo_writeAsync(ctx, data.data(), data.size(), options);
        END_CALL()
        jdo_freeOptions(options);

        return std::make_shared<JindoOperation>(_store, _writer, operationCall);
    }

    JdoStatus flush() override {
        JDO_RETURN_IF_ERROR(_CheckClosed());

        START_CALL2(_store, _writer)
        jdo_flush(ctx, nullptr);
        END_CALL()

        if (errorCode != 0) {
            return JdoStatus::InternalError(errorCode, "flush file failed: ", errorMsg);
        }

        return JdoStatus::OK();
    }

    JdoStatus tell(int64_t& offset) override {
        JDO_RETURN_IF_ERROR(_CheckClosed());

        START_CALL2(_store, _writer)
        offset = jdo_tell(ctx, nullptr);
        END_CALL()

        if (errorCode != 0) {
            return JdoStatus::InternalError(errorCode, "tell file failed: ", errorMsg);
        }

        return JdoStatus::OK();
    }


    JdoTaskPtr tellAsync(std::function<void(JdoStatus, int64_t)> callback) override {
        Int64Args *args = new Int64Args();
        args->store = _store;
        args->writer = _writer;
        args->callback = callback;
        START_CALL2(_store, _writer)
        auto options = jdo_createOptions();
        jdo_setInt64Callback(options, tellCallback);
        jdo_setCallbackContext(options, args);
        auto operationCall = jdo_tellAsync(ctx,options);
        END_CALL()
        jdo_freeOptions(options);

        return std::make_shared<JindoOperation>(_store, _writer, operationCall);
    }

    JdoTaskPtr flushAsync(std::function<void(JdoStatus)> callback) override {
        FlushArgs* args = new FlushArgs();
        args->store = _store;
        args->writer = _writer;
        args->name = _name;
        args->callback = callback;

        START_CALL2(_store, _writer)
        auto options = jdo_createOptions();
        jdo_setBoolCallback(options, flushCallback);
        jdo_setCallbackContext(options, args);
        auto operationCall = jdo_flushAsync(ctx, options);
        END_CALL()
        jdo_freeOptions(options);

        return std::make_shared<JindoOperation>(_store, _writer, operationCall);
    }

    JdoStatus close() override {
        JDO_RETURN_IF_ERROR(_CheckClosed());

        START_CALL2(_store, _writer)
        jdo_close(ctx, nullptr);
        END_CALL()

        if (errorCode != 0) {
            return JdoStatus::InternalError(errorCode, "close file failed: ", errorMsg);
        }
        _is_closed = true;
        return JdoStatus::OK();
    }

    JdoTaskPtr closeAsync(std::function<void(JdoStatus)> callback) override {
        CloseWriterArgs* args = new CloseWriterArgs();
        args->store = _store;
        args->writer = _writer;
        args->name = _name;
        args->isClosed = &_is_closed;
        args->callback = callback;

        START_CALL2(_store, _writer)
        auto options = jdo_createOptions();
        jdo_setBoolCallback(options, closeWriterCallback);
        jdo_setCallbackContext(options, args);
        auto operationCall = jdo_closeAsync(ctx, options);
        END_CALL()
        jdo_freeOptions(options);

        return std::make_shared<JindoOperation>(_store, _writer, operationCall);
    }

private:
    JdoStatus _CheckClosed() {
        if (_is_closed) {
            return JdoStatus::IOError("Already closed.");
        }

        return JdoStatus::OK();
    }

    std::string _name;
    JdoStore_t  _store;
    JdoIOContext_t _writer = nullptr;

    std::atomic<bool> _is_closed = false;
};

JdoFileSystem::JdoFileSystem() {
}

JdoFileSystem::~JdoFileSystem() {
    if (_store != nullptr) {
        jdo_freeStore(_store);
        _store = nullptr;
    }
    if (_options != nullptr) {
        jdo_freeOptions(_options);
        _options = nullptr;
    }
}

JdoStatus JdoFileSystem::init(const std::string &uri, const std::string &user, const std::shared_ptr<JdoConfig>& config) {
    if (_inited) {
        return JdoStatus::InitError("already inited");
    }

    _options = jdo_createOptions();
    auto configMap = config->getAll();
    for (auto& kv : configMap) {
        jdo_setOption(_options, kv.first.c_str(), kv.second.c_str());
    }
    if (getenv("OSS_ENDPOINT")) {
        jdo_setOption(_options, "fs.oss.endpoint", getenv("OSS_ENDPOINT"));
    }
    if (getenv("OSS_ACCESS_ID")) {
        jdo_setOption(_options, "fs.oss.accessKeyId", getenv("OSS_ACCESS_ID"));
    }
    if (getenv("OSS_ACCESS_KEY")) {
        jdo_setOption(_options, "fs.oss.accessKeySecret", getenv("OSS_ACCESS_KEY"));
    }

    jdo_setOption(_options, "logger.appender", "file");

    _store = jdo_createStore(_options, uri.c_str());

    START_CALL(_store)
    jdo_init(ctx, user.c_str());
    END_CALL()

    if (errorCode != 0) {
        destroy();
        return  JdoStatus::InitError("create jdo filesystem failed"
                                     ", uri: ", uri,
                                     ", user: ", user,
                                     ", error:  ", errorMsg);
    }

    _inited = true;
    return JdoStatus::OK();
}

JdoStatus JdoFileSystem::destroy() {
    if (!_inited) {
        return JdoStatus::OK();
    }

    if (_store != nullptr) {
        START_CALL(_store)
        jdo_destroyStore(_store);
        END_CALL()
        jdo_freeStore(_store);
        _store = nullptr;
    }
    if (_options != nullptr) {
        jdo_freeOptions(_options);
        _options = nullptr;
    }
    _inited = false;

	return JdoStatus::OK();
}

JdoStatus JdoFileSystem::openReader(const std::string &path,
                                    std::unique_ptr<JdoReader> *result) {
    auto [store, err] = GetJdoStore(path);
    if (!err.ok()) {
        return err;
    }

    START_CALL(store)
    auto stream = jdo_open(ctx, path.c_str(), JDO_OPEN_FLAG_READ_ONLY, 0777, nullptr);
    END_CALL()

    if (errorCode != 0) {
        return JdoStatus::InternalError(errorCode, "open file reader failed: ", errorMsg);
    }

    result->reset(new JindoReader(path, store, stream));

    return JdoStatus::OK();
}

JdoStatus JdoFileSystem::openWriter(const std::string &path,
                                    std::unique_ptr<JdoWriter> *result) {
    auto [store, err] = GetJdoStore(path);
    if (!err.ok()) {
        return err;
    }

    START_CALL(store)
    auto flag = JDO_OPEN_FLAG_CREATE | JDO_OPEN_FLAG_OVERWRITE;
    auto handle = jdo_open(ctx, path.c_str(), flag, 0777, nullptr);
    END_CALL()

    if (errorCode != 0) {
        return JdoStatus::InternalError(errorCode, "open file writer failed: ", errorMsg);
    }

    result->reset(new JindoWriter(path, store, handle));
    return JdoStatus::OK();
}

JdoStatus JdoFileSystem::mkdir(const std::string& path, bool recursive) {
    auto [store, err] = GetJdoStore(path);
    if (!err.ok()) {
        return err;
    }

    START_CALL(store)
    bool result = jdo_mkdir(ctx, path.c_str(), recursive, 0777, nullptr);
    END_CALL()

    if (errorCode != 0) {
        return JdoStatus::InternalError(errorCode, "mkdir failed: ", errorMsg);
    } else if (!result) {
        return JdoStatus::IOError("mkdir failed: ", path);
    }

    return JdoStatus::OK();
}

struct MkdirArgs {
    std::string path;
    bool recursive;
    std::function<void(JdoStatus)> callback;
};

static void mkdirCallback(JdoHandleCtx_t ctx, bool result, void* userdata) {
    MkdirArgs* args = (MkdirArgs*) userdata;
    auto path = args->path;
    std::function<void(JdoStatus)> callback = args->callback;
    delete args;

    END_CALL()
    JdoStatus status;
    if (errorCode != 0) {
        status = JdoStatus::InternalError(errorCode, "mkdir failed: ", errorMsg);
    } else if (!result) {
        status = JdoStatus::IOError("mkdir failed: ", path);
    }
    if (callback) {
        callback(status);
    }
}

JdoTaskPtr JdoFileSystem::mkdirAsync(const std::string& path, bool recursive, std::function<void(JdoStatus)> callback) {
    auto [store, err] = GetJdoStore(path);
    if (!err.ok()) {
        return nullptr;
    }

    MkdirArgs* args = new MkdirArgs();
    args->path = path;
    args->recursive = recursive;
    args->callback = callback;

    START_CALL(store)
    auto options = jdo_createOptions();
    jdo_setBoolCallback(options, mkdirCallback);
    jdo_setCallbackContext(options, args);
    auto operationCall = jdo_mkdirAsync(ctx, path.c_str(), recursive, 0777, options);
    END_CALL()
    jdo_freeOptions(options);

    return std::make_shared<JindoOperation>(_store, operationCall);
}

JdoStatus JdoFileSystem::rename(const std::string &oldpath, const std::string &newpath) {
    auto [store, err] = GetJdoStore(oldpath);
    if (!err.ok()) {
        return err;
    }

    if (exists(newpath).ok()) {
        auto status = remove(newpath, false);
        if (!status.ok()) {
            return status;
        }
    }

    START_CALL(store)
    bool result = jdo_rename(ctx, oldpath.c_str(), newpath.c_str(), nullptr);
    END_CALL()

    if (errorCode != 0) {
        return JdoStatus::InternalError(errorCode, "rename failed: ", errorMsg);
    } else if (!result) {
        return JdoStatus::IOError("rename failed,"
                                  " from ", oldpath,
                                  " to ", newpath);
    }
    return JdoStatus::OK();
}

struct RenameArgs {
    std::string oldpath;
    std::string newpath;
    std::function<void(JdoStatus)> callback;
};

static void renameCallback(JdoHandleCtx_t ctx, bool result, void* userdata) {
    RenameArgs* args = (RenameArgs*) userdata;
    auto oldpath = args->oldpath;
    auto newpath = args->newpath;
    std::function<void(JdoStatus)> callback = args->callback;
    delete args;

    END_CALL()
    JdoStatus status;
    if (errorCode != 0) {
        status = JdoStatus::InternalError(errorCode, "rename failed: ", errorMsg);
    } else if (!result) {
        status = JdoStatus::IOError("rename failed,"
                                  " from ", oldpath,
                                  " to ", newpath);
    }
    if (callback) {
        callback(status);
    }
}

JdoTaskPtr JdoFileSystem::renameAsync(const std::string &oldpath, const std::string &newpath, std::function<void(JdoStatus)> callback) {
    auto [store, err] = GetJdoStore(oldpath);
    if (!err.ok()) {
        return nullptr;
    }

    if (exists(newpath).ok()) {
        auto status = remove(newpath, false);
        if (!status.ok()) {
            return nullptr;
        }
    }

    RenameArgs* args = new RenameArgs();
    args->oldpath = oldpath;
    args->newpath = newpath;
    args->callback = callback;

    START_CALL(store)
    auto options = jdo_createOptions();
    jdo_setBoolCallback(options, renameCallback);
    jdo_setCallbackContext(options, args);
    auto operationCall = jdo_renameAsync(ctx, oldpath.c_str(), newpath.c_str(), options);
    END_CALL()
    jdo_freeOptions(options);

    return std::make_shared<JindoOperation>(_store, operationCall);
}

JdoStatus JdoFileSystem::remove(const std::string& path, bool recursive) {
    auto [store, err] = GetJdoStore(path);
    if (!err.ok()) {
        return err;
    }

    START_CALL(store)
    bool result = jdo_remove(ctx, path.c_str(), recursive, nullptr);
    END_CALL()

    if (errorCode != 0) {
        return JdoStatus::InternalError(errorCode, "delete failed: ", errorMsg);
    } else if (!result) {
        return JdoStatus::IOError("delete failed: ", path, " recursive ", recursive);
    }
    return JdoStatus::OK();
}

struct RemoveArgs {
    std::string path;
    bool recursive;
    std::function<void(JdoStatus)> callback;
};

static void removeCallback(JdoHandleCtx_t ctx, bool result, void* userdata) {
    RemoveArgs* args = (RemoveArgs*) userdata;
    auto path = args->path;
    auto recursive = args->recursive;
    std::function<void(JdoStatus)> callback = args->callback;
    delete args;

    END_CALL()
    JdoStatus status;
    if (errorCode != 0) {
        status = JdoStatus::InternalError(errorCode, "delete failed: ", errorMsg);
    } else if (!result) {
        status = JdoStatus::IOError("delete failed: ", path, " recursive ", recursive);
    }
    if (callback) {
        callback(status);
    }
}

JdoTaskPtr JdoFileSystem::removeAsync(const std::string& path, bool recursive, std::function<void(JdoStatus)> callback) {
    auto [store, err] = GetJdoStore(path);
    if (!err.ok()) {
        return nullptr;
    }

    RemoveArgs* args = new RemoveArgs();
    args->path = path;
    args->recursive = recursive;
    args->callback = callback;

    START_CALL(store)
    auto options = jdo_createOptions();
    jdo_setBoolCallback(options, removeCallback);
    jdo_setCallbackContext(options, args);
    auto operationCall = jdo_removeAsync(ctx, path.c_str(), recursive, options);
    END_CALL()
    jdo_freeOptions(options);

    return std::make_shared<JindoOperation>(_store, operationCall);
}

JdoStatus JdoFileSystem::exists(const std::string& path) {
    auto [store, err] = GetJdoStore(path);
    if (!err.ok()) {
        return JdoStatus::IOError(err);
    }

    START_CALL(store)
    bool result = jdo_exists(ctx, path.c_str(), nullptr);
    END_CALL()

    if (errorCode != 0) {
        return JdoStatus::InternalError(errorCode, "exists failed: ", errorMsg);
    } else if (!result) {
        return JdoStatus::NotFound(path, " doesn't exist.");
    }

    return JdoStatus::OK();
}

struct ExistsArgs {
    std::string path;
    std::function<void(JdoStatus)> callback;
};

static void existsCallback(JdoHandleCtx_t ctx, bool result, void* userdata) {
    ExistsArgs* args = (ExistsArgs*) userdata;
    auto path = args->path;
    std::function<void(JdoStatus)> callback = args->callback;
    delete args;

    END_CALL()
    JdoStatus status;
    if (errorCode != 0) {
        status = JdoStatus::InternalError(errorCode, "exists failed: ", errorMsg);
    } else if (!result) {
        status = JdoStatus::NotFound(path, " doesn't exist.");
    }
    if (callback) {
        callback(status);
    }
}

JdoTaskPtr JdoFileSystem::existsAsync(const std::string &path, std::function<void(JdoStatus)> callback) {
    auto [store, err] = GetJdoStore(path);
    if (!err.ok()) {
        return nullptr;
    }

    ExistsArgs* args = new ExistsArgs();
    args->path = path;
    args->callback = callback;

    START_CALL(store)
    auto options = jdo_createOptions();
    jdo_setBoolCallback(options, existsCallback);
    jdo_setCallbackContext(options, args);
    auto operationCall = jdo_existsAsync(ctx, path.c_str(), options);
    END_CALL()
    jdo_freeOptions(options);

    return std::make_shared<JindoOperation>(_store, operationCall);
}

JdoStatus JdoFileSystem::getFileInfo(const std::string& path, JdoFileInfo* stat) {
    auto [store, err] = GetJdoStore(path);
    if (!err.ok()) {
        return err;
    }

    return getFileInfoInternal(store, path, stat);
}

struct GetFileInfoArgs {
    std::string path;
    std::function<void(JdoStatus, JdoFileInfoPtr)> callback;
};

static void getFileInfoCallback(JdoHandleCtx_t ctx, JdoFileStatus_t fileStatus, void* userdata) {
    GetFileInfoArgs* args = (GetFileInfoArgs*) userdata;
    auto path = args->path;
    std::function<void(JdoStatus, JdoFileInfoPtr)> callback = args->callback;
    delete args;

    auto info = std::make_shared<JdoFileInfo>();
    covertToFileInfo(fileStatus, info.get());
    jdo_freeFileStatus(fileStatus);

    END_CALL()
    JdoStatus status;
    if (errorCode != 0) {
        if (errorCode == JDO_FILE_NOT_FOUND_ERROR) {
            status = JdoStatus::NotFound("cant not found ", path);
        } else {
            status = JdoStatus::InternalError(errorCode, "get status failed: ", errorMsg);
        }
    }

    if (callback) {
        callback(status, info);
    }
}

JdoTaskPtr JdoFileSystem::getFileInfoAsync(const std::string &path, std::function<void(JdoStatus, JdoFileInfoPtr)> callback) {
    auto [store, err] = GetJdoStore(path);
    if (!err.ok()) {
        return nullptr;
    }

    GetFileInfoArgs* args = new GetFileInfoArgs();
    args->path = path;
    args->callback = callback;

    START_CALL(store)
    auto options = jdo_createOptions();
    jdo_setFileStatusCallback(options, getFileInfoCallback);
    jdo_setCallbackContext(options, args);
    auto operationCall = jdo_getFileStatusAsync(ctx, path.c_str(), options);
    END_CALL()
    jdo_freeOptions(options);

    return std::make_shared<JindoOperation>(_store, operationCall);
}

JdoStatus JdoFileSystem::listDir(const std::string &dir, bool recursive, JdoListResult *result) {
    auto [store, err] = GetJdoStore(dir);
    if (!err.ok()) {
        return err;
    }

    START_CALL(store)
    JdoListDirResult_t listResult = jdo_listDir(ctx, dir.c_str(), recursive, nullptr);
    END_CALL()

    if (errorCode != 0) {
        return JdoStatus::InternalError(errorCode, "get children failed: ", errorMsg);
    }

    auto numEntries = jdo_getListDirResultSize(listResult);
    for (int i = 0; i < numEntries; i++) {
        auto fileStatus = jdo_getListDirFileStatus(listResult, i);
        if (fileStatus == nullptr) {
            continue;
        }
        JdoFileInfo info;
        covertToFileInfo(fileStatus, &info);
        result->infos.push_back(info);
    }
    result->truncated = jdo_isListDirResultTruncated(listResult);
    auto nextMarker = jdo_getListDirResultNextMarker(listResult);
    if (nextMarker) {
        result->nextMarker = nextMarker;
    }

    jdo_freeListDirResult(listResult);
    return JdoStatus::OK();
}

struct listDirArgs {
    std::string path;
    std::function<void(JdoStatus, JdoListResultPtr)> callback;
};

static void listDirCallback(JdoHandleCtx_t ctx, JdoListDirResult_t listResult, void* userdata) {
    listDirArgs* args = (listDirArgs*) userdata;
    auto path = args->path;
    std::function<void(JdoStatus, JdoListResultPtr result)> callback = args->callback;
    delete args;

    END_CALL()
    JdoStatus status;
    auto result = std::make_shared<JdoListResult>();
    if (errorCode != 0) {
        status = JdoStatus::InternalError(errorCode, "get children failed: ", errorMsg);
        callback(status, result);
        return;
    }

    auto numEntries = jdo_getListDirResultSize(listResult);
    for (int i = 0; i < numEntries; i++) {
        auto fileStatus = jdo_getListDirFileStatus(listResult, i);
        if (fileStatus == nullptr) {
            continue;
        }
        JdoFileInfo info;
        covertToFileInfo(fileStatus, &info);
        result->infos.push_back(info);
    }
    result->truncated = jdo_isListDirResultTruncated(listResult);
    auto nextMarker = jdo_getListDirResultNextMarker(listResult);
    if (nextMarker) {
        result->nextMarker = nextMarker;
    }

    jdo_freeListDirResult(listResult);

    if (callback) {
        callback(status, result);
    }
}

JdoTaskPtr JdoFileSystem::listDirAsync(const std::string &path, bool recursive, std::function<void(JdoStatus, JdoListResultPtr)> callback) {
    auto [store, err] = GetJdoStore(path);
    if (!err.ok()) {
        return nullptr;
    }

    listDirArgs* args = new listDirArgs();
    args->path = path;
    args->callback = callback;

    START_CALL(store)
    auto options = jdo_createOptions();
    jdo_setListDirResultCallback(options, listDirCallback);
    jdo_setCallbackContext(options, args);
    auto operationCall = jdo_listDirAsync(ctx, path.c_str(), false, options);
    END_CALL()
    jdo_freeOptions(options);

    return std::make_shared<JindoOperation>(_store, operationCall);
}

JdoStatus JdoFileSystem::getContentSummary(const std::string &dir, bool recursive, JdoContentSummary *result) {
    auto [store, err] = GetJdoStore(dir);
    if (!err.ok()) {
        return err;
    }

    START_CALL(store)
    JdoContentSummary_t contentSummary = jdo_getContentSummary(ctx, dir.c_str(), recursive, nullptr);
    END_CALL()

    if (errorCode != 0) {
        return JdoStatus::InternalError(errorCode, "get content summary failed: ", errorMsg);
    }

    result->fileSize = jdo_getContentSummaryFileSize(contentSummary);
    result->fileCount = jdo_getContentSummaryFileCount(contentSummary);
    result->dirCount = jdo_getContentSummaryDirectoryCount(contentSummary);

    jdo_freeContentSummary(contentSummary);
    return JdoStatus::OK();
}

struct GetContentSummaryArgs {
    std::string path;
    std::function<void(JdoStatus, JdoContentSummaryPtr)> callback;
};

static void getContentSummaryCallback(JdoHandleCtx_t ctx, JdoContentSummary_t contentSummary, void* userdata) {
    GetContentSummaryArgs* args = (GetContentSummaryArgs*) userdata;
    auto path = args->path;
    std::function<void(JdoStatus, JdoContentSummaryPtr result)> callback = args->callback;
    delete args;

    END_CALL()
    JdoStatus status;
    auto result = std::make_shared<JdoContentSummary>();
    if (errorCode != 0) {
        status = JdoStatus::InternalError(errorCode, "get content summary failed: ", errorMsg);
        callback(status, result);
        return;
    }

    result->fileSize = jdo_getContentSummaryFileSize(contentSummary);
    result->fileCount = jdo_getContentSummaryFileCount(contentSummary);
    result->dirCount = jdo_getContentSummaryDirectoryCount(contentSummary);

    jdo_freeContentSummary(contentSummary);

    if (callback) {
        callback(status, result);
    }
}

JdoTaskPtr JdoFileSystem::getContentSummaryAsync(const std::string &path, bool recursive, std::function<void(JdoStatus, JdoContentSummaryPtr)> callback) {
    auto [store, err] = GetJdoStore(path);
    if (!err.ok()) {
        return nullptr;
    }

    GetContentSummaryArgs* args = new GetContentSummaryArgs();
    args->path = path;
    args->callback = callback;

    START_CALL(store)
    auto options = jdo_createOptions();
    jdo_setContentSummaryCallback(options, getContentSummaryCallback);
    jdo_setCallbackContext(options, args);
    auto operationCall = jdo_getContentSummaryAsync(ctx, path.c_str(), false, options);
    END_CALL()
    jdo_freeOptions(options);

    return std::make_shared<JindoOperation>(_store, operationCall);
}

JdoStatus JdoFileSystem::setPermission(const std::string& path, int16_t perm) {
    auto [store, err] = GetJdoStore(path);
    if (!err.ok()) {
        return err;
    }

    START_CALL(store)
    bool result = jdo_setPermission(ctx, path.c_str(), perm, nullptr);
    END_CALL()

    if (errorCode != 0) {
        return JdoStatus::InternalError(errorCode, "setPermission failed: ", errorMsg);
    } else if (!result) {
        return JdoStatus::IOError("setPermission failed: ", path);
    }

    return JdoStatus::OK();
}

struct SetPermissionArgs {
    std::string path;
    int16_t perm;
    std::function<void(JdoStatus)> callback;
};

static void setPermissionCallback(JdoHandleCtx_t ctx, bool result, void* userdata) {
    SetPermissionArgs* args = (SetPermissionArgs*) userdata;
    auto path = args->path;
    std::function<void(JdoStatus)> callback = args->callback;
    delete args;

    END_CALL()
    JdoStatus status;
    if (errorCode != 0) {
        status = JdoStatus::InternalError(errorCode, "setPermission failed: ", errorMsg);
    } else if (!result) {
        status = JdoStatus::IOError("setPermission failed: ", path);
    }
    if (callback) {
        callback(status);
    }
}

JdoTaskPtr JdoFileSystem::setPermissionAsync(const std::string& path, int16_t perm, std::function<void(JdoStatus)> callback) {
    auto [store, err] = GetJdoStore(path);
    if (!err.ok()) {
        return nullptr;
    }

    SetPermissionArgs* args = new SetPermissionArgs();
    args->path = path;
    args->perm = perm;
    args->callback = callback;

    START_CALL(store)
    auto options = jdo_createOptions();
    jdo_setBoolCallback(options, setPermissionCallback);
    jdo_setCallbackContext(options, args);
    auto operationCall = jdo_setPermissionAsync(ctx, path.c_str(), perm, options);
    END_CALL()
    jdo_freeOptions(options);

    return std::make_shared<JindoOperation>(_store, operationCall);
}


JdoStatus JdoFileSystem::setOwner(const std::string& path, const std::string& user, const std::string& group) {
    auto [store, err] = GetJdoStore(path);
    if (!err.ok()) {
        return err;
    }

    START_CALL(store)
    bool result = jdo_setOwner(ctx, path.c_str(), user.c_str(), group.c_str(), nullptr);
    END_CALL()

    if (errorCode != 0) {
        return JdoStatus::InternalError(errorCode, "setOwner failed: ", errorMsg);
    } else if (!result) {
        return JdoStatus::IOError("setOwner failed: ", path);
    }

    return JdoStatus::OK();
}

struct SetOwnerArgs {
    std::string path;
    std::string user;
    std::string group;
    std::function<void(JdoStatus)> callback;
};

static void setOwnerCallback(JdoHandleCtx_t ctx, bool result, void* userdata) {
    SetOwnerArgs* args = (SetOwnerArgs*) userdata;
    auto path = args->path;
    std::function<void(JdoStatus)> callback = args->callback;
    delete args;

    END_CALL()
    JdoStatus status;
    if (errorCode != 0) {
        status = JdoStatus::InternalError(errorCode, "setOwner failed: ", errorMsg);
    } else if (!result) {
        status = JdoStatus::IOError("setOwner failed: ", path);
    }
    if (callback) {
        callback(status);
    }
}

JdoTaskPtr JdoFileSystem::setOwnerAsync(const std::string& path,  const std::string& user, const std::string& group, std::function<void(JdoStatus)> callback) {
    auto [store, err] = GetJdoStore(path);
    if (!err.ok()) {
        return nullptr;
    }

    SetOwnerArgs* args = new SetOwnerArgs();
    args->path = path;
    args->user = user;
    args->group = group;
    args->callback = callback;

    START_CALL(store)
    auto options = jdo_createOptions();
    jdo_setBoolCallback(options, setOwnerCallback);
    jdo_setCallbackContext(options, args);
    auto operationCall = jdo_setOwnerAsync(ctx, path.c_str(), user.c_str(), group.c_str(), options);
    END_CALL()
    jdo_freeOptions(options);

    return std::make_shared<JindoOperation>(_store, operationCall);
}

JdoStatus JdoFileSystem::getFileInfoInternal(JdoStore_t store,
                                             const std::string &path,
                                             JdoFileInfo *info) {
    START_CALL(store)
    JdoFileStatus_t fileStatus = jdo_getFileStatus(ctx, path.c_str(), nullptr);
    END_CALL()

    JdoStatus ret;
    if (errorCode != 0) {
        if (errorCode == JDO_FILE_NOT_FOUND_ERROR) {
            return JdoStatus::NotFound("cant not found ", path);
        }
        return JdoStatus::InternalError(errorCode, "get status failed: ", errorMsg);
    }

    covertToFileInfo(fileStatus, info);

    jdo_freeFileStatus(fileStatus);
    return JdoStatus::OK();
}

std::tuple<JdoStore_t, JdoStatus> JdoFileSystem::GetJdoStore(const std::string &uri) {
    if (!_inited) {
        return {nullptr, JdoStatus::InitError()};
    }
    return {_store, JdoStatus::OK()};
}

