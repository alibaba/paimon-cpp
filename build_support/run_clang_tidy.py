#!/usr/bin/env python
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from __future__ import print_function
import argparse
from fnmatch import fnmatch
import multiprocessing as mp
import lintutils
from subprocess import PIPE
import sys
from functools import partial


def _get_chunk_key(filenames):
    # lists are not hashable so key on the first filename in a chunk
    return filenames[0]


# clang-tidy outputs complaints in '/path:line_number: complaint' format,
# so we can scan its output to get a list of files to fix
def _check_some_files(completed_processes, filenames):
    result = completed_processes[_get_chunk_key(filenames)]
    return lintutils.stdout_pathcolonline(result, filenames)


def _check_all(cmd, filenames):
    # each clang-tidy instance will process 16 files
    chunks = lintutils.chunk(filenames, 16)
    cmds = [cmd + some for some in chunks]
    results = lintutils.run_parallel(cmds, stderr=PIPE, stdout=PIPE)
    error = False
    # record completed processes (keyed by the first filename in the input
    # chunk) for lookup in _check_some_files
    completed_processes = {
        _get_chunk_key(some): result
        for some, result in zip(chunks, results)
    }
    checker = partial(_check_some_files, completed_processes)
    pool = mp.Pool()
    try:
        # check output of completed clang-tidy invocations in parallel
        for problem_files, stdout in pool.imap(checker, chunks):
            if problem_files:
                msg = "clang-tidy suggested fixes for {}"
                print("\n".join(map(msg.format, problem_files)))
                print(stdout)
                error = True
    except Exception:
        error = True
        raise
    finally:
        pool.terminate()
        pool.join()

    if error:
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Runs clang-tidy on all ",
        fromfile_prefix_chars="@"
    )
    parser.add_argument("--clang_tidy_binary",
                        required=True,
                        help="Path to the clang-tidy binary")
    parser.add_argument("--exclude_globs",
                        help="Filename containing globs for files "
                        "that should be excluded from the checks")
    parser.add_argument("--compile_commands",
                        required=True,
                        help="compile_commands.json to pass clang-tidy")
    path_group = parser.add_mutually_exclusive_group(required=True)
    path_group.add_argument("--source_dir",
                            nargs="+",
                            help="Root directory(s) of the source code")
    path_group.add_argument("--source_file",
                            nargs="*",
                            help="file path(s) of every source code")
    parser.add_argument("--extra_arg",
                        help="Extra arguments to pass to clang-tidy")
    parser.add_argument("--fix", default=False,
                        action="store_true",
                        help="If specified, will attempt to fix the "
                        "source code instead of recommending fixes, "
                        "defaults to %(default)s")
    parser.add_argument("--quiet", default=False,
                        action="store_true",
                        help="If specified, only print errors")
    parser.add_argument("--use_color", default=True,
                        action="store_true",
                        help="If specified, print errors in color")
    arguments = parser.parse_args()

    exclude_globs = []
    if arguments.exclude_globs:
        for line in open(arguments.exclude_globs):
            exclude_globs.append(line.strip())

    linted_filenames = []
    if arguments.source_dir:
        for dir in arguments.source_dir:
            for path in lintutils.get_sources(dir, exclude_globs):
                linted_filenames.append(path)
    elif arguments.source_file:
        for path in arguments.source_file:
            if (lintutils.need_do_lint(path, exclude_globs)):
                linted_filenames.append(path)

    if not arguments.quiet:
        msg = 'Tidying {}' if arguments.fix else 'Checking {}'
        print("\n".join(map(msg.format, linted_filenames)))

    cmd = [
        arguments.clang_tidy_binary,
        '-p',
        arguments.compile_commands
    ]

    if arguments.extra_arg:
        cmd.append("--extra-arg={}".format(arguments.extra_arg))

    if arguments.use_color:
        cmd.append("--use-color")

    if arguments.fix:
        cmd.append('-fix')
        results = lintutils.run_parallel(
            [cmd + some for some in lintutils.chunk(linted_filenames, 16)])
        for returncode, stdout, stderr in results:
            if returncode != 0:
                sys.exit(returncode)
    else:
        _check_all(cmd, linted_filenames)
