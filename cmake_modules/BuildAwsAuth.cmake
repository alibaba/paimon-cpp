# Copyright 2026-present Alibaba Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

include(ExternalProject)
include(GNUInstallDirs)

function(paimon_add_s2n_project)
    if(DEFINED ENV{PAIMON_AWS_S2N_URL})
        set(S2N_URL "$ENV{PAIMON_AWS_S2N_URL}")
    elseif(EXISTS "${THIRDPARTY_DIR}/${PAIMON_AWS_S2N_PKG_NAME}")
        set(S2N_URL "${THIRDPARTY_DIR}/${PAIMON_AWS_S2N_PKG_NAME}")
    else()
        set(S2N_URL
            "${THIRDPARTY_MIRROR_URL}https://github.com/aws/s2n-tls/archive/refs/tags/${PAIMON_AWS_S2N_BUILD_VERSION}.zip"
        )
    endif()
    set(S2N_C_FLAGS "${EP_C_FLAGS}")
    set(S2N_CXX_FLAGS "${EP_CXX_FLAGS}")
    string(REPLACE "-Wdocumentation" "" S2N_C_FLAGS "${S2N_C_FLAGS}")
    string(REPLACE "-Wdocumentation" "" S2N_CXX_FLAGS "${S2N_CXX_FLAGS}")
    externalproject_add(s2n_ep
                        URL ${S2N_URL}
                        URL_HASH SHA256=${PAIMON_AWS_S2N_BUILD_SHA256_CHECKSUM}
                        DOWNLOAD_EXTRACT_TIMESTAMP TRUE
                        CMAKE_ARGS ${EP_COMMON_CMAKE_ARGS}
                                   -DCMAKE_C_FLAGS=${S2N_C_FLAGS}
                                   -DCMAKE_CXX_FLAGS=${S2N_CXX_FLAGS}
                                   -DCMAKE_INSTALL_PREFIX=${AWS_AUTH_PREFIX}
                                   -DCMAKE_INSTALL_LIBDIR=${AWS_AUTH_INSTALL_LIBDIR}
                                   -DCMAKE_PREFIX_PATH=${AWS_AUTH_PREFIX}
                                   -Dcrypto_INCLUDE_DIR=${OPENSSL_INCLUDE_DIR}
                                   -Dcrypto_LIBRARY=${OPENSSL_CRYPTO_LIBRARY}
                                   -DCMAKE_POSITION_INDEPENDENT_CODE=ON
                                   -DS2N_INTERN_LIBCRYPTO=OFF
                        BUILD_BYPRODUCTS ${AWS_AUTH_LIB_DIR}/libs2n.a
                                         ${THIRDPARTY_LOG_OPTIONS})
endfunction()

function(paimon_add_aws_c_project
         NAME
         VERSION
         CHECKSUM
         DEPENDS)
    string(REPLACE "aws-c-" "AWS_C_" URL_NAME "${NAME}")
    string(TOUPPER "${URL_NAME}" URL_NAME)
    set(URL_VAR "PAIMON_${URL_NAME}_URL")
    set(PKG_NAME_VAR "PAIMON_${URL_NAME}_PKG_NAME")
    if(DEFINED ENV{${URL_VAR}})
        set(URL "$ENV{${URL_VAR}}")
    elseif(EXISTS "${THIRDPARTY_DIR}/${${PKG_NAME_VAR}}")
        set(URL "${THIRDPARTY_DIR}/${${PKG_NAME_VAR}}")
    else()
        set(URL
            "${THIRDPARTY_MIRROR_URL}https://github.com/awslabs/${NAME}/archive/refs/tags/${VERSION}.tar.gz"
        )
    endif()
    set(AWS_PLATFORM_CMAKE_ARGS)
    list(APPEND
         AWS_PLATFORM_CMAKE_ARGS
         -DOPENSSL_INCLUDE_DIR=${OPENSSL_INCLUDE_DIR}
         -DOPENSSL_SSL_LIBRARY=${OPENSSL_SSL_LIBRARY}
         -DOPENSSL_CRYPTO_LIBRARY=${OPENSSL_CRYPTO_LIBRARY}
         -Dcrypto_INCLUDE_DIR=${OPENSSL_INCLUDE_DIR}
         -Dcrypto_LIBRARY=${OPENSSL_CRYPTO_LIBRARY})
    if(NAME STREQUAL "aws-c-cal")
        list(APPEND AWS_PLATFORM_CMAKE_ARGS -DUSE_OPENSSL=ON)
    endif()
    if(APPLE)
        list(APPEND AWS_PLATFORM_CMAKE_ARGS -DAWS_USE_SECITEM=ON)
    endif()
    # aws-c-io's installed package unconditionally looks for s2n on Unix even
    # when it was built with Apple's native TLS backend.
    if(APPLE AND (NAME STREQUAL "aws-c-http" OR NAME STREQUAL "aws-c-auth"))
        list(APPEND AWS_PLATFORM_CMAKE_ARGS -DBYO_CRYPTO=ON)
    endif()
    externalproject_add(${NAME}_ep
                        URL ${URL}
                        URL_HASH SHA256=${CHECKSUM}
                        DOWNLOAD_EXTRACT_TIMESTAMP TRUE
                        CMAKE_ARGS ${EP_COMMON_CMAKE_ARGS}
                                   -DCMAKE_INSTALL_PREFIX=${AWS_AUTH_PREFIX}
                                   -DCMAKE_INSTALL_LIBDIR=${AWS_AUTH_INSTALL_LIBDIR}
                                   -DCMAKE_PREFIX_PATH=${AWS_AUTH_PREFIX}
                                   -DCMAKE_POSITION_INDEPENDENT_CODE=ON
                                   -DENABLE_TESTING=OFF
                                   ${AWS_PLATFORM_CMAKE_ARGS}
                        DEPENDS ${DEPENDS}
                        BUILD_BYPRODUCTS ${AWS_AUTH_LIB_DIR}/lib${NAME}.a
                                         ${THIRDPARTY_LOG_OPTIONS})
endfunction()

function(build_aws_auth)
    set(AWS_AUTH_PREFIX
        "${CMAKE_CURRENT_BINARY_DIR}/aws-auth_ep-install"
        PARENT_SCOPE)
    set(AWS_AUTH_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/aws-auth_ep-install")
    set(AWS_AUTH_INSTALL_LIBDIR "${CMAKE_INSTALL_LIBDIR}")
    set(AWS_AUTH_LIB_DIR "${AWS_AUTH_PREFIX}/${AWS_AUTH_INSTALL_LIBDIR}")
    file(MAKE_DIRECTORY "${AWS_AUTH_PREFIX}/include")
    file(MAKE_DIRECTORY "${AWS_AUTH_LIB_DIR}")

    find_package(OpenSSL REQUIRED)

    set(AWS_IO_DEPENDS "aws-c-common_ep;aws-c-cal_ep")
    if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
        paimon_add_s2n_project()
        list(APPEND AWS_IO_DEPENDS s2n_ep)
    endif()

    paimon_add_aws_c_project(aws-c-common ${PAIMON_AWS_C_COMMON_BUILD_VERSION}
                             ${PAIMON_AWS_C_COMMON_BUILD_SHA256_CHECKSUM} "")
    paimon_add_aws_c_project(aws-c-sdkutils ${PAIMON_AWS_C_SDKUTILS_BUILD_VERSION}
                             ${PAIMON_AWS_C_SDKUTILS_BUILD_SHA256_CHECKSUM}
                             aws-c-common_ep)
    paimon_add_aws_c_project(aws-c-cal ${PAIMON_AWS_C_CAL_BUILD_VERSION}
                             ${PAIMON_AWS_C_CAL_BUILD_SHA256_CHECKSUM} aws-c-common_ep)
    paimon_add_aws_c_project(aws-c-compression ${PAIMON_AWS_C_COMPRESSION_BUILD_VERSION}
                             ${PAIMON_AWS_C_COMPRESSION_BUILD_SHA256_CHECKSUM}
                             aws-c-common_ep)
    paimon_add_aws_c_project(aws-c-io ${PAIMON_AWS_C_IO_BUILD_VERSION}
                             ${PAIMON_AWS_C_IO_BUILD_SHA256_CHECKSUM} "${AWS_IO_DEPENDS}")
    paimon_add_aws_c_project(aws-c-http ${PAIMON_AWS_C_HTTP_BUILD_VERSION}
                             ${PAIMON_AWS_C_HTTP_BUILD_SHA256_CHECKSUM}
                             "aws-c-common_ep;aws-c-io_ep;aws-c-compression_ep")
    paimon_add_aws_c_project(aws-c-auth
                             ${PAIMON_AWS_C_AUTH_BUILD_VERSION}
                             ${PAIMON_AWS_C_AUTH_BUILD_SHA256_CHECKSUM}
                             "aws-c-common_ep;aws-c-sdkutils_ep;aws-c-cal_ep;aws-c-io_ep;aws-c-http_ep"
    )

    find_package(Threads REQUIRED)
    set(AWS_AUTH_PLATFORM_LIBS Threads::Threads ${CMAKE_DL_LIBS})
    if(APPLE)
        list(APPEND
             AWS_AUTH_PLATFORM_LIBS
             "-framework Security"
             "-framework CoreFoundation"
             "-framework Network")
    endif()
    set(AWS_AUTH_TLS_LIBS)
    if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
        list(APPEND AWS_AUTH_TLS_LIBS "${AWS_AUTH_LIB_DIR}/libs2n.a")
    endif()
    set(AWS_AUTH_RUNTIME_LIBS
        "${AWS_AUTH_LIB_DIR}/libaws-c-http.a"
        "${AWS_AUTH_LIB_DIR}/libaws-c-io.a"
        "${AWS_AUTH_LIB_DIR}/libaws-c-compression.a"
        "${AWS_AUTH_LIB_DIR}/libaws-c-cal.a"
        "${AWS_AUTH_LIB_DIR}/libaws-c-sdkutils.a"
        "${AWS_AUTH_LIB_DIR}/libaws-c-common.a"
        ${AWS_AUTH_TLS_LIBS})
    if(NOT APPLE)
        list(PREPEND AWS_AUTH_RUNTIME_LIBS "-Wl,--start-group")
        list(APPEND AWS_AUTH_RUNTIME_LIBS "-Wl,--end-group")
    endif()
    # Keep OpenSSL after the static AWS archives. In embedded builds CMake can
    # resolve these targets to static archives, where link order matters.
    list(APPEND AWS_AUTH_RUNTIME_LIBS "${OPENSSL_SSL_LIBRARY}"
         "${OPENSSL_CRYPTO_LIBRARY}")
    add_library(aws_auth_minimal STATIC IMPORTED GLOBAL)
    set_target_properties(aws_auth_minimal
                          PROPERTIES IMPORTED_LOCATION
                                     "${AWS_AUTH_LIB_DIR}/libaws-c-auth.a"
                                     INTERFACE_INCLUDE_DIRECTORIES
                                     "${AWS_AUTH_PREFIX}/include"
                                     INTERFACE_LINK_LIBRARIES
                                     "${AWS_AUTH_RUNTIME_LIBS};${AWS_AUTH_PLATFORM_LIBS}")
    add_dependencies(aws_auth_minimal aws-c-auth_ep)

    set(AWS_AUTH_INCLUDE_DIR
        "${AWS_AUTH_PREFIX}/include"
        PARENT_SCOPE)
    set(AWS_AUTH_LIB_DIR
        "${AWS_AUTH_LIB_DIR}"
        PARENT_SCOPE)
endfunction()
