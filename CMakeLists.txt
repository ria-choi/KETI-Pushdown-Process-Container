# Copyright 2021 the gRPC authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# cmake build file for C++ keyvaluestore example.
# Assumes protobuf and gRPC have been installed using cmake.
# See cmake_externalproject/CMakeLists.txt for all-in-one cmake build
# that automatically builds all the dependencies before building keyvaluestore.

cmake_minimum_required(VERSION 3.5.1)

project(snippet_sample C CXX)

include(cmake/common.cmake)

# Proto file
get_filename_component(ss_proto "./protos/snippet_sample.proto" ABSOLUTE)
get_filename_component(ss_proto_path "${ss_proto}" PATH)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)


# Generated sources
set(ss_proto_srcs "${CMAKE_CURRENT_BINARY_DIR}/snippet_sample.pb.cc")
set(ss_proto_hdrs "${CMAKE_CURRENT_BINARY_DIR}/snippet_sample.pb.h")
set(ss_grpc_srcs "${CMAKE_CURRENT_BINARY_DIR}/snippet_sample.grpc.pb.cc")
set(ss_grpc_hdrs "${CMAKE_CURRENT_BINARY_DIR}/snippet_sample.grpc.pb.h")

# set(Boost_USE_STATIC_LIBS OFF) 
# set(Boost_USE_MULTITHREADED ON)  
# set(Boost_USE_STATIC_RUNTIME OFF) 
# find_package(Boost 1.45.0 COMPONENTS filesystem regex) 
# set(Boost_USE_STATIC_LIBS        ON)  # only find static libs
# set(Boost_USE_DEBUG_LIBS         OFF) # ignore debug libs and 
# set(Boost_USE_RELEASE_LIBS       ON)  # only find release libs 
# set(Boost_USE_MULTITHREADED      ON)
# set(Boost_USE_STATIC_RUNTIME    OFF) 
# find_package(Boost 1.71.0 COMPONENTS date_time filesystem system ...)
find_package(Boost COMPONENTS thread REQUIRED)
include_directories(${Boost_INCLUDE_DIRS})



# set(SnippetManager "SnippetManager.cc" "SnippetManager.h" "buffer_manager.h" "mergequerykmc.h" "TableManager.h" "keti_type.h")
# set(TableManager "TableManager.cc" "TableManager.h" "keti_type.h")
# set(BufferManager "buffer_manager.cc" "buffer_manager.h" "keti_type.h")
# set(SnippetScheduler "CSDScheduler.cc" "keti_type.h")
# set(CSDManager "CSDManager.h" "CSDManager.cc")
# set(MergeQueryManager "mergequerykmc.cc" "mergequerykmc.h" "buffer_manager.h" "keti_type.h")
# set(repidjson "/usr/include/rapidjson/prettywriter.h" "/usr/include/rapidjson/stringbuffer.h" "/usr/include/rapidjson/writer.h" "/usr/include/rapidjson/document.h")

# set(SnippetManager "SnippetManager.cc" "SnippetManager.h")
# set(TableManager "TableManager.cc" "TableManager.h")
# set(BufferManager "buffer_manager.cc" "buffer_manager.h")
# set(SnippetScheduler "CSDScheduler.cc")
# set(CSDManager "CSDManager.h" "CSDManager.cc")
# set(MergeQueryManager "mergequerykmc.cc" "mergequerykmc.h" "buffer_manager.h" "keti_type.h")
set(repidjson "/usr/local/include/rapidjson/prettywriter.h" "/usr/local/include/rapidjson/stringbuffer.h" "/usr/local/include/rapidjson/writer.h" "/usr/local/include/rapidjson/document.h")


add_custom_command(
      OUTPUT "${ss_proto_srcs}" "${ss_proto_hdrs}" "${ss_grpc_srcs}" "${ss_grpc_hdrs}"
      COMMAND ${_PROTOBUF_PROTOC}
      ARGS --grpc_out "${CMAKE_CURRENT_BINARY_DIR}"
        --cpp_out "${CMAKE_CURRENT_BINARY_DIR}"
        -I "${ss_proto_path}"
        --plugin=protoc-gen-grpc="${_GRPC_CPP_PLUGIN_EXECUTABLE}"
        "${ss_proto}"
      DEPENDS "${ss_proto}")

# Include generated *.pb.h files
include_directories("${CMAKE_CURRENT_BINARY_DIR}")

# kvs_grpc_proto
add_library(ss_grpc_proto
  ${ss_grpc_srcs}
  ${ss_grpc_hdrs}
  ${ss_proto_srcs}
  ${ss_proto_hdrs}
  # ${Boost_FILESYSTEM_LIBRARY}
)
target_link_libraries(ss_grpc_proto
  ${_REFLECTION}
  ${_GRPC_GRPCPP}
  ${Boost_PROGRAM_OPTIONS_LIBRARY}
  ${_PROTOBUF_LIBPROTOBUF})
  

# DB_Connector_Instance
# add_executable(db_connector_instance "db_connector_instance.cc")
# target_link_libraries(db_connector_instance
#   ss_grpc_proto
#   ${_REFLECTION}
#   ${_GRPC_GRPCPP}
#   ${_PROTOBUF_LIBPROTOBUF})

set(Casablanca_LIBRARIES "-lcrypto -lssl -lcpprest")
set(LINKING_LIBRARIES ${Casablanca_LIBRARIES})

# # Storage_Engine_Instance
add_executable(storage_engine_instance "StorageEngineInputInterface.cc" 
"SnippetManager.cc" "TableManager.cc" "buffer_manager.cc" "CSDScheduler.cc" "CSDManager.cc" "mergequerykmc.cc" 
"testmodule.cc" 
"WalManager.cc" "keti_util.h" "log_msg.h")


target_link_libraries(storage_engine_instance
  ss_grpc_proto
  # ${SnippetManager}
  # ${TableManager}
  # ${BufferManager}
  # ${SnippetScheduler}
  # ${MergeQueryManager}
  # ${CSDManager}
  ${Boost_PROGRAM_OPTIONS_LIBRARY}
  ${Boost_THREAD_LIBRARY}
  ${Boost_LIBRARIES}
  ${repidjson}
  ${_REFLECTION}
  ${_GRPC_GRPCPP}
  ${_PROTOBUF_LIBPROTOBUF}
  ${LINKING_LIBRARIES} )

# target_link_libraries(storage_engine_instance Boost::filesystem Boost::regex)