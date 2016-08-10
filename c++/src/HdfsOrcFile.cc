/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "orc/HdfsOrcFile.hh"
#include "Adaptor.hh"
#include "Exceptions.hh"

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

namespace orc {

  class HDFSInputStream : public InputStream {
  private:
    std::string filename;
    hdfsFile file;
    long totalLength;
    hdfsFS hdfs;
  public:
    HDFSInputStream(hdfsFileInfo * fi, hdfsFS fs) {
      filename = fi->mName;
      hdfs = fs;
      totalLength = fi->mSize;
     
      long sampleBufferSize = fi->mBlockSize < 65536 ? fi->mBlockSize : 65536;
      sampleBufferSize = sampleBufferSize < totalLength/10 ? sampleBufferSize : totalLength/10;

      file = hdfsOpenFile(hdfs, fi->mName, O_RDONLY, 
                   (int)sampleBufferSize, // buffer size
                   0, // replication, take the default size 
                   (tSize)fi->mBlockSize); // blocksize 
    }
    HDFSInputStream(const std::string& path) {
      filename = path;
      struct hdfsBuilder * builder = hdfsNewBuilder();
      hdfsBuilderSetNameNode(builder, "default");
      hdfsBuilderSetNameNodePort(builder, 0);
      hdfs = hdfsBuilderConnect(builder);

      hdfsFileInfo * fileInfo = hdfsGetPathInfo(hdfs, path.c_str());
      filename = fileInfo->mName;

      totalLength = (uint64_t)fileInfo->mSize;

      file = hdfsOpenFile(hdfs, fileInfo->mName, O_RDONLY, 
                 65536, // buffer size
                 0, // replication, take the default size 
                 static_cast<tSize>(fileInfo->mBlockSize) // blocksize 
                 );
    }
    ~HDFSInputStream(){
       hdfsCloseFile(hdfs, file);
    }

    uint64_t getLength() const override {
      return totalLength;
    }

    uint64_t getNaturalReadSize() const override {
      return 128 * 1024;
    }

    void read(void* buf,
              uint64_t length,
              uint64_t offset) override {
      if (!buf) {
        throw ParseError("Buffer is null");
      }
      //ssize_t bytesRead = pread(file, buf, length, static_cast<off_t>(offset));

      ssize_t bytesRead = (ssize_t)hdfsPread(hdfs, file, static_cast<tOffset>(offset), buf, static_cast<tSize>(length));

      if (bytesRead == -1) {
        throw ParseError("Bad read of " + filename);
      }
      if (static_cast<uint64_t>(bytesRead) != length) {
        throw ParseError("Short read of " + filename);
      }
    }

    const std::string& getName() const override {
      return filename;
    }
  };//HDFSInputStream
  
  std::unique_ptr<InputStream> readHDFSFile(const std::string& path) {
    return std::unique_ptr<InputStream>(new HDFSInputStream(path));
  }

  std::unique_ptr<InputStream> readHDFSFile(hdfsFileInfo* fi, hdfsFS fs) {
    return std::unique_ptr<InputStream>(new HDFSInputStream(fi, fs));
  }

}//orc
