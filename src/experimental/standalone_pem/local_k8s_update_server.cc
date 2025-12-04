/*
 * Copyright 2018- The Pixie Authors.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

#include "src/experimental/standalone_pem/local_k8s_update_server.h"

#include <fcntl.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include <filesystem>
#include <string>

#include "src/shared/k8s/metadatapb/metadata.pb.h"

namespace px {
namespace vizier {
namespace agent {

std::string LocalK8sUpdateServer::SocketPath() {
  const char* envp = std::getenv("PL_LOCAL_K8S_UPDATE_UDS");
  if (envp != nullptr && std::strlen(envp) > 0) {
    return std::string(envp);
  }
  return std::string("/var/run/px-pem/mds.sock");
}

Status LocalK8sUpdateServer::Start() {
  server_thread_ = std::thread([this]() {
    auto s = this->ServerLoop();
    if (!s.ok()) {
      LOG(ERROR) << "LocalK8sUpdateServer error: " << s.msg();
    }
  });
  return Status::OK();
}

void LocalK8sUpdateServer::Stop() {
  stop_.store(true);
  if (server_thread_.joinable()) {
    server_thread_.join();
  }
}

Status LocalK8sUpdateServer::ServerLoop() {
  std::string path = SocketPath();
  std::filesystem::path p(path);
  std::filesystem::create_directories(p.parent_path());

  int fd = ::socket(AF_UNIX, SOCK_STREAM, 0);
  if (fd < 0) {
    return error::Internal("Failed to create AF_UNIX socket");
  }

  // Remove stale socket file if exists.
  ::unlink(path.c_str());

  struct sockaddr_un addr;
  memset(&addr, 0, sizeof(addr));
  addr.sun_family = AF_UNIX;
  ::strncpy(addr.sun_path, path.c_str(), sizeof(addr.sun_path) - 1);

  if (::bind(fd, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) < 0) {
    ::close(fd);
    return error::Internal("Failed to bind UDS");
  }
  if (::listen(fd, 8) < 0) {
    ::close(fd);
    return error::Internal("Failed to listen on UDS");
  }

  while (!stop_.load()) {
    int cfd = ::accept(fd, nullptr, nullptr);
    if (cfd < 0) {
      if (errno == EINTR) {
        continue;
      }
      break;
    }

    // Simple framing: 4-byte big-endian length prefix followed by protobuf bytes.
    for (;;) {
      uint32_t be_len = 0;
      ssize_t n = ::read(cfd, &be_len, sizeof(be_len));
      if (n == 0) {
        break;  // EOF
      }
      if (n < 0) {
        if (errno == EINTR) continue;
        break;
      }
      if (n != sizeof(be_len)) {
        break;
      }
      uint32_t len = ntohl(be_len);
      std::string buf;
      buf.resize(len);
      size_t off = 0;
      while (off < len) {
        ssize_t rn = ::read(cfd, buf.data() + off, len - off);
        if (rn <= 0) {
          break;
        }
        off += rn;
      }
      if (off != len) {
        break;
      }
      px::shared::k8s::metadatapb::ResourceUpdate update;
      if (!update.ParseFromArray(buf.data(), buf.size())) {
        LOG(WARNING) << "Failed to parse ResourceUpdate protobuf";
        continue;
      }
      auto s = receiver_->Push(update);
      if (!s.ok()) {
        LOG(WARNING) << "Receiver push failed: " << s.msg();
      }
    }
    ::close(cfd);
  }

  ::close(fd);
  ::unlink(path.c_str());
  return Status::OK();
}

}  // namespace agent
}  // namespace vizier
}  // namespace px
